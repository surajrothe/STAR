import pandas as pd
import numpy as np
from datetime import datetime
import time
import logging
from scenario_logger import logger
class AlertChecker:
    def __init__(self, current_date: datetime, job_details: dict, threshold_details: dict):
        self.job_details = job_details
        self.thresholds = threshold_details
        self.current_date = current_date
        self.alert_id_counter = 1000001
        self.logger = logger
        self.logger.info("Initializing AlertChecker for TS_SCN_30")


    def calculate_min_open_date_ind(self):
        min_open_date = self.current_date - pd.DateOffset(days=int(self.thresholds['MIN ACCT AGE IND']))
        return min_open_date.date()

    def calculate_min_open_date_org(self):
        min_open_date = self.current_date - pd.DateOffset(days=int(self.thresholds['MIN ACCT AGE ORG']))
        return min_open_date.date()


    def calculate_lookback_dates(self):
        if 1 <= self.current_date.day <= 27:
            cur_mon_end_date = (self.current_date.replace(day=1) - pd.offsets.MonthBegin(int(self.thresholds['LOOKBACK PERIOD']))).replace(day=1)
            cur_mon_start_date = (self.current_date.replace(day=1) - pd.Timedelta(days=1))

            # Calculate the last day of the previous month for lookback_start_date
            lookback_start_date = (cur_mon_end_date.replace(day=1) - pd.Timedelta(days=1))

            lookback_end_date = (lookback_start_date - pd.offsets.MonthBegin(6)).replace(day=1)

            return lookback_start_date.date(), lookback_end_date.date(), cur_mon_start_date.date(), cur_mon_end_date.date()
        else:
            cur_mon_start_date = self.current_date.replace(day=1)
            cur_mon_end_date = (cur_mon_start_date + pd.offsets.MonthEnd(0))

            # Calculate the last day of the previous month for lookback_start_date
            lookback_start_date = (cur_mon_end_date.replace(day=1) - pd.Timedelta(days=1))

            lookback_end_date = (lookback_start_date.replace(day=1) - pd.offsets.MonthBegin(6)).replace(day=1)

            return lookback_start_date.date(), lookback_end_date.date(), cur_mon_start_date.date(), cur_mon_end_date.date()


    def check_alerts(self, dataframe: pd.DataFrame) -> pd.DataFrame:

        try:

            dataframe = dataframe[dataframe['ACCT_OPEN_DT'].notnull()]

            dataframe['TRXN_EXCN_DT'] = pd.to_datetime(dataframe['TRXN_EXCN_DT'], format='%Y-%m-%d')
            dataframe['TRXN_EXCN_DT'] = dataframe['TRXN_EXCN_DT'].dt.date
            dataframe['ACCT_OPEN_DT'] = pd.to_datetime(dataframe['ACCT_OPEN_DT'], format='%Y-%m-%d')
            dataframe['ACCT_OPEN_DT'] = dataframe['ACCT_OPEN_DT'].dt.date

            cust_type_mapping = dataframe[['ACCT_ID', 'CUST_TYP']].drop_duplicates().set_index('ACCT_ID')['CUST_TYP'].to_dict()
            cust_risk_mapping = dataframe[['ACCT_ID', 'CUSTOMER_RISK']].drop_duplicates().set_index('ACCT_ID')['CUSTOMER_RISK'].to_dict()

            min_open_date_ind = self.calculate_min_open_date_ind()
            min_open_date_org = self.calculate_min_open_date_org()

            lookback_start_date, lookback_end_date, cur_mon_start_date, cur_mon_end_date = self.calculate_lookback_dates()


            # Filter data based on min open date
            filtered_data_ind = dataframe[
                (dataframe["CUST_TYP"] == "IND") &
                (dataframe['TRXN_EXCN_DT'].between(cur_mon_end_date, cur_mon_start_date)) &
                (dataframe['ACCT_OPEN_DT'] <= min_open_date_ind)
            ].copy()

            filtered_data_org = dataframe[
                (dataframe["CUST_TYP"] == "ORG") &
                (dataframe['TRXN_EXCN_DT'].between(cur_mon_end_date, cur_mon_start_date)) &
                (dataframe['ACCT_OPEN_DT'] <= min_open_date_org)
            ].copy()

            lookback_data = pd.concat([filtered_data_ind, filtered_data_org], ignore_index=True)


            if lookback_data.empty:
                self.logger.info("No data found for lookback period")
                return pd.DataFrame()

            lookback_acct_ids = lookback_data['ACCT_ID'].unique()


            # Filter data based on min open date
            filtered_data_ind_risk_period = dataframe[
                (dataframe["CUST_TYP"] == "IND") &
                (dataframe['TRXN_EXCN_DT'].between(lookback_end_date, lookback_start_date)) &
                (dataframe['ACCT_OPEN_DT'] <= min_open_date_ind) &
                (dataframe['ACCT_ID'].isin(lookback_acct_ids))
            ].copy()

            filtered_data_org_risk_period = dataframe[
                (dataframe["CUST_TYP"] == "ORG") &
                (dataframe['TRXN_EXCN_DT'].between(lookback_end_date, lookback_start_date)) &
                (dataframe['ACCT_OPEN_DT'] <= min_open_date_org) &
                (dataframe['ACCT_ID'].isin(lookback_acct_ids))
            ].copy()

            risk_period_data = pd.concat([filtered_data_ind_risk_period, filtered_data_org_risk_period], ignore_index=True)

            # Find common accounts between lookback_data and risk_period_data
            common_acct_ids = pd.merge(lookback_data[['ACCT_ID']], risk_period_data[['ACCT_ID']], on='ACCT_ID')

            # Filter lookback_data based on common account IDs
            lookback_data = lookback_data[lookback_data['ACCT_ID'].isin(common_acct_ids['ACCT_ID'])]

            # Filter risk_period_data based on common account IDs
            risk_period_data = risk_period_data[risk_period_data['ACCT_ID'].isin(common_acct_ids['ACCT_ID'])]



            # Calculate various metrics for each subset of data
            LOOKBACK_DATA_AGG = lookback_data.groupby(['ACCT_ID']).agg(
                TOTAL_TRXN_AMOUNT=('TRXN_AMOUNT', 'sum')
            ).reset_index()

            # Calculate the sum of transaction amounts for each ACCT_ID
            RISK_PERIOD_AGG = risk_period_data.groupby(['ACCT_ID']).agg(
                TOTAL_TRXN_AMOUNT_RISK_PERIOD=('TRXN_AMOUNT', 'sum')
            ).reset_index()

            # Calculate the average transaction amount by dividing the total by 6
            RISK_PERIOD_AGG['AVG_TRXN_AMOUNT'] = RISK_PERIOD_AGG['TOTAL_TRXN_AMOUNT_RISK_PERIOD'] / 6

            merged_df = pd.merge(LOOKBACK_DATA_AGG, RISK_PERIOD_AGG, on='ACCT_ID', how='left')

            merged_df = merged_df.fillna(0)

            # Calculate the risk percentage, avoiding division by zero
            merged_df['RISK_PERCENTAGE'] = merged_df.apply(
                lambda row: ((row['TOTAL_TRXN_AMOUNT'] - row['AVG_TRXN_AMOUNT']) / row['AVG_TRXN_AMOUNT'] * 100), axis=1
            )

            # Group lookback_data by ACCT_ID and aggregate the TRXN_IDs into a list
            trxn_ids_lookup = lookback_data.groupby('ACCT_ID')['TRXN_ID'].apply(list).reset_index()

            # Merge the TRXN_IDs list into merged_df based on ACCT_ID
            merged_df = pd.merge(merged_df, trxn_ids_lookup, on='ACCT_ID', how='left')

            merged_df['CUST_TYP'] = merged_df['ACCT_ID'].map(cust_type_mapping)
            merged_df['CUSTOMER_RISK'] = merged_df['ACCT_ID'].map(cust_risk_mapping)

            if self.job_details["THRESHOLD_LEVEL_FL"] == "ACTIVE":
                # Define thresholds for different customer types and risk levels
                thresholds_map = {
                    'IND':{
                            'MIN TRXN AMOUNT': float(self.thresholds['MIN TRXN AMOUNT IND LOW']),
                            'MIN RISK PERCENTAGE': int(self.thresholds['MIN RISK PERCENTAGE IND LOW'])

                        },
                        'MEDIUM': {
                            'MIN TRXN AMOUNT': float(self.thresholds['MIN TRXN AMOUNT IND MEDIUM']),
                            'MIN RISK PERCENTAGE': int(self.thresholds['MIN RISK PERCENTAGE IND MEDIUM'])
                        },
                        'HIGH': {
                            'MIN TRXN AMOUNT': float(self.thresholds['MIN TRXN AMOUNT IND HIGH']),
                            'MIN RISK PERCENTAGE': int(self.thresholds['MIN RISK PERCENTAGE IND HIGH'])

                    },
                    'ORG': {
                        'LOW': {
                            'MIN TRXN AMOUNT': float(self.thresholds['MIN TRXN AMOUNT ORG LOW']),
                            'MIN RISK PERCENTAGE': int(self.thresholds['MIN RISK PERCENTAGE ORG LOW'])
                        },
                        'MEDIUM': {
                            'MIN TRXN AMOUNT': float(self.thresholds['MIN TRXN AMOUNT ORG MEDIUM']),
                            'MIN RISK PERCENTAGE': int(self.thresholds['MIN RISK PERCENTAGE ORG MEDIUM'])
                        },
                        'HIGH': {
                            'MIN TRXN AMOUNT': float(self.thresholds['MIN TRXN AMOUNT ORG HIGH']),
                            'MIN RISK PERCENTAGE': int(self.thresholds['MIN RISK PERCENTAGE ORG HIGH'])
                        }
                    }
                }

                def apply_thresholds(row):
                    cust_typ = row['CUST_TYP']
                    cust_risk = row['CUSTOMER_RISK']
                    return thresholds_map[cust_typ][cust_risk]

                merged_df['thresholds'] = merged_df.apply(apply_thresholds, axis=1)

                result_df = merged_df[
                    (merged_df['TOTAL_TRXN_AMOUNT'] >= merged_df['thresholds'].apply(lambda x: x['MIN TRXN AMOUNT'])) &
                    (merged_df['RISK_PERCENTAGE'] >= merged_df['thresholds'].apply(lambda x: x['MIN RISK PERCENTAGE']))

                ]
            else:
                default_thresholds_map = {
                    'IND': {
                        'MIN TRXN AMOUNT': float(self.thresholds['MIN TRXN AMOUNT IND']),
                        'MIN RISK PERCENTAGE': int(self.thresholds['MIN RISK PERCENTAGE IND'])
                    },
                    'ORG': {
                        'MIN TRXN AMOUNT': float(self.thresholds['MIN TRXN AMOUNT ORG']),
                        'MIN RISK PERCENTAGE': int(self.thresholds['MIN RISK PERCENTAGE ORG'])
                    }
                }
                def apply_default_thresholds(row):
                    cust_typ = row['CUST_TYP']
                    return default_thresholds_map[cust_typ]

                merged_df['default_thresholds'] = merged_df.apply(apply_default_thresholds, axis=1)


                result_df = merged_df[
                    (merged_df['TOTAL_TRXN_AMOUNT'] >= merged_df['default_thresholds'].apply(lambda x: x['MIN TRXN AMOUNT'])) &
                    (merged_df['RISK_PERCENTAGE'] >= merged_df['default_thresholds'].apply(lambda x: x['MIN RISK PERCENTAGE']))

                ]

            # Create a mapping between ACCT_ID and Alert_ID
            acct_id_mapping = dict(zip(result_df['ACCT_ID'].unique(), range(1000001, 1000001 + len(result_df['ACCT_ID'].unique()))))

            # Assign Alert_ID based on the mapping
            result_df = result_df.copy()
            result_df['Alert_ID'] = result_df['ACCT_ID'].map(acct_id_mapping)

            # result_df.to_csv('After_Result_TS_SCN_30.csv', index=False)

            alert_data_list = []

            # Iterate through rows of result_df
            for _, row in result_df.iterrows():
                alert_id = row['Alert_ID']
                acct_id = row['ACCT_ID']
                customer_id = lookback_data.loc[lookback_data['ACCT_ID'] == acct_id, 'CUSTOMER_ID'].iloc[0]

                customer_name = lookback_data.loc[lookback_data['ACCT_ID'] == acct_id, 'DISPLAY_NM'].iloc[0]

                account_open_date = lookback_data.loc[lookback_data['ACCT_ID'] == acct_id, 'ACCT_OPEN_DT'].iloc[0]

                account_age_days = (self.current_date - pd.to_datetime(account_open_date)).days

                trxn_id_data = lookback_data[lookback_data['ACCT_ID'] == acct_id]

                trxn_ids = trxn_id_data['TRXN_ID'].tolist()

                # Calculate years and months
                account_age_years = account_age_days // 365
                account_age_months = (account_age_days % 365) // 30

                alert_txt_values = {
                                    "SCENARIO NAME": self.job_details['SCENARIO_NAME'],
                                    "CUSTOMER TYPE": row['CUST_TYP'],
                                    "ACCOUNT AGE": f"{account_age_years} years and {account_age_months} months",
                                    "TOTAL TRXN AMOUNT": round(row['TOTAL_TRXN_AMOUNT'], 2),
                                    "PREVIOUS AVERAGE TRXN AMOUNT": round((row['AVG_TRXN_AMOUNT']), 2),
                                    "RISK PERCENTAGE": round(row['RISK_PERCENTAGE'], 2),

                                    "CUSTOMER RISK LEVEL": row['CUSTOMER_RISK'],
                                    "LOOKBACK PERIOD": f"{cur_mon_end_date.strftime('%Y-%m-%d')}  -  {cur_mon_start_date.strftime('%Y-%m-%d')}"


                                }

                alert_data = pd.DataFrame({
                                'Alert_ID': [alert_id],
                                'ALERT_TX': [alert_txt_values],
                                'TRXN_IDS': [trxn_ids],
                                'ALERT_TYPE_ID': [int(self.job_details['ALERT_TYPE_ID'])],
                                'PRCSNG_BATCH_NM': ['TS_JOB_UST_ACTIVITY'],
                                'JOB_ID': self.job_details['JOB_ID'],
                                'ACCT_ID': acct_id,
                                'CUST_ID': customer_id,
                                'CUSTOMER_NAME': customer_name,
                                'CREATED_DATE': self.current_date.strftime('%Y-%m-%d'),
                                'SCENARIO_ID': f"{(self.job_details['SCENARIO_ID'])}",
                                'THRESHOLD_SET_ID' : self.thresholds['THRESHOLD_SET_ID'],
                                'CONFIG_ID' : int([config['CONFIG_ID'] for config in self.job_details['SCENARIO_CONFIG']][0]),
                                'PREV_MATCH_CT_SM_SCENARIO_CLASS' : [0],
                                'PREV_MATCH_CT_ALL' : [0],
                                'PREV_MATCH_CT' : [0],
                                'BREAK_MATCH_CT' : [0],
                                'THRESHOLD_TX' : [str(self.thresholds)],
                                'ARS_TX': [alert_txt_values],
                                'TRXN_AMOUNT': [round(row['TOTAL_TRXN_AMOUNT'],2)],
                                'PERIOD_START_DATE': cur_mon_end_date.strftime('%Y-%m-%d'),
                                'PERIOD_END_DATE': cur_mon_start_date.strftime('%Y-%m-%d')

                            })

                alert_data_list.append(alert_data)

            if not alert_data_list:
                self.logger.info("No Alerts found!")

                return pd.DataFrame()

            alert_data_df = pd.concat(alert_data_list, ignore_index=True)

            self.logger.info(f"Job ID - {self.job_details['JOB_ID']} | {self.job_details['SCENARIO_NAME']} | Total no. of alerts found - {alert_data_df.shape[0]}")

            # alert_data_df.to_csv('Alert_Table.csv', index=False)


            return alert_data_df


        except Exception as e:
            self.logger.error(f"Error in alert_checker | {self.job_details['SCENARIO_ID']}: {e}")
            raise e



