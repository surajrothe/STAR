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
        self.logger.info("Initializing AlertChecker for TS_SCN_01")


    def check_alerts(self, dataframe: pd.DataFrame) -> pd.DataFrame:

        try:
            # dataframe.to_csv('Data.csv', index=False)
            lookback_start_date = self.current_date

            lookback_end_date = lookback_start_date - pd.DateOffset(days=int(self.thresholds['LOOKBACK PERIOD']) - 1)

            # Copy TRXN_POST_DT  to TRXN_EXCN_DT if TRXN_TYPE_CD starts with 'CHECK' or 'check'
            dataframe.loc[dataframe['TRXN_TYPE_CD'].str.startswith(('CHECK', 'check')), 'TRXN_EXCN_DT'] = dataframe['TRXN_POST_DT']

            # Convert date columns to datetime type
            dataframe['TRXN_EXCN_DT'] = pd.to_datetime(dataframe['TRXN_EXCN_DT'], format='%Y-%m-%d %H:%M:%S')
            dataframe['TRXN_EXCN_DT'] = dataframe['TRXN_EXCN_DT'].dt.date

            dataframe['TRXN_POST_DT'] = pd.to_datetime(dataframe['TRXN_POST_DT'], format='%Y-%m-%d %H:%M:%S')
            dataframe['TRXN_POST_DT'] = dataframe['TRXN_POST_DT'].dt.date

            dataframe['ACCT_OPEN_DT'] = pd.to_datetime(dataframe['ACCT_OPEN_DT'], format='%Y-%m-%d')
            dataframe['ACCT_OPEN_DT'] = dataframe['ACCT_OPEN_DT'].dt.date

            # Convert date columns to datetime type and extract only the date

            # dataframe['TRXN_EXCN_DT'] = pd.to_datetime(dataframe['TRXN_EXCN_DT'], format='%Y-%m-%d %H:%M:%S').dt.date
            # dataframe['TRXN_POST_DT'] = pd.to_datetime(dataframe['TRXN_POST_DT'], format='%Y-%m-%d %H:%M:%S').dt.date
            # dataframe['ACCT_OPEN_DT'] = pd.to_datetime(dataframe['ACCT_OPEN_DT'], format='%Y-%m-%d %H:%M:%S').dt.date

            # For ACCT_OPEN_DT, since the format doesn't include time, no need for normalize
            # dataframe['ACCT_OPEN_DT'] = pd.to_datetime(dataframe['ACCT_OPEN_DT'], format='%Y-%m-%d').dt.date



            # Filter data based on conditions
            filtered_data_ind = dataframe[(dataframe['CUST_TYP'] == 'IND') & (dataframe['TRXN_AMOUNT'] >= float(self.thresholds['INDIVIDUAL TRXN AMOUNT IND'])) &
                (dataframe['TRXN_EXCN_DT'].between(lookback_end_date.date(), lookback_start_date.date()))
            ]

            filtered_data_org = dataframe[(dataframe['CUST_TYP'] == 'ORG') & (dataframe['TRXN_AMOUNT'] >= float(self.thresholds['INDIVIDUAL TRXN AMOUNT ORG'])) &
                (dataframe['TRXN_EXCN_DT'].between(lookback_end_date.date(), lookback_start_date.date()))
            ]

            filtered_data = pd.concat([filtered_data_ind, filtered_data_org], ignore_index=True)

            # filtered_data.to_csv('Filtered_Data.csv', index=False)

            filtered_trxn_data = filtered_data.groupby(['TRXN_EXCN_DT', 'ACCT_ID', 'CUSTOMER_ID', 'HIGHRISK_FLAG'])['TRXN_AMOUNT'].sum().reset_index()

            # filtered_trxn_data.to_csv('Filtered_TRXN_Data.csv', index=False)


            unique_customer_ids = filtered_trxn_data['CUSTOMER_ID'].unique()

            alert_table = pd.DataFrame()

            alerts_and_records = []

            alerts = []

            for customer_id in unique_customer_ids:

                customer_data = filtered_data[filtered_data['CUSTOMER_ID'] == customer_id]
                customer_data = customer_data.sort_values(by='TRXN_EXCN_DT')

                # Fetch the CUST_TYP for the given customer_id
                cust_type = dataframe[dataframe['CUSTOMER_ID'] == customer_id]['CUST_TYP'].iloc[0] # Assuming customer_id is unique and there is at least one row

                cust_risk = dataframe[dataframe['CUSTOMER_ID'] == customer_id]['CUSTOMER_RISK'].iloc[0]

                customer_data = customer_data.groupby(['TRXN_EXCN_DT', 'ACCT_ID', 'CUSTOMER_ID', 'DISPLAY_NM', 'HIGHRISK_FLAG'])['TRXN_AMOUNT'].sum().reset_index()

                customer_data['TRXN_RISK_TYPE'] = 'NORMAL'
                customer_data.loc[customer_data['HIGHRISK_FLAG'] == 'N', 'TRXN_RISK_TYPE'] = 'NORMAL'

                customer_data.loc[customer_data['HIGHRISK_FLAG'] == 'Y', 'TRXN_RISK_TYPE'] = 'HRG'

                customer_data['Alert_Flag'] = False

                frequency_start_date = None    # Initialize the start date
                frequency_end_date = None     # Initialize the end date

                sum_temp = 0    # Initialising sum_temp

                # Initialize a list to keep track of indices to update
                dates_to_update = []

                for outer_index, outer_row in customer_data[::-1].iterrows():

                    # Check for the condition and skip if Outer_Alert_Flag is True
                    if customer_data.at[outer_index, 'Alert_Flag'] == True:
                        continue

                    if outer_row['HIGHRISK_FLAG'] == 'N':
                        continue

                    if outer_row['TRXN_EXCN_DT'] in dates_to_update:
                        continue

                    else:
                        if frequency_start_date is None and outer_row['Alert_Flag'] == False:

                            # Set the start date to the current row's Trans_Date
                            frequency_start_date = outer_row['TRXN_EXCN_DT']

                            # Set the end date to the start date minus the frequency period
                            frequency_end_date = frequency_start_date - pd.Timedelta(days=int(self.thresholds['FREQUENCY PERIOD']) - 1)

                            sum_temp = 0

                            dates_to_update = []

                        reversed_indices = list(customer_data.index[::-1])
                        for inner_index in reversed_indices[reversed_indices.index(outer_index):]:

                            inner_row = customer_data.loc[inner_index]
                            if inner_row['Alert_Flag'] == True:
                                continue

                            if inner_row['HIGHRISK_FLAG'] == 'N':
                                # print(f"Skipping beacause N flag")
                                continue

                            if inner_row['TRXN_EXCN_DT'] in dates_to_update:
                                continue

                            if inner_row['TRXN_EXCN_DT'] < frequency_end_date:
                                sum_temp = 0

                                dates_to_update = []

                                frequency_start_date = None
                                frequency_end_date = None
                                break

                            # Check if inner_row's Trans_Date is within the current frequency period
                            if frequency_start_date is not None:

                                # Sum the Trans_Amount for the current frequency period
                                sum_temp += inner_row['TRXN_AMOUNT']
                                # print(f"High risk flag - {inner_row['HIGHRISK_FLAG']} and sum_temp is {sum_temp}")

                                start_date = frequency_start_date
                                end_date = inner_row['TRXN_EXCN_DT']

                                # print(f"Start Date - {start_date} and End Date - {end_date}")

                                Total_Trans_Amount = sum_temp


                                Total_All_Trans_Amount = customer_data[(customer_data['TRXN_EXCN_DT'] >= end_date) &
                                                (customer_data['TRXN_EXCN_DT'] <= start_date)]['TRXN_AMOUNT'].sum()


                                Total_HRG_Trans_Ct = len(customer_data[(customer_data['TRXN_EXCN_DT'] >= end_date) &
                                                (customer_data['TRXN_EXCN_DT'] <= start_date) & (customer_data['HIGHRISK_FLAG'] == 'Y')])

                                # print(f"Total transactions Count - {Total_HRG_Trans_Ct}")

                                Total_HRG_Trans_Amount = customer_data[(customer_data['TRXN_EXCN_DT'] >= end_date) &
                                                (customer_data['TRXN_EXCN_DT'] <= start_date) & (customer_data['HIGHRISK_FLAG'] == 'Y')]['TRXN_AMOUNT'].sum()

                                # print(f"Total_HRG_Trans_Amount - {Total_HRG_Trans_Amount}")

                                if cust_type == 'IND':


                                    # Check if sum_temp meets the threshold
                                    primary_condition = sum_temp >= float(self.thresholds['HRG TRXN AMOUNT IND'])


                                    condition3 = (
                                        (Total_HRG_Trans_Ct >= int(self.thresholds['HRG TRXN COUNT IND'])) &
                                        (Total_HRG_Trans_Amount >= float(self.thresholds['HRG TRXN AMOUNT IND'])) &
                                        (100 * (Total_HRG_Trans_Amount / Total_All_Trans_Amount) >= float(self.thresholds['HRG % AMOUNT IND'])) &
                                        (inner_row['HIGHRISK_FLAG'] == 'Y')
                                    )
                                else:
                                    # Check if sum_temp meets the threshold
                                    primary_condition = sum_temp >= float(self.thresholds['HRG TRXN AMOUNT ORG'])

                                    condition3 = (
                                        (Total_HRG_Trans_Ct >= int(self.thresholds['HRG TRXN COUNT ORG'])) &
                                        (Total_HRG_Trans_Amount >= float(self.thresholds['HRG TRXN AMOUNT ORG'])) &
                                        (100 * (Total_HRG_Trans_Amount / Total_All_Trans_Amount) >= float(self.thresholds['HRG % AMOUNT ORG'])) &
                                        (inner_row['HIGHRISK_FLAG'] == 'Y')
                                    )

                                if primary_condition and condition3:

                                    # Group by 'Trans_Date', 'Customer_ID', 'Transaction_Type', and 'TS_country_name' and sum 'Trans_Amount'

                                    trxn_id_data = filtered_data[(filtered_data['CUSTOMER_ID'] == customer_id) & (filtered_data['TRXN_EXCN_DT'] >= end_date) &
                                        (filtered_data['TRXN_EXCN_DT'] <= start_date) & (filtered_data['HIGHRISK_FLAG'] == 'Y')]

                                    trxn_ids = trxn_id_data['TRXN_ID'].tolist()


                                    alert_id = f'{self.alert_id_counter:07}'
                                    self.alert_id_counter += 1

                                    dates_to_update.append(inner_row['TRXN_EXCN_DT'])


                                    alert_txt_values = {
                                        "SCENARIO NAME" : self.job_details['SCENARIO_NAME'],

                                        "CUSTOMER TYPE": cust_type,

                                        "FREQUENCY PERIOD": f"{end_date.strftime('%Y-%m-%d')}  -  {start_date.strftime('%Y-%m-%d')}",

                                        "LOOKBACK PERIOD": f"{lookback_end_date.strftime('%Y-%m-%d')} - {lookback_start_date.strftime('%Y-%m-%d')}",

                                        "TOTAL TRXN AMOUNT" : round(Total_All_Trans_Amount, 2),

                                        "TOTAL HRG TRXN AMOUNT" : round(Total_HRG_Trans_Amount, 2),

                                        "TOTAL HRG TRXN COUNT" : Total_HRG_Trans_Ct,

                                        "PERCENTAGE TOTAL HRG TRXN AMOUNT" : round(100 * (Total_HRG_Trans_Amount / Total_All_Trans_Amount), 2) if Total_Trans_Amount != 0 else 0.0,

                                        "CUSTOMER RISK LEVEL": cust_risk

                                    }
                                    alerts.append(alert_txt_values)


                                    alert_data = pd.DataFrame({
                                        'Alert_ID': [alert_id],
                                        'ALERT_TX': [alert_txt_values],
                                        'TRXN_IDS': [trxn_ids],
                                        'PRCSNG_BATCH_NM': [self.job_details['JOB_NAME']],
                                        'JOB_ID': [self.job_details['JOB_ID']],
                                        'ACCT_ID': [', '.join(filtered_data[filtered_data['CUSTOMER_ID'] == inner_row['CUSTOMER_ID']]['ACCT_ID'].unique().tolist())],
                                        'CUST_ID': inner_row['CUSTOMER_ID'],
                                        'CUSTOMER_NAME': inner_row['DISPLAY_NM'],
                                        'CREATED_DATE': self.current_date.strftime('%Y-%m-%d'),
                                        'SCENARIO_ID': self.job_details['SCENARIO_ID'],
                                        'PERIOD_START_DATE': end_date.strftime('%Y-%m-%d'),
                                        'PERIOD_END_DATE': start_date.strftime('%Y-%m-%d'),
                                        'THRESHOLD_SET_ID' : self.thresholds['THRESHOLD_SET_ID'],
                                        'CONFIG_ID' : int([config['CONFIG_ID'] for config in self.job_details['SCENARIO_CONFIG']][0]),
                                        'PREV_MATCH_CT_ALL' : [0],
                                        'PREV_MATCH_CT' : [0],
                                        'TRXN_AMOUNT': round(Total_HRG_Trans_Amount, 2),
                                        'BREAK_MATCH_CT' : [0],
                                        'ALERT_TYPE_ID': [int(self.job_details['ALERT_TYPE_ID'])],
                                        'THRESHOLD_TX' : [str(self.thresholds)],
                                        'ARS_TX': [alert_txt_values]



                                    })

                                    alerts_and_records.append(alert_data)

                                    # Reset
                                    sum_temp = 0
                                    frequency_start_date = None
                                    frequency_end_date = None
                                    break


                                else:
                                    dates_to_update.append(inner_row['TRXN_EXCN_DT'])

                        sum_temp = 0

                    # After the inner loop, update the flags for both inner and outer indices
                    customer_data.loc[customer_data['TRXN_EXCN_DT'].isin(dates_to_update), ['Alert_Flag']] = True

            if not alerts_and_records or not alerts:
                self.logger.info("No Alerts found!")

                return pd.DataFrame()

            # alert_df = pd.DataFrame(alerts)

            alert_table = pd.concat(alerts_and_records, ignore_index=True)

            self.logger.info(f"Total alerts found - {len(alert_table)}")



            # alert_table.to_csv('ALert_HRG_old.csv', index=False)
            # alert_table.to_csv('Alert_Table_Zero_Threshold.csv', index=False)


            # return alert_table, alert_trxn_table, evidence_report
            return alert_table

        except Exception as e:
            self.logger.error(f"Error in alert_checker | TS_SCN_01: {e}")
            raise e



