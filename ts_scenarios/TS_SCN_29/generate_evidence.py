import numpy as np
import pandas as pd
import json
from datetime import datetime
from scenario_logger import logger

class EvidenceReportGenerator:
    def __init__(self, current_date: datetime, job_details: dict, threshold_details: dict):
        """
        Initializes the EvidenceReportGenerator class with current date, job details, and threshold details.

        Args:
            current_date (datetime): The current date for report generation.
            job_details (dict): Details of the job, including scenario name.
            threshold_details (dict): Threshold values to be added to the DataFrame.
        """
        self.current_date = current_date
        self.job_details = job_details
        self.thresholds = threshold_details
        self.logger = logger


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

    def generate_evidence_report(self, dataframe: pd.DataFrame, alert_trxn_table: pd.DataFrame) -> pd.DataFrame:
        """
        Generate an evidence report DataFrame.

        Args:
            evidence_data_df (pd.DataFrame): DataFrame containing evidence data.
            alert_trxn_table (pd.DataFrame): DataFrame containing alert transaction data.

        Returns:
            pd.DataFrame: DataFrame formatted with evidence report details.
        """
        try:
            # Assuming 'ALERT_ID' is present in the 'alert_trxn_table' DataFrame
            alert_ids = alert_trxn_table['Alert_ID'].tolist()

            lookback_start_date, lookback_end_date, cur_mon_start_date, cur_mon_end_date = self.calculate_lookback_dates()

            lookback_start_datetime = pd.to_datetime(f'{lookback_end_date} 00:00:00')

            lookback_end_datetime = pd.to_datetime(f'{lookback_start_date} 23:59:59')


            cur_mon_start_datetime = pd.to_datetime(f'{cur_mon_end_date} 00:00:00')

            cur_mon_end_datetime = pd.to_datetime(f'{cur_mon_start_date} 23:59:59')

            # Filter data based on 'ACCT_ID' in alert_trxn_table
            filtered_data_acct = dataframe[(dataframe['ACCT_ID'].isin(alert_trxn_table['ACCT_ID'].unique())) &
                (dataframe['TRXN_EXCN_DT'].between(lookback_start_datetime, lookback_end_datetime))
            ]

            # Initialize an empty list to store the final result
            final_result_data = []

            # Columns to select from the filtered data
            selected_columns = ['CUSTOMER_ID', 'ACCT_OPEN_DT', 'ACCT_ID', 'TRXN_ID', 'TRXN_AMOUNT','TRXN_EXCN_DT', 'CREDIT_DEBIT_CODE', 'TRXN_TYPE_CD', 'DISPLAY_NM', 'ACCT_TYPE']
            # print(f"Lookback Start Date: {lookback_start_date} | Lookback End Date: {lookback_end_date} | Current Month Start Date: {cur_mon_start_date} | Current Month End Date: {cur_mon_end_date}")

            # Iterate through each 'ALERT_ID' and filter data based on 'TRXN_EXCN_DT'
            for alert_id in alert_ids:
                # Get corresponding 'ACCT_ID' for the current 'ALERT_ID'
                acct_id = alert_trxn_table.loc[alert_trxn_table['Alert_ID'] == alert_id, 'ACCT_ID'].iloc[0]


                filtered_data_alert = filtered_data_acct.loc[filtered_data_acct['ACCT_ID'] == acct_id][selected_columns]

                filtered_data_alert['Alert_ID'] = alert_id

                final_result_data.append(filtered_data_alert)

            evidence_data_df = pd.concat(final_result_data, ignore_index=True)




            # Get unique transaction IDs from the alert table
            trxn_ids_in_alert_table = alert_trxn_table['TRXN_ID'].unique()


            # Create 'ALERT_FLAG' column to mark transactions present in the alert table
            evidence_data_df['ALERT_FLAG'] = np.where(evidence_data_df['TRXN_ID'].isin(trxn_ids_in_alert_table), 'Y', 'N')





            # Create a date range for each ACCT_ID
            date_range = pd.date_range(start=lookback_end_date, end=lookback_start_date, freq='MS')


            # Create a DataFrame with all combinations of ACCT_ID and date_range for 'previous_data_db'
            all_combinations_db = pd.DataFrame([(acct_id, date) for acct_id in evidence_data_df['ACCT_ID'].unique() for date in date_range],
                                            columns=['ACCT_ID', 'TRXN_EXCN_DT'])


            # Convert 'TRXN_EXCN_DT' to datetime in all_combinations_db
            all_combinations_db['TRXN_EXCN_DT'] = pd.to_datetime(all_combinations_db['TRXN_EXCN_DT'])

            evidence_data_df['TRXN_EXCN_DT'] = pd.to_datetime(evidence_data_df['TRXN_EXCN_DT'])



            # Monthly aggregation for 'previous_data_db'
            MONTHY_SUM = evidence_data_df.groupby(['ACCT_ID', pd.Grouper(key='TRXN_EXCN_DT', freq='MS')]).agg(
                MONTHY_SUM=('TRXN_AMOUNT', 'sum')
            ).reset_index()

            MONTHY_SUM = MONTHY_SUM[MONTHY_SUM['TRXN_EXCN_DT'].isin(date_range)]


            # Merge all combinations with aggregated data for 'previous_data_db'
            MONTHY_SUM_DATA = pd.merge(all_combinations_db, MONTHY_SUM, on=['ACCT_ID', 'TRXN_EXCN_DT'], how='outer').fillna(0)

            # Sort by ACCT_ID and TRXN_EXCN_DT in descending order
            MONTHY_SUM_DATA = MONTHY_SUM_DATA.sort_values(by=['ACCT_ID', 'TRXN_EXCN_DT'], ascending=[True, False])

                        # Step 3: Function to create a list of month-wise sums for each 'ACCT_ID'.
            def MONTHLY_SUM(group):
                return [
                    {"Value": row['MONTHY_SUM'], "Month": row['TRXN_EXCN_DT'].strftime('%Y-%m')}
                    for _, row in group.iterrows()
                ]

            # Step 4: Group the data by 'ACCT_ID' and apply the aggregation function.
            MONTHY_SUM_DATA = (
                MONTHY_SUM_DATA
                .groupby('ACCT_ID')
                .apply(MONTHLY_SUM)
                .reset_index(name='TOTAL_SUMS')
            )
            # Filter to keep only the first row for each ACCT_ID (since we want one row per ACCT_ID)
            final_result = MONTHY_SUM_DATA.drop_duplicates(subset=['ACCT_ID']).reset_index(drop=True)

            # Drop unnecessary columns



            # final_result.to_csv('MONTHY_SUM_DATA.csv', index=False)
            evidence_data_df = pd.merge(evidence_data_df, final_result, on=['ACCT_ID'], how='left')
            # evidence_data_df.to_csv('evidence_data_df.csv', index=False)

            evidence_data_df['TRXN_EXCN_DT'] = pd.to_datetime(evidence_data_df['TRXN_EXCN_DT']).dt.strftime('%Y-%m-%d')
            evidence_data_df['TRXN_AMOUNT'] = evidence_data_df['TRXN_AMOUNT'].astype(float)
            evidence_data_df['ACCT_OPEN_DT'] = pd.to_datetime(evidence_data_df['ACCT_OPEN_DT']).dt.strftime('%Y-%m-%d')

            evidence_data_df['LOOKBACK PERIOD'] = self.thresholds['LOOKBACK PERIOD']
            evidence_data_df['MIN TRXN AMOUNT IND'] = self.thresholds['MIN TRXN AMOUNT IND']
            evidence_data_df['MIN TRXN AMOUNT ORG'] = self.thresholds['MIN TRXN AMOUNT ORG']
            evidence_data_df['MIN ACCT AGE IND'] = self.thresholds['MIN ACCT AGE IND']
            evidence_data_df['MIN ACCT AGE ORG'] = self.thresholds['MIN ACCT AGE ORG']
            evidence_data_df['MIN RISK PERCENTAGE IND'] = self.thresholds['MIN RISK PERCENTAGE IND']
            evidence_data_df['MIN RISK PERCENTAGE ORG'] = self.thresholds['MIN RISK PERCENTAGE ORG']

            if self.job_details["THRESHOLD_LEVEL_FL"] == "ACTIVE":
                evidence_data_df['MIN TRXN AMOUNT IND LOW'] = self.thresholds['MIN TRXN AMOUNT IND LOW']
                evidence_data_df['MIN TRXN AMOUNT IND MEDIUM'] = self.thresholds['MIN TRXN AMOUNT IND MEDIUM']
                evidence_data_df['MIN TRXN AMOUNT IND HIGH'] = self.thresholds['MIN TRXN AMOUNT IND HIGH']
                evidence_data_df['MIN TRXN AMOUNT ORG LOW'] = self.thresholds['MIN TRXN AMOUNT ORG LOW']
                evidence_data_df['MIN TRXN AMOUNT ORG MEDIUM'] = self.thresholds['MIN TRXN AMOUNT ORG MEDIUM']
                evidence_data_df['MIN TRXN AMOUNT ORG HIGH'] = self.thresholds['MIN TRXN AMOUNT ORG HIGH']
                evidence_data_df['MIN RISK PERCENTAGE IND LOW'] = self.thresholds['MIN RISK PERCENTAGE IND LOW']
                evidence_data_df['MIN RISK PERCENTAGE IND MEDIUM'] = self.thresholds['MIN RISK PERCENTAGE IND MEDIUM']
                evidence_data_df['MIN RISK PERCENTAGE IND HIGH'] = self.thresholds['MIN RISK PERCENTAGE IND HIGH']
                evidence_data_df['MIN RISK PERCENTAGE ORG LOW'] = self.thresholds['MIN RISK PERCENTAGE ORG LOW']
                evidence_data_df['MIN RISK PERCENTAGE ORG MEDIUM'] = self.thresholds['MIN RISK PERCENTAGE ORG MEDIUM']
                evidence_data_df['MIN RISK PERCENTAGE ORG HIGH'] = self.thresholds['MIN RISK PERCENTAGE ORG HIGH']




            def extract_alert_tx(acct_id):
                alert_tx_rows = alert_trxn_table.loc[alert_trxn_table['ACCT_ID'] == acct_id, 'ALERT_TX'].values
                return alert_tx_rows[0] if len(alert_tx_rows) > 0 else {}

            # Get all unique keys present in ALERT_TX across all rows
            all_keys = set()
            for alert_tx_dict in alert_trxn_table['ALERT_TX']:
                all_keys.update(alert_tx_dict.keys())

            # Apply the function to create separate columns for each key in ALERT_TX
            for key in all_keys:
                evidence_data_df[key] = evidence_data_df['ACCT_ID'].apply(lambda x: extract_alert_tx(x).get(key, ''))



            # evidence_data_df.to_csv('Unagg_Evidence-TS_SCN_30.csv', index=False)


            # Aggregate all records into a list of dictionaries for each Alert_ID
            aggregated_data = (evidence_data_df.groupby('Alert_ID').apply(lambda group: group.drop('Alert_ID', axis=1).to_dict(orient='records')).tolist())

            evidence_df = pd.DataFrame({'Alert_ID': evidence_data_df['Alert_ID'].unique(), 'RECORDS': aggregated_data})

            evidence_df['CREATE_DT'] = self.current_date.strftime('%Y-%m-%d')
            evidence_df['SCNRO_NM'] = self.job_details['SCENARIO_NAME']


            return evidence_df

        except Exception as e:
            # Handle exceptions gracefully and log the error
            self.logger.error(f"Error occurred while generating the evidence report: {e}")
            raise e
