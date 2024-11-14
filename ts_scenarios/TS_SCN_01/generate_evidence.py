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
            alert_ids = alert_trxn_table['Alert_ID'].unique().tolist()
            evidence_start_date = self.current_date

            evidence_end_date = evidence_start_date - pd.DateOffset(days=90)
            evidence_data_df = dataframe[(dataframe['CUSTOMER_ID'].isin(alert_trxn_table['CUST_ID'].unique())) &
            (dataframe['TRXN_EXCN_DT'] >= evidence_end_date.date()) &
            (dataframe['TRXN_EXCN_DT'] <= evidence_start_date.date())].copy()

            # Initialize an empty list to store the final result
            final_result_data = []

            # Iterate through each 'ALERT_ID' and filter data based on 'TRXN_EXCN_DT'
            for alert_id in alert_ids:
                # Get corresponding 'ACCT_ID' for the current 'ALERT_ID'
                cust_id = alert_trxn_table.loc[alert_trxn_table['Alert_ID'] == alert_id, 'CUST_ID'].iloc[0]


                # Filter the data for the specific 'CUST_ID'
                filtered_data_alert = evidence_data_df.loc[evidence_data_df['CUSTOMER_ID'] == cust_id].copy()

                # Add the 'Alert_ID' column to the filtered data
                filtered_data_alert['Alert_ID'] = alert_id

                final_result_data.append(filtered_data_alert)

            evidence_data_df = pd.concat(final_result_data, ignore_index=True)


            # Get unique transaction IDs from the alert table
            trxn_ids_in_alert_table = set(alert_trxn_table['TRXN_ID'].unique())

            # Create 'ALERT_FLAG' column to mark transactions present in the alert table
            evidence_data_df['ALERT_FLAG'] = np.where(evidence_data_df['TRXN_ID'].isin(trxn_ids_in_alert_table), 'Y', 'N')

            # Extract keys from the first 'ALERT_TX' entry for creating new columns
            alert_tx_keys = alert_trxn_table['ALERT_TX'].iloc[0].keys() if not alert_trxn_table.empty else []

            # Map ALERT_TX values to corresponding TRXN_IDs for faster lookup
            alert_tx_dict = alert_trxn_table.set_index('TRXN_ID')['ALERT_TX'].to_dict()

            # Create new columns for each key in 'ALERT_TX' using the mapped dictionary
            for key in alert_tx_keys:
                evidence_data_df[key] = evidence_data_df['TRXN_ID'].map(lambda x: alert_tx_dict.get(x, {}).get(key, ''))



            # Set 'TRXN RISK TYPE' based on 'HIGHRISK_FLAG'
            evidence_data_df['TRXN RISK TYPE'] = np.where(
                evidence_data_df['HIGHRISK_FLAG'] == 'Y',
                'HRG',
                'NORMAL'
            )

            # Format date columns to 'YYYY-MM-DD'
            for col in ['ACCT_OPEN_DT', 'TRXN_EXCN_DT', 'UPDATED_DATE', 'DT_OF_BIRTH', 'TRXN_POST_DT']:
                if col in evidence_data_df.columns:
                    evidence_data_df[col] = pd.to_datetime(evidence_data_df[col]).dt.strftime('%Y-%m-%d').fillna('')

            # Add threshold values as new columns
            for key, value in self.thresholds.items():
                evidence_data_df[key] = round(float(value), 2)

            evidence_data_df = evidence_data_df.fillna('')


            # Group data by 'Alert_ID' and convert each group to a list of dictionaries
            grouped_records_json = evidence_data_df.groupby('Alert_ID').apply(
                lambda group: group.drop('Alert_ID', axis=1).to_dict(orient='records')
            ).tolist()

            # Create final DataFrame for JSON output
            evidence_df = pd.DataFrame({
                'Alert_ID': evidence_data_df['Alert_ID'].unique(),
                'RECORDS': grouped_records_json
            })

            # Add additional metadata columns
            evidence_df['CREATE_DT'] = self.current_date.strftime('%Y-%m-%d')
            evidence_df['SCNRO_NM'] = self.job_details.get('SCENARIO_NAME', '')

            return evidence_df

        except Exception as e:
            # Handle exceptions gracefully and log the error
            self.logger.error(f"Error occurred while generating the evidence report: {e}")
            raise e
