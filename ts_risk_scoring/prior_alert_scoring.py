import pandas as pd
import logging
from ts_fetcher.util import SQLQueryExecutor
from datetime import datetime
from typing import Tuple, List
from scenario_logger import logger
class PriorAlertScorer:
    def __init__(self, prior_alert_query_base_path: str, job_details: dict, score_bucket: dict, alert_data: pd.DataFrame) -> None:
        self.sql_executor = SQLQueryExecutor()
        self.job_details = job_details
        self.logger = logger
        self.logger.info(f"{self.job_details['SCENARIO_NAME']} : calculating Prior Alert Scores.")
        self.score_bucket = score_bucket
        self.prior_alert_query_base_path = prior_alert_query_base_path
        self.alert_data = alert_data

    def fetch_prior_alert_data(self, prior_alert_query_base_path: str, alert_data: pd.DataFrame, lookback_time: int) -> pd.DataFrame:
        """
        Fetches prior alerts within the specified lookback period.

        Parameters:
        prior_alert_query_base_path (str): Path to the base SQL query file for prior alerts.
        alert_data (pd.DataFrame): DataFrame containing current alert data.
        lookback_time (int): Number of months to look back for prior alerts.

        Returns:
        pd.DataFrame: DataFrame containing the prior alerts.
        """
        try:
            # print(f"Alert data: {alert_data.columns}")
            # Calculate the date lookback_time months ago from the current date
            six_months_ago = datetime.now() - pd.DateOffset(months=lookback_time)

            if self.job_details['SCENARIO_FOCUS'] == 'ACCOUNT':
                acct_id_str = ', '.join(map(lambda x: f"'{x}'", alert_data['ACCT_ID']))
                scenario_focus_string = f"TS_ALERT_CUST.ACCT_ID IN ({acct_id_str})"
            elif self.job_details['SCENARIO_FOCUS'] == 'CUSTOMER':
                cust_ids_str = ', '.join(map(lambda x: f"'{x}'", alert_data['CUST_ID']))
                scenario_focus_string = f"TS_ALERT_CUST.CUST_ID IN ({cust_ids_str})"
            else:
                raise ValueError("Invalid SCENARIO_FOCUS specified.")

            prior_alert_base_query = self.sql_executor.load_sql_query(prior_alert_query_base_path)

            query_prior_alert_data = f"""
                    {prior_alert_base_query}
                    WHERE
                    {scenario_focus_string}
                    AND TS_ALERT.CREATED_DATE >= '{six_months_ago.strftime('%Y-%m-%d')}'
                """

            prior_alert_data_df = self.sql_executor.execute_query(query_prior_alert_data)
            return prior_alert_data_df
        except Exception as e:
            self.logger.error(f"Error fetching prior alert data: {e}")
            return pd.DataFrame()

    def prior_alert_score(self, row: pd.Series, prior_df: pd.DataFrame) -> Tuple[int, List[str], List[str]]:
        """
        Calculates the prior alert score based on the prior alert data.

        Parameters:
        row (pd.Series): A row of the alert_data DataFrame.
        prior_df (pd.DataFrame): DataFrame containing prior alert data.

        Returns:
        Tuple[int, List[str], List[str]]: Score, statuses, and auto-reason flags.
        """
        entity_id = str(row['ACCT_ID']).strip() if self.job_details['SCENARIO_FOCUS'] == 'ACCOUNT' else str(row['CUST_ID']).strip()
        prior_entity_ids_str = ','.join(prior_df['ACCT_ID'].astype(str).values) if self.job_details['SCENARIO_FOCUS'] == 'ACCOUNT' else ','.join(prior_df['CUST_ID'].astype(str).values)

        prior_alert_score = int(next((attr['ATTR_SCORE'] for attr in self.score_bucket['PRIOR_ALERT']['ATTRIBUTES']['ALERT_STATUSES'] if attr['ALERT_STATUS'] == 'NEW'), 0))
        statuses, auto_reason, score = [], [], []

        if not prior_df.empty and entity_id in prior_entity_ids_str:
            matching_row = prior_df[prior_df['ACCT_ID'] == entity_id].drop_duplicates(subset='ALERT_ID') if self.job_details['SCENARIO_FOCUS'] == 'ACCOUNT' else prior_df[prior_df['CUST_ID'] == entity_id].drop_duplicates(subset='ALERT_ID')

            if not matching_row.empty:
                for _, alert_row in matching_row.iterrows():
                    prior_alert_status = alert_row['STATUS_CD'] if alert_row['STATUS_CD'] else 'NEW'
                    auto_reason_fl = alert_row['AUTO_REASN_FL']

                    if prior_alert_status == 'ESCALATED' and alert_row['SAR_ID'] is not None:
                        sar_score = int(next((attr['ATTR_SCORE'] for attr in self.score_bucket['PRIOR_ALERT']['ATTRIBUTES']['ALERT_STATUSES'] if attr['ALERT_STATUS'] == 'SAR'), 0))
                        return sar_score, ['SAR'], []

                    if prior_alert_status in ('REOPEN', 'PARKED', 'ASSIGNED'):
                        prior_alert_status = 'OTHER'

                    statuses.append(prior_alert_status)
                    score.append(int(next((attr['ATTR_SCORE'] for attr in self.score_bucket['PRIOR_ALERT']['ATTRIBUTES']['ALERT_STATUSES'] if attr['ALERT_STATUS'] == prior_alert_status), 0)))
                    auto_reason.append(auto_reason_fl)

                final_score = sum(status_score for status_score in score) / len(statuses) if statuses else prior_alert_score
                return final_score, statuses, auto_reason
            else:
                return prior_alert_score, [], []



        return prior_alert_score, [], []

    def run_prior_alert_scoring(self) -> pd.DataFrame:
        """
        Executes the prior alert scoring process and returns the updated alert data with scores.

        Returns:
        pd.DataFrame: Updated alert data with prior alert scores.
        """
        try:
            prior_df = self.fetch_prior_alert_data(self.prior_alert_query_base_path, self.alert_data, 9)
            prior_alert_data_df = self.alert_data.copy()
            prior_alert_data_df['PRIOR_ALERT_SCORE'], prior_alert_data_df['LIST_PRIOR_ALERTS'], prior_alert_data_df['AUTO_REASON'] = zip(*prior_alert_data_df.apply(lambda row: self.prior_alert_score(row, prior_df), axis=1))
            # In cases where one Alert_ID is associated with more than one distinct customer_id/account_id, we take the maximum score for that alert
            prior_alert_data_df = prior_alert_data_df.groupby('Alert_ID').max().reset_index()
            # prior_alert_data_df.to_csv('prior_alert_score.csv', index=False)
            return prior_alert_data_df[['Alert_ID','PRIOR_ALERT_SCORE','LIST_PRIOR_ALERTS','AUTO_REASON']]
        except Exception as e:
            self.logger.error(f"Error running prior alert scoring: {e}")
            # return pd.DataFrame()
            raise e
