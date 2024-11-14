import pandas as pd
from typing import Dict
import pandas as pd
from ts_scenarios.util import import_alert_checker, import_evidence_generator, apply_scenario_config
from ts_risk_scoring.alert_risk_scoring import AlertRiskScorer
from scenario_logger import logger
from ts_fetcher.util import SQLQueryExecutor
from ts_pusher.data_pusher import AlertDataPusher
import os
import glob
from datetime import datetime
class ScenarioManager:
    def __init__(self, scenario_id: int, job_details: Dict, threshold_details: Dict):
        """
        Initializes the ScenarioManager instance.

        Args:
            scenario_id (int): The ID of the scenario.
            job_details (Dict): Details of the job.
            threshold_details (Dict): Threshold details for the scenario.
        """
        # self.current_date = pd.to_datetime('2024-09-26')
        self.current_date = pd.to_datetime(datetime.now())
        self.job_details = job_details
        self.threshold_details = threshold_details
        self.scenario_id = scenario_id
        self.logger = logger
        self.sql_executor = SQLQueryExecutor()


    def _initialize_alert_checker(self):
        """
        Imports and initializes the ALERTCHECKER module.

        Returns:
            alert_checker: An instance of the ALERTCHECKER.
        """
        try:
            alert_checker = import_alert_checker(self.scenario_id)
            self.logger.info("ALERTCHECKER initialized successfully.")
            return alert_checker
        except Exception as e:
            self.logger.error(f"Failed to initialize ALERTCHECKER: {e}", exc_info=True)
            raise

    def _initialize_evidence_generator(self):
        """
        Imports and initializes the EvidenceReportGenerator module.

        Returns:
            evidence_generator: An instance of the EvidenceReportGenerator.
        """
        try:
            evidence_generator = import_evidence_generator(self.scenario_id)
            self.logger.info("EvidenceReportGenerator initialized successfully.")
            return evidence_generator
        except Exception as e:
            self.logger.error(f"Failed to initialize EvidenceReportGenerator: {e}", exc_info=True)
            raise

    def process_with_alert_checker(self, df: pd.DataFrame) -> bool:
        """
        Processes a DataFrame using the ALERTCHECKER.

        Args:
            df (pandas.DataFrame): The DataFrame containing the transaction data.

        Returns:
            bool: True if processing is successful, False otherwise.
        """

        try:

            AlertChecker = self._initialize_alert_checker()
            alert_generator = AlertChecker(self.current_date, self.job_details, self.threshold_details)
        except Exception as e:
            self.logger.error(f"Error initializing ALERTCHECKER: {e}", exc_info=True)
            return False, 0

        try:
            EvidenceReportGenerator = self._initialize_evidence_generator()
            evidence_generator = EvidenceReportGenerator(self.current_date, self.job_details, self.threshold_details)
        except Exception as e:
            self.logger.error(f"Error initializing EvidenceReportGenerator: {e}", exc_info=True)
            return False, 0



        try:
            alert_table = alert_generator.check_alerts(df)
            self.logger.info(f"Successfully processed data with ALERTCHECKER. Total Alert - {len(alert_table)}")

            print(f"Total Alert - {len(alert_table)}")

            if not alert_table.empty:
                alert_trxn_table = self.generate_trxn_table(alert_table)
                self.logger.info(f"Generated alert transaction table with {len(alert_trxn_table)} rows.")

                evidence_table = evidence_generator.generate_evidence_report(df, alert_trxn_table)
                self.logger.info(f"Generated evidence report with {len(evidence_table)} rows.")

                alert_acct_cust_table = self.alert_cust_acct_mapper(alert_table)
                self.logger.info(f"Generated alert customer account mapping table with {len(alert_acct_cust_table)} rows.")


                ars_table = self.alert_score_table(alert_table, alert_trxn_table, df)


                ars_table.to_csv('ars_table.csv', index=False)
                alert_table = self.get_alert_prev_match_ct(alert_table)


                alert_pusher = AlertDataPusher()

                alert_pusher.push_alerts(
                    self.job_details, alert_table, alert_trxn_table, ars_table, evidence_table, alert_acct_cust_table, self.current_date
                )

            else:
                self.logger.info("No alerts generated!")

            return True, len(alert_table)

        except Exception as e:
            self.logger.error(f"Error processing data with ALERTCHECKER: {e}", exc_info=True)
            return False, 0

    def generate_trxn_table(self, alert_table: pd.DataFrame) -> pd.DataFrame:
        """
        Generates a transaction table from the alert table.

        Args:
            alert_table (pandas.DataFrame): The DataFrame containing alert data.

        Returns:
            pandas.DataFrame: The generated transaction table.
        """

        try:
            result_list = []

            for _, row in alert_table.iterrows():
                alert_id = row['Alert_ID']
                alert_tx = row['ALERT_TX']
                trxns_ids_list = row['TRXN_IDS']

                for trxns_id in trxns_ids_list:
                    result_list.append({'Alert_ID': alert_id, 'ALERT_TX': alert_tx, 'TRXN_ID': trxns_id})

            result_df = pd.DataFrame(result_list)
            result_df = pd.merge(result_df, alert_table[['Alert_ID', 'ARS_TX', 'CUST_ID', 'ACCT_ID']], on='Alert_ID', how='left')

            return result_df
        except Exception as e:
            self.logger.error(f"Error in generate_trxn_table: {e}", exc_info=True)
            raise e

    def alert_cust_acct_mapper(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Maps customer and account IDs to generate a comprehensive table.

        Args:
            df (pandas.DataFrame): The DataFrame containing alert data.

        Returns:
            pandas.DataFrame: The mapped customer and account table.
        """

        try:
            df['ACCT_ID_SPLIT'] = df['ACCT_ID'].str.split(',')
            df['CUST_ID_SPLIT'] = df['CUST_ID'].str.split(',')

            expanded_rows = []

            for _, row in df.iterrows():
                acct_ids = [acct_id.strip() for acct_id in row['ACCT_ID_SPLIT']]
                cust_ids = [cust_id.strip() for cust_id in row['CUST_ID_SPLIT']]

                if len(acct_ids) == len(cust_ids):
                    for acct_id, cust_id in zip(acct_ids, cust_ids):
                        expanded_rows.append({'Alert_ID': row['Alert_ID'], 'ACCT_ID': acct_id, 'CUST_ID': cust_id})

                elif len(cust_ids) == 1 and len(acct_ids) > 1:
                    for acct_id in acct_ids:
                        expanded_rows.append({'Alert_ID': row['Alert_ID'], 'ACCT_ID': acct_id, 'CUST_ID': cust_ids[0]})

                elif len(acct_ids) == 1 and len(cust_ids) > 1:
                    for cust_id in cust_ids:
                        expanded_rows.append({'Alert_ID': row['Alert_ID'], 'ACCT_ID': acct_ids[0], 'CUST_ID': cust_id})

                else:
                    raise ValueError(f"Unhandled case for ALERT_ID {row['Alert_ID']}")

            df_expanded = pd.DataFrame(expanded_rows)
            # df_expanded.to_csv('alert_cust_acct_mapper.csv', index=False)
            return df_expanded
        except Exception as e:
            self.logger.error(f"Error in alert_cust_acct_mapper: {e}", exc_info=True)
            raise e

    def alert_score_table(self, alert_df: pd.DataFrame, alert_trxn_df: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generates a table with alert scores.

        Args:
            alert_df (pandas.DataFrame): The DataFrame containing alert data.
            alert_trxn_df (pandas.DataFrame): The DataFrame containing alert transaction data.
            df (pandas.DataFrame): The DataFrame containing transaction data.

        Returns:
            pandas.DataFrame: The generated alert score table.
        """
        try:

            selected_inserted_data = alert_df[['Alert_ID', 'CREATED_DATE']]
            merged_alert_data = pd.merge(alert_trxn_df, selected_inserted_data, on='Alert_ID', how='inner')

            # print(f"merged_alert_data columns  -{merged_alert_data.columns}")

            result_df = pd.merge(merged_alert_data, df[['LEDGER_BAL_BASE', 'UPDATED_DATE', 'ACCT_NET_WRTH_BASE',
                                                            'ACCT_OPEN_DT', 'ACCT_TYPE', 'CUSTOMER_ID', 'BORROWER_INCOME',
                                                            'TRXN_ID', 'TRXN_AMOUNT', 'TRXN_EXCN_DT', 'TRXN_INSTRMT_NUM',
                                                            'TRXN_POST_DT', 'TRXN_CRSS_BRDR', 'TRXN_TYPE_CD', 'TRANSACTION_CHANNEL',
                                                            'TRXN_TYPE_ID', 'PRODUCT_TYPE', 'BENEFICIARY_NAME', 'INTERNAL_ACCOUNT',
                                                            'ORIGINATOR_NAME', 'TRXN_PARTY_ID', 'BENEFICIARY_BANK_NAME',
                                                            'CREDIT_DEBIT_CODE', 'CUSTOMER_RISK', 'DISPLAY_NM', 'AGE', 'CUST_TYP',
                                                            'IS_PEP', 'CTZNSHIP_STS', 'DT_OF_BIRTH', 'PARTY_CNTRY_NAME',
                                                            'HIGHRISK_FLAG', 'CNTRY_OF_RESIDENCE', 'CNTRY_OF_INCORP']], on='TRXN_ID', how='inner')
            # result_df = pd.merge(merged_alert_data, df, on=['TRXN_ID', 'ACCT_ID'], how='inner')
            result_df = result_df.loc[:, ~result_df.columns.duplicated()]

            alert_risk_scorer = AlertRiskScorer(self.job_details, self.threshold_details)



            result_df = alert_risk_scorer.alert_risk_scoring(result_df)
            # result_df = result_df[['Alert_ID', 'ALERT_PRIORITY', 'ALERT_SCORE', 'AUTO_CLOSE']]

            return result_df

        except Exception as e:
            self.logger.error(f"Error in alert_score_table: {e}", exc_info=True)
            raise e

    def get_alert_prev_match_ct(self, alert_table: pd.DataFrame) -> pd.DataFrame:
        try:
            # Get data for all alerts including acct_id, cust_id, scenario_id
            all_alerts_df = self.sql_executor.execute_query("""
                                    SELECT TS_ALERT_CUST.ALERT_ID, TS_ALERT_CUST.CUST_ID, TS_ALERT_CUST.ACCT_ID , TS_ALERT.SCENARIO_ID
                                    FROM TS_ALERT_CUST
                                    INNER JOIN TS_ALERT ON TS_ALERT_CUST.ALERT_ID = TS_ALERT.ALERT_ID
                                                        """)

            key_column = 'ACCT_ID' if self.job_details['SCENARIO_FOCUS'] == 'ACCOUNT' else 'CUST_ID'

            # Filter all_alerts_df to include only those accounts/customers present in alert_data
            filtered_alerts_df = all_alerts_df[all_alerts_df[key_column].isin(alert_table[key_column])]

            # Create PREV_MATCH_CT: count distinct alert_id where acct_id / cust_id matches between the two DataFrames
            prev_match_ct = (filtered_alerts_df
                            .groupby(key_column)['ALERT_ID']
                            .nunique()
                            .reset_index())
            prev_match_ct.columns = [key_column, 'PREV_MATCH_CT']

            # Update existing PREV_MATCH_CT in alert_table
            alert_table = alert_table.merge(prev_match_ct, on=key_column, how='left')
            alert_table['PREV_MATCH_CT'] = alert_table['PREV_MATCH_CT_y'].fillna(0).astype(int)
            alert_table.drop(columns=['PREV_MATCH_CT_x', 'PREV_MATCH_CT_y'], inplace=True)

            # Create PREV_MATCH_CT_ALL: count distinct alert_id where both acct_id and scenario_id match
            prev_match_ct_all = (all_alerts_df
                                .groupby([key_column, 'SCENARIO_ID'])['ALERT_ID']
                                .nunique()
                                .reset_index())
            prev_match_ct_all.columns = [key_column, 'SCENARIO_ID', 'PREV_MATCH_CT_ALL']

            # Update existing PREV_MATCH_CT_ALL in alert_table
            alert_table = alert_table.merge(prev_match_ct_all, on=[key_column, 'SCENARIO_ID'], how='left')
            alert_table['PREV_MATCH_CT_ALL'] = alert_table['PREV_MATCH_CT_ALL_y'].fillna(0).astype(int)
            alert_table.drop(columns=['PREV_MATCH_CT_ALL_x', 'PREV_MATCH_CT_ALL_y'], inplace=True)

            # alert_table.to_csv('prior_alert_table.csv', index=False)

            return alert_table
        except Exception as e:
            self.logger.error(f"Error in get_alert_prev_match_ct: {e}")
            raise e





# # Example trigger function for process_with_alert_checker
# def trigger_process_with_alert_checker(df: pd.DataFrame, scenario_id: int, job_details: Dict, threshold_details: Dict) -> bool:
#     """
#     Trigger function to call process_with_alert_checker.

#     Args:
#         df (pandas.DataFrame): The DataFrame to process.
#         scenario_id (int): The scenario ID.
#         job_details (Dict): Details of the job.
#         threshold_details (Dict): Threshold details.

#     Returns:
#         bool: Result of the processing.
#     """
#     manager = ScenarioManager(scenario_id, job_details, threshold_details)
#     return manager.process_with_alert_checker(df)
