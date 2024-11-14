from scenario_logger import logger
import pandas as pd
from ts_pusher.util import SQLQueryExecutor
from typing import List, Dict, Any, Optional
from ts_pusher.autocloseNarrative import generate_narratives_for_autoclosure
from datetime import datetime
from dateutil.relativedelta import relativedelta
class AlertDataPusher:
    def __init__(self):
        self.sql_executor = SQLQueryExecutor()
        self.logger = logger
        self.current_date = pd.to_datetime(datetime.now())

    def fetch_alert_display_text(self, job_details: Dict[str, Any]) -> List[str]:
        """
        Fetch display text for alerts based on scenario configuration.

        Args:
            job_details (Dict[str, Any]): Contains SCENARIO_ID and other job details.

        Returns:
            List[str]: List of active alert text names.
        """
        try:
            query = f"""
                SELECT ALERT_TX_NM
                FROM TS_ALERT_DSPLY_CONFIG
                WHERE IS_ACTIVE = 1 AND SCENARIO_ID = '{job_details['SCENARIO_ID']}'
            """
            display_data = self.sql_executor.execute_query(query)
            return display_data["ALERT_TX_NM"].to_list()
        except Exception as e:
            self.logger.error(f"Error while fetching alert display text: {e}")
            raise e

    def push_alert_data(
        self,
        job_details: Dict[str, Any],
        display_data: List[str],
        alert_table: pd.DataFrame,
    ) -> None:
        """
        Push processed alert data to the database.

        Args:
            job_details (Dict[str, Any]): Job details including job_id and scenario_id.
            display_data (List[str]): List of display text names to filter alerts.
            alert_table (pd.DataFrame): DataFrame containing alert data to be pushed.
        """
        try:
            alert_table["ALERT_TX"] = alert_table.apply(
                lambda row: {
                    key: value
                    for key, value in row["ALERT_TX"].items()
                    if key in display_data
                },
                axis=1,
            )
            alert_table = alert_table[
                [
                    "ALERT_TX",
                    "JOB_ID",
                    "PRCSNG_BATCH_NM",
                    "BREAK_MATCH_CT",
                    "PREV_MATCH_CT",
                    "PREV_MATCH_CT_ALL",
                    "SCENARIO_ID",
                    "ACCT_ID",
                    "CUST_ID",
                    "CREATED_DATE",
                    "CUSTOMER_NAME",
                    "CONFIG_ID",
                    "ALERT_TYPE_ID",
                    "PERIOD_START_DATE",
                    "PERIOD_END_DATE",
                    "THRESHOLD_TX",
                    "TRXN_AMOUNT",
                ]
            ]
            alert_table["ALERT_TX"] = alert_table["ALERT_TX"].astype(str)

            chunk_size = 100000
            total_rows = len(alert_table)

            for i in range(0, total_rows, chunk_size):
                chunk = alert_table.iloc[i : i + chunk_size]
                chunk.to_sql(
                    name="TS_ALERT",
                    con=self.sql_executor.create_engine(),
                    if_exists="append",
                    index=False,
                )
        except Exception as e:
            self.logger.error(

                f"Error: An error occurred while pushing alert data - {e}"
            )
            raise e

    def fetch_inserted_alerts(
        self, current_date: str, job_details: Dict[str, Any]
    ) -> Dict[str, int]:
        """
        Fetch inserted alerts based on the current date and job details.

        Args:
            current_date (str): The date for which to fetch alerts.
            job_details (Dict[str, Any]): Contains job_id and scenario_id.

        Returns:
            Dict[str, int]: Mapping of alert text to alert IDs.
        """
        try:
            fetch_alert_id = f"""
                SELECT *
                FROM TS_ALERT
                WHERE CREATED_DATE = '{current_date}'
                AND SCENARIO_ID = '{job_details['SCENARIO_ID']}'
                AND JOB_ID = '{job_details['JOB_ID']}'
            """
            inserted_data = self.sql_executor.execute_query(fetch_alert_id)
            return dict(zip(inserted_data["ALERT_TX"], inserted_data["ALERT_ID"]))
        except Exception as e:
            self.logger.error(f"Error while fetching inserted alerts: {e}")
            raise e

    def push_alert_trxn_data(
        self,
        job_details: Dict[str, Any],
        alert_tx_mapping_dict: Dict[str, int],
        display_data: List[str],
        alert_trans_table: pd.DataFrame,
    ) -> Optional[Dict[int, int]]:
        """
        Push alert transaction data to the database.

        Args:
            job_details (Dict[str, Any]): Job details including job_id and scenario_id.
            alert_tx_mapping_dict (Dict[str, int]): Mapping of alert text to alert IDs.
            display_data (List[str]): List of display text names to filter transactions.
            alert_trans_table (pd.DataFrame): DataFrame containing alert transaction data to be pushed.

        Returns:
            Optional[Dict[int, int]]: Mapping of Alert_ID to Alert_TX after insertion.
        """
        try:
            alert_trans_table["ALERT_TX"] = alert_trans_table.apply(
                lambda row: {
                    key: value
                    for key, value in row["ALERT_TX"].items()
                    if key in display_data
                },
                axis=1,
            )
            alert_trans_table["ALERT_TX"] = alert_trans_table["ALERT_TX"].astype(str)
            alert_trans_table["ALERT_ID"] = alert_trans_table["ALERT_TX"].map(
                alert_tx_mapping_dict
            )

            alert_trans_table.to_csv("ALERT_TRXN_HRG_before.csv", index=False)

            alert_id_mapping_dict = dict(
                zip(
                    alert_trans_table["Alert_ID"].astype(int),
                    alert_trans_table["ALERT_ID"],
                )
            )
            alert_trans_table = alert_trans_table[["ALERT_ID", "TRXN_ID"]]

            chunk_size = 100000
            total_rows = len(alert_trans_table)

            for i in range(0, total_rows, chunk_size):
                chunk = alert_trans_table.iloc[i : i + chunk_size]
                chunk.to_sql(
                    name="TS_ALERT_TRXN",
                    con=self.sql_executor.create_engine(),
                    if_exists="append",
                    index=False,
                )

            return alert_id_mapping_dict
        except Exception as e:
            self.logger.error(

                f"Error: An error occurred while pushing alert transaction data - {e}"
            )
            raise e

    def push_evidence_graph_data(self, alert_id_mapping_dict: Dict[int, int], evidence_table: pd.DataFrame) -> None:
        """
        Inserts evidence graph data into the database.

        Args:
            alert_id_mapping_dict (Dict[int, int]): Mapping of Alert_IDs to transaction IDs.
            evidence_table (pd.DataFrame): DataFrame containing evidence data to be inserted.
        """
        try:
            evidence_table["RECORDS"] = evidence_table["RECORDS"].astype(str)
            evidence_table["ALERT_ID"] = evidence_table["Alert_ID"].astype(int).map(alert_id_mapping_dict)

            columns_to_insert = ["ALERT_ID", "RECORDS", "CREATE_DT", "SCNRO_NM"]
            evidence_table = evidence_table[columns_to_insert]

            chunk_size = 100000
            total_rows = len(evidence_table)

            for i in range(0, total_rows, chunk_size):
                chunk = evidence_table.iloc[i:i + chunk_size]
                chunk.to_sql(
                    name="TS_EVIDENCE_REPORT",
                    con=self.sql_executor.create_engine(),
                    if_exists="append",
                    index=False,
                )

        except Exception as e:
            self.logger.error(f"Error occurred while inserting evidence data: {e}")
            raise e

    def push_alert_acct_cust_mapping_data(self, alert_id_mapping_dict: Dict[int, int], alert_acct_cust_table: pd.DataFrame) -> None:
        """
        Inserts alert account customer mapping data into the database.

        Args:
            alert_acct_cust_table (pd.DataFrame): DataFrame containing alert account customer mapping data to be inserted.
        """
        try:
            alert_acct_cust_table["ALERT_ID"] = alert_acct_cust_table["Alert_ID"].astype(int).map(alert_id_mapping_dict)
            alert_acct_cust_table = alert_acct_cust_table[["ALERT_ID", "ACCT_ID", "CUST_ID"]]

            chunk_size = 100000
            total_rows = len(alert_acct_cust_table)

            for i in range(0, total_rows, chunk_size):
                chunk = alert_acct_cust_table.iloc[i:i + chunk_size]
                chunk.to_sql(
                    name="TS_ALERT_CUST",
                    con=self.sql_executor.create_engine(),
                    if_exists="append",
                    index=False,
                )

        except Exception as e:
            self.logger.error(f"Error occurred while inserting alert account customer mapping data: {e}")
            raise e


    def push_auto_closure_narrative(self, narrative_data: pd.DataFrame, alert_id_mapping_dict: Dict[int, int]) -> None:
        """
        Push the narrative closure details into TS_ALERT_NARR table using the SQLQueryExecutor.

        Args:
            narrative_data (pd.DataFrame): DataFrame containing narrative details to push.
        """

        # # Map Alert_ID using the provided dictionary
        # narrative_data["ALERT_ID"] = (
        #     narrative_data["Alert_ID"].astype(int).map(alert_id_mapping_dict)
        # )


        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for index, row in narrative_data.iterrows():
            try:
                # Extract narrative text and alert ID
                narrative_text = row['NARRATIVE']
                alert_id = row['ALERT_ID']

                if not alert_id:
                    self.logger.error(f"Missing ALERT_ID in row {index}, skipping...")
                    continue

                # Fetch the ALERT_INVSTN_ID using ALERT_ID
                alert_invstn_id_query = f"""
                    SELECT ALERT_INVSTN_ID FROM TS_ALERT_INVGTN WHERE ALERT_ID = {alert_id}
                """
                result = self.sql_executor.execute_query(alert_invstn_id_query)

                if not result.empty:
                    alert_invstn_id = result['ALERT_INVSTN_ID'].iloc[0]

                    # Escape single quotes in narrative_text
                    narrative_text_safe = narrative_text.replace("'", "''")

                    # Prepare the INSERT query for the TS_ALERT_NARR table
                    narrative_query = f"""
                    INSERT INTO TS_ALERT_NARR (ALERT_INVSTN_ID, NARRATIVE, UPLOADED_BY, UPLOADED_DATE)
                    VALUES ({alert_invstn_id}, '{narrative_text_safe}', 'system', '{current_datetime}')
                    """
                    # Execute the query
                    self.sql_executor.execute_query((narrative_query))
                else:
                    self.logger.error(f"ALERT_INVSTN_ID not found for ALERT_ID {alert_id}, skipping...")

            except Exception as e:
                self.logger.error(f"Error inserting narrative for ALERT_ID {alert_id}: {e}")
                continue

        self.logger.info("Narrative data successfully pushed to TS_ALERT_NARR.")



    def push_investigation_details(self, job_details: Dict[str, Any], alert_table: pd.DataFrame, ars_table: pd.DataFrame) -> None:



        ars_table = pd.merge(ars_table, alert_table, on='Alert_ID', how='left')
        # ars_table.to_excel('ars_table_merged.xlsx', index=False)


        alert_investigation_data = []
        for index, row in ars_table.iterrows():

            # Get the current date
            cur_date = datetime.now()

            # Add one month to the current date
            due_date = cur_date + relativedelta(months=1)

            # Format the due date in 'YYYY-MM-DD' format
            due_date_str = due_date.strftime('%Y-%m-%d')

            # Create a new DataFrame row with the investigation details
            alert_investigation_df = pd.DataFrame({
                'AGE': [row['AGE'] if 'AGE' in row else None],
                'ALERT_PRIORITY': [row['ALERT_PRIORITY'] if 'ALERT_PRIORITY' in row else None],
                'ALERT_RISK': [row['ALERT_SCORE'] if 'ALERT_SCORE' in row else None],
                'ALERT_TX': [row['ALERT_TX'] if 'ALERT_TX' in row else None],
                'AUTO_REASN_FL': ['alert_deemed_as_false-posetive'],
                'CASE_FL': ['N'],
                'CLOSE_DT': [self.current_date.strftime('%Y-%m-%d')],
                'CONFIG_ID': [row['CONFIG_ID'] if 'CONFIG_ID' in row else None],
                'CREAT_TS': [self.current_date.strftime('%Y-%m-%d')],
                'CUSTOMER_ID': [row['CUST_ID'] if 'CUST_ID' in row else None],
                'CUST_DSPLY_NM': [row['CUSTOMER_NAME'] if 'CUSTOMER_NAME' in row else None],
                'REPORTED_BY': ['system'],
                'SCENARIO_DISPL_NM': [row['SCENARIO_NAME'] if 'SCENARIO_NAME' in row else None],
                'SCENARIO_ID': [row['SCENARIO_ID'] if 'SCENARIO_ID' in row else None],
                'ALERT_ID': [row['ALERT_ID'] if 'ALERT_ID' in row else None],
                'DUE_DATE': [due_date_str],
                'ALRT_ID': [row['ALERT_ID'] if 'ALERT_ID' in row else None],
                'AMOUNT': [row['TRXN_AMOUNT'] if 'TRXN_AMOUNT' in row else None],
                'ALERT_DATE': [row['CREATED_DATE'] if 'CREATED_DATE' in row else None],
                'STATUS_ID': [4]
            })

            alert_investigation_data.append(alert_investigation_df)

            # # Create a new DataFrame row with the investigation details
            # alert_investigation_df = pd.DataFrame({
            #     'AGE': [row['AGE'] if 'AGE' in row else None],
            #     'ALERT_PRIORITY': [row['ALERT_PRIORITY'] if 'ALERT_PRIORITY' in row else None],
            #     'ALERT_RISK': [row['ALERT_RISK'] if 'ALERT_RISK' in row else None],
            #     'ALERT_TX': [alert_type_id],
            #     'AUTO_CLS_CT': ['TS_JOB_UST_ACTIVITY'],
            #     'AUTO_REASN_FL': [job_id],
            #     'CASE_FL': ['N'],
            #     'CLOSE_DT': self.current_date.strftime('%Y-%m-%d'),
            #     'CLS_ID': [row['CLS_ID'] if 'CLS_ID' in row else None],
            #     'CNTRY_KEY_ID': [row['CNTRY_KEY_ID'] if 'CNTRY_KEY_ID' in row else None],
            #     'CNTRY_TYPE_CD': [row['CNTRY_TYPE_CD'] if 'CNTRY_TYPE_CD' in row else None],
            #     'CONFIG_ID': [config_id],
            #     'CREAT_TS': self.current_date.strftime('%Y-%m-%d'),
            #     'CUSTOMER_ID': [customer_id],
            #     'CUST_DSPLY_NM': [row['CUST_DSPLY_NM'] if 'CUST_DSPLY_NM' in row else None],
            #     'EXTRL_REF_ID': [row['EXTRL_REF_ID'] if 'EXTRL_REF_ID' in row else None],
            #     'EXTRL_REF_LINK': [None],
            #     'EXTRL_REF_SRC_ID': [row['ALERT_PRIORITY'] if 'ALERT_PRIORITY' in row else None],
            #     'ORIG_OWNER_SEQ_ID': [None],
            #     'OWNER_SEQ_ID': [None],
            #     'REPORTED_BY': ['system'],
            #     'SCENARIO_DISPL_NM': [job_details['SCENARIO_NAME'] if 'SCENARIO_NAME' in job_details else None],
            #     'SCENARIO_ID': [job_details['SCENARIO_ID'] if 'SCENARIO_ID' in job_details else None],
            #     'ALERT_ID': [row['ALERT_ID'] if 'ALERT_ID' in row else None],
            #     'ALERT_AUTO_CLS_ID': [None],
            #     'CRIME_SUB_TYPE_ID': [None],
            #     'CRIME_TYPE_ID': [None],
            #     # 'DUE_DATE': [cur_mon_start_date.strftime('%Y-%m-%d')],
            #     # 'ALRT_ID': [row['ALERT_ID'] if 'ALERT_ID' in row else None],
            #     # 'AMOUNT': [row['AMOUNT'] if 'AMOUNT' in row else None],
            #     # 'ALERT_DATE': [cur_mon_start_date.strftime('%Y-%m-%d')],
            #     # 'LAST_ACTVY_TYPE_CD': [cur_mon_start_date.strftime('%Y-%m-%d')],
            #     # 'LAST_LINK_FL': [cur_mon_start_date.strftime('%Y-%m-%d')],
            #     # 'STATUS_ID': [cur_mon_start_date.strftime('%Y-%m-%d')]
            # })



            # Now you can push or save this investigation data
            # Assuming there's a function or method to handle the pushing
            # self.push_to_database(alert_investigation_data)

        # return
        alert_investigation_df = pd.concat(alert_investigation_data, ignore_index=True)

        alert_investigation_df["ALERT_TX"] = alert_investigation_df["ALERT_TX"].astype(str)

        alert_investigation_df.to_excel("investigtaiion.xlsx", index=False)


        chunk_size = 100000
        total_rows = len(alert_investigation_df)

        for i in range(0, total_rows, chunk_size):
            chunk = alert_investigation_df.iloc[i : i + chunk_size]
            chunk.to_sql(
                name="TS_ALERT_INVGTN",
                con=self.sql_executor.create_engine(),
                if_exists="append",
                index=False,
            )

        print(f"Aler investigation data pushed to TS_ALERT_INVGTN - {alert_investigation_df}")



    def push_alert_score_data(self, job_details: Dict[str, Any], alert_id_mapping_dict: Dict[int, int], alert_table: pd.DataFrame, ars_table: pd.DataFrame) -> None:

        try:



            print(f"columns in alert_table: {alert_table.columns}")
            print(f"columns in ars_table: {ars_table.columns}")






            if len(ars_table[ars_table['AUTO_CLOSE'] == 1]) > 0:
                narrative_data = generate_narratives_for_autoclosure(ars_table, alert_id_mapping_dict)

                # alert_table["ALERT_ID"] = (
                #     alert_table["Alert_ID"].astype(int).map(alert_id_mapping_dict)
                # )

                ars_table = pd.merge(ars_table, alert_table, on='Alert_ID', how='left')

                self.push_investigation_details(job_details, alert_table, ars_table)

                self.push_auto_closure_narrative(narrative_data, alert_id_mapping_dict)


            else:
                self.logger.info("No alerts found with AUTO_CLOSE = 1.")

            ars_table["ALERT_ID"] = (
                ars_table["Alert_ID"].astype(int).map(alert_id_mapping_dict)
            )



            print(f"Columns in ars_table: {ars_table.columns}")


            ars_table = ars_table[["ALERT_ID", "ALERT_PRIORITY", "ALERT_SCORE"]]

            chunk_size = 100000
            total_rows = len(ars_table)

            for i in range(0, total_rows, chunk_size):
                chunk = ars_table.iloc[i : i + chunk_size]
                chunk.to_sql(
                    name="TS_ALERT_SCORE",
                    con=self.sql_executor.create_engine(),
                    if_exists="append",
                    index=False,
                )

            alert_ids_str = ", ".join(map(str, ars_table["ALERT_ID"]))

            query_alert_data = f"""
                    SELECT * FROM TS_ALERT_SCORE
                    WHERE ALERT_ID IN ({alert_ids_str})
                """

            ars_data_df = self.sql_executor.execute_query(query_alert_data)

            update_query = f"""
                    UPDATE TS_ALERT
                    SET ALERT_SCORE_ID = t.ALERT_SCORE_ID
                    FROM (VALUES {", ".join(f"({row['ALERT_ID']}, {row['ALERT_SCORE_ID']})" for _, row in ars_data_df.iterrows())}) AS t(ALERT_ID, ALERT_SCORE_ID)
                    WHERE TS_ALERT.ALERT_ID = t.ALERT_ID;
                """

            self.sql_executor.execute_query(update_query)

            update_colour = f"""
                UPDATE [TS_ALERT_SCORE]
                    SET [ALERT_PRIORITY_CLR_CD] =
                    CASE
                    WHEN [ALERT_PRIORITY] = 'Critical' THEN '#FB404B'
                    WHEN [ALERT_PRIORITY] = 'High' THEN '#FB404B'
                    WHEN [ALERT_PRIORITY] = 'Low' THEN '#47BOA2'
                    WHEN [ALERT_PRIORITY] = 'Medium' THEN '#FFA534'
                    WHEN [ALERT_PRIORITY] = 'Recommended Autoclosure' THEN '#47BOA2'
                    ELSE [ALERT_PRIORITY_CLR_CD]
                    END;
            """

            self.sql_executor.execute_query(update_colour)

        except Exception as e:
            self.logger.error(
                f"Error occurred while mapping generated ALERT_ID with alert risk scoring data: {e}"
            )
            raise e

    def push_alerts(
        self,
        job_details: Dict[str, Any],
        alert_table: pd.DataFrame,
        alert_trans_table: pd.DataFrame,
        ars_table: pd.DataFrame,
        evidence_table: pd.DataFrame,
        alert_acct_cust_table: pd.DataFrame,
        current_date: str,
    ):
        """
        Trigger function to fetch, process, and push alert and transaction data.
        """
        try:
            # Fetch display text configuration
            display_data = self.fetch_alert_display_text(job_details)

            # Push alert data
            self.push_alert_data(job_details, display_data, alert_table)

            # Fetch inserted alerts
            alert_tx_mapping_dict = self.fetch_inserted_alerts(current_date, job_details)

            # Push alert transaction data
            alert_id_mapping_dict = self.push_alert_trxn_data(
                job_details, alert_tx_mapping_dict, display_data, alert_trans_table
            )

            # Push alert score data
            self.push_alert_score_data(job_details, alert_id_mapping_dict, alert_table, ars_table)

            # Push evidence graph data
            self.push_evidence_graph_data(alert_id_mapping_dict, evidence_table)

            # Push alert account customer mapping data
            self.push_alert_acct_cust_mapping_data(alert_id_mapping_dict, alert_acct_cust_table)

        except Exception as e:
            self.logger.error(f"Error while pushing alert data: {e}")
            raise e





# def push_alerts(job_details, alert_table, alert_trans_table, current_date):
#     alert_pusher = AlertDataPusher()
#     alert_pusher.trigger_alert_push(
#         job_details, alert_table, alert_trans_table, current_date
#     )
#     pass
