from scenario_logger import logger
import pandas as pd
from ts_pusher.util import SQLQueryExecutor
from typing import List, Dict, Any, Optional
from datetime import datetime

class JobStatusUpdater:
    def __init__(self, job_details):
        self.sql_executor = SQLQueryExecutor()
        self.logger = logger
        self.job_details = job_details

    def create_job_monitor_status(self, status, start_time, end_time):
        # status_code = 'SUCCESS' if success else 'FAIL'
        status_dict = {'SCHEDULED': 77, 'RUNNING': 78, 'COMPLETED': 79, 'FAILED': 80}

        status_id = status_dict.get(status, None)
        if status_id is None:
            raise ValueError(f"Invalid status: {status}")

        start_datetime = datetime.fromtimestamp(start_time)
        end_datetime = datetime.fromtimestamp(end_time)

        print(f"Job details: {self.job_details}")

        job_id = self.job_details['JOB_ID']

        try:
            # Check if JOB_ID exists
            query = f"SELECT COUNT(1) FROM TS_JOB_MONITOR WHERE JOB_ID = '{job_id}'"
            result = pd.read_sql(query, con=self.sql_executor.create_engine())

            # If JOB_ID exists, update the status to RUNNING
            if result.iloc[0, 0] > 0:
                update_query = f"""
                    UPDATE TS_JOB_MONITOR
                    SET STATUS = {status_id}, START_TIME = '{start_datetime}', STOP_TIME = '{end_datetime}'
                    WHERE JOB_ID = '{job_id}'
                """
                self.sql_executor.execute_query(update_query)

                self.logger.info(f"Job ID - {job_id} | Job Monitor Details Status updated to {status}")

            else:
                # If JOB_ID doesn't exist, insert a new record
                job_data = pd.DataFrame({
                    'JOB_NAME': [self.job_details['JOB_NAME']],
                    "START_TIME": [start_datetime],
                    "STOP_TIME": [end_datetime],
                    "STATUS": [status_id],
                    "TRIGGERED_BY": [self.job_details['TRIGGER_BY']],
                    'JOB_ID': [self.job_details['JOB_ID']],
                })

                job_data.to_sql(name='TS_JOB_MONITOR', con=self.sql_executor.create_engine(), if_exists='append', index=False)
                self.logger.info(f"Job ID - {job_id} | Job Monitor Details Created Successfully")

        except Exception as e:
            self.logger.error(f"Job ID - {job_id} | Error creating or updating job monitor status: {e}")
            raise e

    def update_job_monitor_status(self, status, start_time, end_time):

        # status_code = 'SUCCESS' if success else 'FAIL'
        status_dict = {'SCHEDULED': 77, 'RUNNING': 78, 'COMPLETED': 79, 'FAILED': 80}

        status_id = status_dict.get(status, None)
        if status_id is None:
            raise ValueError(f"Invalid status: {status}")



        if status_id == 77:
            start_datetime = datetime.fromtimestamp(start_time)
            end_datetime = datetime.fromtimestamp(end_time)
            update_query = (
                f"UPDATE TS_JOB_MONITOR SET STATUS = '{status_id}', "
                f"START_TIME = '{start_datetime}', "
                f"END_TIME = '{end_datetime}' "
                f"WHERE JOB_ID = {self.job_details['JOB_ID']}"
            )
        else:

            update_query = (
                    f"UPDATE TS_JOB_MONITOR SET STATUS = '{status}'"
                    f"WHERE JOB_ID = {self.job_details['JOB_ID']}"
                )

        try:
            self.sql_executor.execute_query(update_query)

            self.logger.info(f"Job ID - {self.job_details['JOB_ID']} |  Job Monitor Details Updated Successfully")

        except Exception as e:
            self.logger.error(f"Job ID - {self.job_details['JOB_ID']} |  Error updating job monitor status: {e}")
            raise e



    def update_job_execution_status(self, status, desc, start_time, end_time):
        # status_code = 'SUCCESS' if success else 'FAIL'

        current_date = pd.to_datetime(datetime.now()).strftime('%Y-%m-%d')

        start_datetime = datetime.fromtimestamp(start_time)
        end_datetime = datetime.fromtimestamp(end_time)
        job_data = pd.DataFrame({
            'JOB_NAME': [self.job_details['JOB_NAME']],
            'JOB_TEXT': [desc],
            "EXEC_DATE": [current_date],
            "END_TS": [end_datetime],
            "START_TS": [start_datetime],
            "STATUS": [status],
            "TRIGGERED_BY": [self.job_details['TRIGGER_BY']],
            'JOB_ID': [self.job_details['JOB_ID']]
        })

        try:
            job_data.to_sql(name='TS_JOB_EXEC', con=self.sql_executor.create_engine(), if_exists='append', index=False)

            self.logger.info(f"Job ID - {self.job_details['JOB_ID']} | Job Monitor Details Updated Successfully")

        except Exception as e:
            self.logger.error(f"Job ID - {self.job_details['JOB_ID']} | Error updating job monitor status: {e}")
            raise e


    def update_job_details_status(self, desc):
        job_data = pd.DataFrame({
            'JOB_TEXT': [desc],
            'JOB_NAME': [self.job_details['JOB_NAME']],
            'JOB_ID': [self.job_details['JOB_ID']]
        })

        try:
            job_data.to_sql(name='TS_JOB_DETAILS', con=self.sql_executor.create_engine(), if_exists='append', index=False)

            self.logger.info(f"Job ID - {self.job_details['JOB_ID']} |  Job Details Updated Successfully")

        except Exception as e:
            self.logger.error(f"Job ID - {self.job_details['JOB_ID']} |  Error in update_job_details_status while updating job details status: {e}")
            # # print((f"Error updating job status: {e}")
            raise e
