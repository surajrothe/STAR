import pandas as pd
from ts_fetcher.util import SQLQueryExecutor
from scenario_logger import logger
def retrieve_scenario_data(received_request: dict) -> dict:
    """
    Fetches and processes scenario data from the database based on provided job details.

    Args:
        job_details (dict): Dictionary containing job details including 'SCENARIO_IDS', 'JOB_ID'.

    Returns:
        dict: Updated job_details dictionary including 'SCENARIO_DATA'.
    """
    try:
        alchemy_executor = SQLQueryExecutor() # Initialize the SQLQueryExecutor
    except Exception as e:
        logger.error(f"Failed to initialize SQLQueryExecutor in retrieve_scenario_data: {e}", exc_info=True)
        raise

    try:
        logger.info("Fetching scenario configuration data")

        # Format scenario IDs for SQL query
        formatted_scenario_ids = ", ".join(f"'{id_}'" for id_ in received_request.get('SCENARIO_IDS', []))

        # Define the SQL query
        query = f"""
        SELECT
            TS_SCENARIO.SCENARIO_ID,
            TS_SCENARIO.SCENARIO_NM,
            TS_SCENARIO.ALERT_TYPE_ID,
            TS_SCENARIO.SCENARIO_FOCUS,
            TS_SCENARIO.SCENARIO_FREQUENCY,
            TS_SCENARIO.STATUS_ID AS SCENARIO_STATUS_ID,
            TS_SCENARIO.THRESHOLD_LEVEL_FL,
            TS_SCN_CONFIG.CONFIG_ID,
            TS_SCN_CONFIG.SQL_TX,
            TS_SCN_CONFIG.CONFIG_TYPE_CD,
            TS_SCN_CONFIG.STATUS_ID AS CONFIG_STATUS_ID,
            SCENARIO_STATUS.STATUS_NAME AS SCENARIO_STATUS_NAME,
            CONFIG_STATUS.STATUS_NAME AS CONFIG_STATUS_NAME,
            THRESHOLD_LEVEL_STATUS.STATUS_NAME AS THRESHOLD_STATUS_NAME
        FROM TS_SCENARIO
        LEFT JOIN TS_SCN_CONFIG ON TS_SCENARIO.SCENARIO_ID = TS_SCN_CONFIG.SCENARIO_ID
        LEFT JOIN TS_STATUS AS SCENARIO_STATUS ON TS_SCENARIO.STATUS_ID = SCENARIO_STATUS.STATUS_ID
        LEFT JOIN TS_STATUS AS THRESHOLD_LEVEL_STATUS ON TS_SCENARIO.THRESHOLD_LEVEL_FL = THRESHOLD_LEVEL_STATUS.STATUS_ID
        LEFT JOIN TS_STATUS AS CONFIG_STATUS ON TS_SCN_CONFIG.STATUS_ID = CONFIG_STATUS.STATUS_ID
        WHERE TS_SCENARIO.SCENARIO_ID IN ({formatted_scenario_ids})
        """

        # Execute the query
        scenario_data = alchemy_executor.execute_query(query)
        job_query = F"""SELECT JOB_NAME FROM TS_JOB_RUN WHERE JOB_ID = {received_request['JOB_ID']}"""
        job_data = alchemy_executor.execute_query(job_query)



        # Process the data
        scenario_data = scenario_data.apply(
            lambda x: int(x) if isinstance(x, (pd.Int64Dtype, int)) else x
        )

        # Group and aggregate data
        grouped_data = (
            scenario_data.groupby('SCENARIO_ID')
            .apply(lambda x: {
                'JOB_ID': received_request['JOB_ID'],
                'JOB_NAME': job_data['JOB_NAME'].iloc[0],
                'TRIGGER_BY': received_request['TRIGGER_BY'],
                'SCENARIO_ID': x['SCENARIO_ID'].iloc[0],
                'SCENARIO_NAME': x['SCENARIO_NM'].iloc[0],
                'ALERT_TYPE_ID': int(x['ALERT_TYPE_ID'].iloc[0]),
                'SCENARIO_FOCUS': x['SCENARIO_FOCUS'].iloc[0],
                'THRESHOLD_LEVEL_FL': x['THRESHOLD_STATUS_NAME'].iloc[0],
                'SCENARIO_FREQUENCY': x['SCENARIO_FREQUENCY'].iloc[0],
                # 'SCENARIO_STATUS_ID': int(x['SCENARIO_STATUS_ID'].iloc[0]),
                # 'SCENARIO_STATUS_NAME': x['SCENARIO_STATUS_NAME'].iloc[0],
                'SCENARIO_CONFIG': x[['CONFIG_ID', 'SQL_TX', 'CONFIG_TYPE_CD', 'CONFIG_STATUS_ID', 'CONFIG_STATUS_NAME']]
                                  .astype({'CONFIG_ID': int, 'CONFIG_STATUS_ID': int})
                                  .to_dict('records')
            })
            .to_dict()
        )
        received_request['JOB_NAME'] = job_data['JOB_NAME'].iloc[0]

        # Update job_details with the processed scenario data
        return grouped_data, received_request

    except Exception as e:
        logger.error(f"Error while fetching and processing scenario data: {e}")
        raise
