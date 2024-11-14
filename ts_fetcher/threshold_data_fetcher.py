import pandas as pd
from scenario_logger import logger  # Import the logger from scenario_logger
from ts_fetcher.util import SQLQueryExecutor  # Import the SQLQueryExecutor from ts_fetcher.util

def retrieve_threshold_data(job_details: dict) -> dict:
    """
    Fetches threshold data for each scenario ID in job_details, constructs combined SQL conditions,
    and updates the thresholds_details dictionary.

    :param job_details: Dictionary containing job-specific details, including scenario IDs and configurations.
    :return: A dictionary containing threshold details for each scenario ID.
    """
    thresholds_details = {}

    try:
        # Initialize the SQLQueryExecutor
        alchemy_executor = SQLQueryExecutor()
        logger.info("SQLQueryExecutor initialized successfully.")
    except Exception as e:
        logger.error("Failed to initialize SQLQueryExecutor in fetch_threshold_data.", exc_info=True)
        raise Exception("InitializationError: Failed to initialize SQLQueryExecutor.") from e

    try:
        logger.info("Fetching threshold configuration data.")
        for scenario_id, scenario_data in job_details.items():
            logger.debug(f"Processing scenario ID: {scenario_id}")

            # Construct the base SQL query for the given scenario ID
            threshold_base_query = _construct_base_query(scenario_id)

            # Extract and combine threshold configuration SQL statements
            scn_configs = scenario_data.get("SCENARIO_CONFIG", [])
            threshold_config_sql_tx = [
                config['SQL_TX'] for config in scn_configs
                if config.get('CONFIG_TYPE_CD') == "THRESHOLD" and config.get('SQL_TX') not in [None, '', '--']
            ]

            # Combine SQL_TX statements with AND if multiple THRESHOLD configurations exist
            combined_conditions = ' AND '.join(threshold_config_sql_tx) if threshold_config_sql_tx else ''
            final_threshold_query = f"{threshold_base_query} {f'AND {combined_conditions}' if combined_conditions else ''}"

            logger.debug(f"Executing SQL query for scenario ID {scenario_id}: {final_threshold_query}")

            # Execute the final threshold query
            threshold_data = alchemy_executor.execute_query(final_threshold_query)

            if not threshold_data.empty:
                # Store thresholds in a dictionary
                scenario_thresholds = {
                    row['DPLY_NM']: row['THRESHOLD_VALUE']
                    for _, row in threshold_data.iterrows()
                }

                # Update the thresholds_details dictionary
                thresholds_details[scenario_id] = {
                    'THRESHOLD_SET_ID': threshold_data['THRESHOLD_SET_ID'].iloc[0],
                    **scenario_thresholds
                }

        logger.info("Threshold data fetching completed successfully.")
        return thresholds_details

    except Exception as e:
        logger.error("Error occurred while fetching threshold data.", exc_info=True)
        raise Exception("DataFetchError: Failed to fetch threshold data.") from e

def _construct_base_query(scenario_id: str) -> str:
    """
    Constructs the base SQL query for fetching threshold data based on the scenario ID.

    :param scenario_id: The scenario identifier for which the threshold data is to be fetched.
    :return: A string representing the base SQL query.
    """
    return f"""
    SELECT
        TS_THRESHOLD_SET.THRESHOLD_SET_ID,
        TS_THRESHOLD.THRESHOLD_VALUE,
        TS_THRESHOLD_TYPE.DPLY_NM,
        TS_THRESHOLD_TYPE.THRESHOLD_NM,
        TS_THRESHOLD_TYPE.THRESHOLD_TYPE_ID,
        TS_THRESHOLD_TYPE.SCENARIO_ID
    FROM TS_THRESHOLD_SET
    JOIN TS_THRESHOLD ON TS_THRESHOLD_SET.THRESHOLD_SET_ID = TS_THRESHOLD.THRESHOLD_SET_ID
    JOIN TS_THRESHOLD_TYPE ON TS_THRESHOLD_TYPE.SCENARIO_ID = TS_THRESHOLD_SET.SCENARIO_ID
    AND TS_THRESHOLD_TYPE.THRESHOLD_TYPE_ID = TS_THRESHOLD.THRESHOLD_TYPE_ID
    WHERE TS_THRESHOLD_TYPE.SCENARIO_ID = '{scenario_id}'
    """
