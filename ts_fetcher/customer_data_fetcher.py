import json
import pandas as pd
from ts_fetcher.util import SQLQueryExecutor
from scenario_logger import logger

def retrieve_customer_ids() -> list[str]:
    """
    Fetches customer IDs from the database and returns them as a list of strings.

    Returns:
        list[str]: A list of customer IDs.

    Raises:
        Exception: If there is an error initializing the SQLQueryExecutor or fetching the data.
    """
    # Initialize SQLQueryExecutor
    try:
        alchemy_executor = SQLQueryExecutor()
    except Exception as e:
        logger.error(f"Failed to initialize SQLQueryExecutor in retrieve_customer_ids: {e}", exc_info=True)
        raise

    # Fetch customer IDs from the database
    try:
        query = "SELECT CUST_ID FROM TS_CUST ORDER BY CUST_ID"
        customer_ids_df = alchemy_executor.execute_query(query)

        if customer_ids_df.empty:
            logger.warning("No customer IDs found in the database.")
            return []

        # Convert to list of strings
        customer_ids = customer_ids_df['CUST_ID'].astype(str).tolist()
        logger.info(f"Successfully fetched {len(customer_ids)} customer IDs.")
        return customer_ids

    except Exception as e:
        logger.error(f"Error occurred while fetching customer IDs: {e}", exc_info=True)
        raise
