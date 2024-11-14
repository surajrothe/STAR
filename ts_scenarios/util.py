import importlib
from scenario_logger import logger
import pandas as pd
import re

def import_alert_checker(scenario_id):
    """
    Import the AlertChecker class dynamically based on the provided scenario_id.

    :param scenario_id: The scenario identifier used to determine the module path.
    :return: The AlertChecker class from the dynamically imported module.
    :raises ValueError: If the scenario_id is unsupported or invalid.
    """
    # Construct the module path dynamically based on the scenario_id
    module_path = f"ts_scenarios.{scenario_id}.alert_checker"

    try:
        logger.info(f"Importing AlertChecker class for scenario_id: {scenario_id}")
        # Import the module using importlib
        module = importlib.import_module(module_path)
        return module.AlertChecker
    except ModuleNotFoundError:
        logger.error(f"Module not found for scenario_id: {scenario_id}")
        raise ValueError(f"Module not found for scenario_id: {scenario_id}")
    except AttributeError:
        logger.error(f"AlertChecker class not found in module for scenario_id: {scenario_id}")
        raise ValueError(f"AlertChecker class not found in module for scenario_id: {scenario_id}")

def import_evidence_generator(scenario_id):
    """
    Import the EvidenceReportGenerator class dynamically based on the provided scenario_id.

    :param scenario_id: The scenario identifier used to determine the module path.
    :return: The EvidenceReportGenerator class from the dynamically imported module.
    :raises ValueError: If the scenario_id is unsupported or invalid.
    """
    # Construct the module path dynamically based on the scenario_id
    module_path = f"ts_scenarios.{scenario_id}.generate_evidence"

    try:
        logger.info(f"Importing EvidenceReportGenerator class for scenario_id: {scenario_id}")
        # Import the module using importlib
        module = importlib.import_module(module_path)
        return module.EvidenceReportGenerator
    except ModuleNotFoundError:
        logger.error(f"Module not found for scenario_id: {scenario_id}")
        raise ValueError(f"Module not found for scenario_id: {scenario_id}")
    except AttributeError:
        logger.error(f"EvidenceReportGenerator class not found in module for scenario_id: {scenario_id}")
        raise ValueError(f"EvidenceReportGenerator class not found in module for scenario_id: {scenario_id}")


def apply_scenario_config(base_df: pd.DataFrame, job_details: dict) -> pd.DataFrame:
    """
    Apply scenario configurations to filter the base pandas DataFrame.

    Args:
        base_df (pd.DataFrame): The base DataFrame containing transaction data.
        job_details (dict): Dictionary containing job details including scenario configurations.
        threshold_details (dict): Dictionary containing threshold details.

    Returns:
        pd.DataFrame: Filtered DataFrame based on scenario configurations.
    """
    scn_configs = job_details.get("SCENARIO_CONFIG", [])

    # Extract the SQL conditions from the scenario configurations
    trxn_config_sql_tx = [config['SQL_TX'] for config in scn_configs if config['CONFIG_TYPE_CD'] == "TRXN_DATA" and config.get('SQL_TX')
    and '--' not in config['SQL_TX']]

    if len(trxn_config_sql_tx) == 0:
        logger.info("No scenario configurations found for transaction data.")
        return base_df




    # Combine conditions with 'and'
    combined_conditions = ' & '.join(trxn_config_sql_tx) if trxn_config_sql_tx else 'True'  # Default to 'True' if no conditions

    # Translate SQL syntax to pandas-compatible syntax if necessary
    # Basic replacements for logical operators, adjust further if needed
    combined_conditions = combined_conditions.replace(' AND ', ' & ').replace(' OR ', ' | ')

    # Translate SQL syntax to pandas-compatible syntax
    combined_conditions = combined_conditions.replace('IN', 'in').replace('=', '==')


    # Apply the combined conditions using .query() method
    try:
        filtered_df = base_df.query(combined_conditions)
        return filtered_df
    except Exception as e:
        logger.error(f"Error applying scenario config conditions to DataFrame: {e}")
        raise e


