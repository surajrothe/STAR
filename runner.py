from flask import Flask, jsonify, request
from ts_fetcher.customer_data_fetcher import retrieve_customer_ids
from ts_fetcher.scenario_data_fetcher import retrieve_scenario_data
from ts_fetcher.threshold_data_fetcher import retrieve_threshold_data
from ts_fetcher.trxn_data_fetcher import retrieve_trxn_data
from ts_scenarios.util import apply_scenario_config
from ts_scenarios.scenario_manager import ScenarioManager
from scenario_logger import logger
import os
import glob
import pandas as pd
import time
from ts_pusher.job_status_updater import JobStatusUpdater
app = Flask(__name__)

@app.route('/api/execute_pipeline', methods=['POST'])
def run_pipeline():
    """
    Endpoint to trigger the data processing pipeline.
    """
    try:
        start_time = time.time()
        # Extract and validate request data
        data = request.get_json()
        job_id = data.get("jobId")
        scenario_ids = data.get("scenarioId")
        trigger_by = data.get("triggeredBy")
        is_manual_job = data.get("isManual")

        if not all([job_id, scenario_ids, trigger_by, is_manual_job]):
            return jsonify({"error": "Missing required fields"}), 400

        received_request = {
            "JOB_ID": job_id,
            "SCENARIO_IDS": scenario_ids,
            "TRIGGER_BY": trigger_by,
            "IS_MANUAL_JOB": is_manual_job
        }

        # Log the details
        logger.info(f"Received request to start pipeline with details: {received_request}")

        # Retrieve scenario data
        logger.info("Starting scenario data retrieval.")
        job_details, received_request = retrieve_scenario_data(received_request)
        if not job_details:
            return jsonify({
                "status": "error",
                "message": "Failed to retrieve scenario data.",
                "total_alerts": 0,
                "success_scenarios": [],
                "failed_scenarios": scenario_ids,
                "manual_job": is_manual_job
            }), 500


        job_updater = JobStatusUpdater(received_request)
        job_updater.create_job_monitor_status("RUNNING", start_time, time.time())

        # Retrieve customer IDs
        logger.info("Starting customer ID retrieval.")
        customer_ids = retrieve_customer_ids()
        if not customer_ids:
            return jsonify({
                "status": "error",
                "message": "Error - Failed to retrieve customer IDs",
                "total_alerts": 0,
                "success_scenarios": [],
                "failed_scenarios": scenario_ids,
                "manual_job": is_manual_job
            }), 500


        # Retrieve threshold data
        logger.info("Starting threshold data retrieval.")
        threshold_details = retrieve_threshold_data(job_details)
        if not threshold_details:
            return jsonify({
                "status": "error",
                "message": "Error - Failed to retrieve threshold data",
                "total_alerts": 0,
                "success_scenarios": [],
                "failed_scenarios": scenario_ids,
                "manual_job": is_manual_job
            }), 500

        # Process customer IDs
        logger.info("Starting customer ID processing.")
        # success_chunk, no_of_chunks = retrieve_trxn_data(customer_ids, received_request, threshold_details)

        # # Check transaction processing results
        # if success_chunk < no_of_chunks:
        #     return jsonify({
        #             "status": "error",
        #             "message": "Error - Some transaction data chunks failed to process.",
        #             "total_alerts": 0,
        #             "success_scenarios": [],
        #             "failed_scenarios": scenario_ids,
        #             "manual_job": is_manual_job
        #         }), 500

        no_of_chunks = 6


        # Process Parquet files chunk by chunk
        logger.info("Starting Parquet files processing.")
        result = process_parquet_files(received_request, job_details, threshold_details, no_of_chunks)

        # Update job status to completed
        job_updater.update_job_monitor_status("COMPLETED", start_time, time.time())

        job_updater.update_job_details_status(f"{result['total_alerts']} alerts found! | Successful scenarios - {result['successful_scenarios']} | Failed scenarios - {result['failed_scenarios']}")


        # job_updater.update_job_execution_status("SUCCESS", f"{result['total_alerts']} alerts found! | Successful scenarios - {result['successful_scenarios']} | Failed scenarios - {result['failed_scenarios']}", start_time, time.time())

        # Log completion
        logger.info("Pipeline execution completed successfully.")

        return jsonify({
            "status": "success",
            "message": "Pipeline executed successfully.",
            "total_alerts": result['total_alerts'],
            "success_scenarios": result['successful_scenarios'],
            "failed_scenarios": result['failed_scenarios'],
            "manual_job": is_manual_job
        }), 200

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": f"Error - {str(e)}",
            "success_scenarios": [],
            "failed_scenarios": scenario_ids,
            "total_alerts": 0,
            "manual_job": is_manual_job
        }), 500

def process_parquet_files(received_request: dict, job_details: dict, threshold_details: dict, no_of_chunks: int) -> dict:
    """
    Processes a limited number of Parquet files generated during the pipeline execution.

    Args:
        received_request (dict): Dictionary containing request details, including the job ID for file path construction.
        job_details (dict): Dictionary containing job details.
        threshold_details (dict): Dictionary containing threshold details.
        no_of_chunks (int): The number of chunks to process.

    Returns:
        dict: A dictionary with the total alerts found, status of processing, successful scenario IDs, and failed scenario IDs.
    """
    output_path = f"Parquet_Data/{received_request.get('JOB_ID')}/"
    logger.info(f"Processing up to {no_of_chunks} Parquet files in directory: {output_path}")

    # Use glob to find all Parquet files in the specified directory
    parquet_files = sorted(glob.glob(os.path.join(output_path, "trxn_data_chunk_*.parquet")))
    results = {
        "processed": 0,
        "total_alerts": 0,
        "successful_scenarios": [],
        "failed_scenarios": []
    }

    # Limit the files processed to the number of chunks specified
    parquet_files_to_process = parquet_files[:no_of_chunks]

    if not parquet_files_to_process:
        logger.info("No Parquet files found to process.")
        return {"total_alerts": 0, "successful_scenarios": [], "failed_scenarios": []}

    total_customers = 0
    total_rows_processed = 0

    # Track scenario success for all files processed
    for scenario_id in received_request.get("SCENARIO_IDS", []):
        scenario_success = True  # Track if the scenario was successful for all files

        # Process each Parquet file
        for parquet_file in parquet_files_to_process:
            try:
                logger.info(f"Reading Parquet file: {parquet_file}")
                df = pd.read_parquet(parquet_file)

                total_rows_processed += len(df)
                total_customers += df['CUSTOMER_ID'].nunique()

                logger.info(f"Fetched {len(df)} transactions | Total unique customers: {total_customers}")

                if not df.empty:
                    filtered_df = apply_scenario_config(df.copy(), job_details[scenario_id])

                    if filtered_df is None or filtered_df.empty:
                        logger.error(f"No data found for scenario ID {scenario_id} in Parquet file {parquet_file} after applying scenario config filters.")
                        continue

                    # Process the DataFrame with ALERTCHECKER
                    alert_checker_manager = ScenarioManager(scenario_id, job_details[scenario_id], threshold_details[scenario_id])
                    success_val, alert_count = alert_checker_manager.process_with_alert_checker(filtered_df)

                    if not success_val:
                        logger.error(f"Error processing Parquet file {parquet_file} with scenario ID {scenario_id}")
                        scenario_success = False  # Mark the scenario as failed
                        break  # Exit the inner loop for this scenario

                    results["total_alerts"] += alert_count
                    results["processed"] += 1

                    logger.info(f"Successfully processed Parquet file {parquet_file} with scenario ID {scenario_id}")

            except Exception as e:
                logger.error(f"Error processing Parquet file {parquet_file}: {e}", exc_info=True)
                scenario_success = False  # Mark the scenario as failed
                break  # Exit the inner loop for this scenario

        # Track successful and failed scenarios
        if scenario_success:
            results["successful_scenarios"].append(scenario_id)
        else:
            results["failed_scenarios"].append(scenario_id)

    logger.info(f"All data fetched: {total_rows_processed} transactions | Total unique customers: {total_customers}")
    return results





if __name__ == "__main__":
    app.run(host='0.0.0.0', port=6000, debug=True)
    # app.run(host='0.0.0.0', port=6485, debug=True)
