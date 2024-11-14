from pyspark.sql import SparkSession
import pandas as pd
import os
import concurrent.futures
from scenario_logger import logger

# Constants for JDBC and Spark configurations
JDBC_DRIVER_PATH = "/home/hackit/JDBC/sqljdbc_12.8.0.0_enu/sqljdbc_12.8/enu/jars/mssql-jdbc-12.8.0.jre11.jar"
# JDBC_DRIVER_PATH = "/home/ubuntu/SPARK_TESTING/JDBC/mssql-jdbc-12.8.0.jre11.jar"
# JDBC_URL = "jdbc:sqlserver://54.226.161.24;databaseName=TS_STAR;encrypt=true;trustServerCertificate=true"
# JDBC_PROPERTIES = {
#     "user": "transvision",
#     "password": "Transvision@2023",
#     "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
# }

JDBC_URL = "jdbc:sqlserver://34.196.144.75;databaseName=TS_STAR;encrypt=true;trustServerCertificate=true"
JDBC_PROPERTIES = {
    "user": "script_user",
    "password": "transvision@2024",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}


def create_spark_session() -> SparkSession:
    try:
        logger.info("Initializing Spark session with JDBC configurations.")
        spark = (SparkSession.builder
                 .appName("TS_FETCHER")
                 .master("local[*]")
                 .config("spark.driver.memory", "10g")
                 .config("spark.executor.memory", "6g")
                 .config("spark.executor.cores", "4")
                 .config("spark.sql.session.timeZone", "UTC")  # Set session timezone to UTC
                 .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")  # Set executor timezone
                 .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")  # Set driver timezone
                 .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                 .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
                 .config("spark.jars", JDBC_DRIVER_PATH)
                #  .config("spark.local.dir", "/home/ubuntu/SCENARIO_SPARK/TS_TRXN_MONITORING_SPARK")
                 .config("spark.local.dir", "/home/hackit/Desktop/TM/SCENARIO_SPARK/TS_TRXN_MONITORING_SPARK/")

                 .getOrCreate())
        logger.info("Spark session initialized successfully.")
        return spark
    except Exception as e:
        logger.error("Failed to create Spark session.", exc_info=True)
        raise RuntimeError("SparkSessionInitializationError: Unable to create Spark session.") from e

def fetch_trxn_data(spark: SparkSession, customer_ids: list, received_request: dict, parquet_name: str) -> bool:
    formatted_cust_ids = ", ".join(f"'{id_}'" for id_ in customer_ids)
    query = f"""
    --SELECT * FROM TS_BASE_TRXN_DATA
    --WHERE CUSTOMER_ID IN ({formatted_cust_ids}) AND TRXN_EXCN_DT BETWEEN '2024-06-01 00:00:00' AND '2024-08-31 23:59:59'
    SELECT DISTINCT
        TS_ACCT.ACCT_ID,
        TS_ACCT_BAL.LEDGER_BAL_BASE,
        TS_ACCT_BAL.UPDATED_DATE,
        TS_ACCT_BAL.ACCT_NET_WRTH_BASE,
        TS_ACCT.ACCT_OPEN_DT,
        TS_ACCT.ACCT_TYPE,
        TS_ACCT.PRIMARY_CUSTOMER_IDENTIFIER AS CUSTOMER_ID,
        TS_LOAN.BORROWER_INCOME,
        TS_TRXN.TRXN_ID,
        TS_TRXN.TRXN_AMOUNT,
        TS_TRXN.TRXN_EXCN_DT,
        TS_TRXN.TRXN_INSTRMT_NUM,
        TS_TRXN.TRXN_POST_DT,
        TS_TRXN.TRXN_CRSS_BRDR,
        TS_TRXN.TRXN_TYPE_CD,
        TS_TRXN.TRANSACTION_CHANNEL,
        TS_TRXN.TRXN_TYPE_ID,
        TS_TRXN.PRODUCT_TYPE,
        TS_TRXN_PARTY.BENEFICIARY_NAME,
        TS_TRXN_PARTY.INTERNAL_ACCOUNT,
        TS_TRXN_PARTY.ORIGINATOR_NAME,
        TS_TRXN_PARTY.TRXN_PARTY_ID,
        TS_TRXN_PARTY.BENEFICIARY_BANK_NAME,
        TS_TRXN.CREDIT_DEBIT_CODE,
        TS_CUST_RISK.CUSTOMER_RISK,
        TS_CUST.DISPLAY_NM,
        TS_CUST.AGE,
        TS_CUST.CUST_TYP,
        TS_CUST.IS_PEP,
        TS_CUST.CTZNSHIP_STS,
        TS_CUST.DT_OF_BIRTH,
        TCNTRY.COUNTRY_NAME AS PARTY_CNTRY_NAME,
        TCNTRY.HIGHRISK_FLAG,
        TCNTRY_RESIDENCE.COUNTRY_NAME AS CNTRY_OF_RESIDENCE,
        TCNTRY_INCORP.COUNTRY_NAME AS CNTRY_OF_INCORP
    FROM
        TS_ACCT
    LEFT JOIN
        TS_TRXN ON TS_ACCT.ACCT_ID = TS_TRXN.ACCNT_ID
    LEFT JOIN
        TS_ACCT_BAL ON TS_ACCT.ACCT_ID = TS_ACCT_BAL.ACCT_ID
    LEFT JOIN
        TS_CUST ON TS_ACCT.PRIMARY_CUSTOMER_IDENTIFIER = TS_CUST.CUST_ID
    LEFT JOIN
        TS_CUST_RISK ON TS_CUST_RISK.CUSTOMER_RISK_ID = TS_CUST.CUST_RISK_ID
    LEFT JOIN
        TS_TRXN_PARTY ON TS_TRXN.TRNX_PARTY_ID = TS_TRXN_PARTY.TRXN_PARTY_ID
    LEFT JOIN
        TS_COUNTRY TCNTRY_RESIDENCE ON TS_CUST.CNTRY_OF_RESIDENCE = TCNTRY_RESIDENCE.COUNTRY_ID
    LEFT JOIN
        TS_COUNTRY TCNTRY_INCORP ON TS_CUST.CNTRY_OF_INCORP = TCNTRY_INCORP.COUNTRY_ID
    LEFT JOIN
        TS_COUNTRY TCNTRY ON TS_TRXN_PARTY.PARTY_CNTRY = TCNTRY.COUNTRY_NAME
    LEFT JOIN
        TS_LOAN ON TS_ACCT.ACCT_ID = TS_LOAN.ACCNT_ID


    WHERE TS_ACCT.PRIMARY_CUSTOMER_IDENTIFIER IN ({formatted_cust_ids})
    --WHERE TS_ACCT.PRIMARY_CUSTOMER_IDENTIFIER IN ({formatted_cust_ids}) AND TRXN_EXCN_DT BETWEEN '2024-08-01 00:00:00' AND '2024-09-30 23:59:59'
    """
    logger.info(f"Fetching transaction data for {len(customer_ids)} customers.")
    try:
        df = spark.read \
            .jdbc(url=JDBC_URL,
                  table=f"({query}) AS t",
                  properties=JDBC_PROPERTIES)


        # df.select("TRXN_EXCN_DT").show(truncate=False)


        job_id = received_request.get('JOB_ID')
        output_dir = f"/home/hackit/Desktop/TM/SCENARIO_SPARK/TS_TRXN_MONITORING_SPARK/Parquet_Data/{job_id}"
        output_dir = f"Parquet_Data/{job_id}"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, parquet_name)

        df.write.mode("overwrite").parquet(output_path)

        logger.info(f"Data saved to Parquet file: {output_path}")
        return True

    except Exception as e:
        logger.error(f"Error fetching transaction data: {e}", exc_info=True)
        return False

def process_customer_chunks(customer_chunks: list, job_details: dict, threshold_details: dict) -> int:
    spark = create_spark_session()

    def fetch_and_save(chunk: list, index: int) -> bool:
        parquet_name = f"trxn_data_chunk_{index}.parquet"
        return fetch_trxn_data(spark, chunk, job_details, parquet_name)

    success_count = 0

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(fetch_and_save, chunk, idx) for idx, chunk in enumerate(customer_chunks)]
            for future in concurrent.futures.as_completed(futures):
                if future.result():
                    success_count += 1
        logger.info("All customer chunks processed.")
    except Exception as e:
        logger.error("Error during parallel processing of customer chunks.", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

    return success_count

def chunkify(lst: list, chunk_size: int) -> list:
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def retrieve_trxn_data(customer_ids: list, job_details: dict, threshold_details: dict) -> int:
    customer_chunks = chunkify(customer_ids, 25000)
    # Return only the count of processed chunks
    processed_chunks_count = process_customer_chunks(customer_chunks, job_details, threshold_details)
    return processed_chunks_count, len(customer_chunks)
