
from openai import OpenAI
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get the API key from environment variables
api_key = os.getenv('OPENAI_API_KEY')

# Initialize OpenAI client with the loaded API key
client = OpenAI(api_key=api_key)

def get_autoclosure_narrative(alert_data):
    try:
        print(alert_data)
        # Define the system prompt and alert details
        alert_details = {"ALERT_OVERVIEW_DETAILS": alert_data.to_dict()}

        print(f"Generating narrative for alert ID: {alert_details}")

        response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
            "role": "system",
            "content": [
                {
                "type": "text",
                "text": "You are an alert investigator at a financial institution, tasked with analyzing the alert data provided in JSON format. Your job is to review this data and generate a detailed description of the alerts and why the alert has been recommended in the autoclosure. The alerts is autoclosed when for the exisiting alert the score is below 35 and the alert's associated customer/account has previous alerts generated but were closed after the invistigation and were deemed as false positive, then only the alert is autoclosed. The description should include key details such as the alerted transaction amount , different parameters resulting in the alerts, the transactions that led to this alert, analyzing the transaction pattern and resulting into conclusion.The narrative should be well-structured into paragraphs that clearly explain why it is recommended for the alert to autoclose. Emphasis should be placed on highlighting the minimal nature of the activity in the alert, ensuring readability and proper spacing throughout. \n\nRefer to the example format below:\n\Alert Details JSON Data:\n {ALERT_OVERVIEW_DETAILS : {'ALERT_OVERVIEW_DETAILS': '[{'ALERT_ID':6862,'ARS_TX':'{\'SCENARIO NAME\': \'Account Awakening\', \'ACCOUNT AGE\': \'8 months, 1 days\', \'AGGREGATED TRANSCTION AMOUNT\': 20039.73, \'LOOKBACK START DATE\': \'2024-05-01\', \'LOOKBACK END DATE\': \'2024-05-31\'}','ACCT_ID':1242206515,'CUSTOMER_ID':'7b02B1B4BCcbB0a','CUST_TYP':'IND','ALERT_SCORE':31.0,'LIST_PRIOR_ALERTS':'[\'CLOSED\']','AUTO_REASON':'['alert_deemed_as_false_positive']','AUTO_CLOSE':1,'ORIGINATOR_NAME':'Wayne Ashley'}]'} \n\nNarrative should be like below:\n\nThe alert with ID 6862 was generated under the scenario named 'Account Awakening.' This alert pertains to an individual customer with an account age of 8 months and 1 day. The alert was triggered due to an aggregated transaction amount of $20,039.73 observed between the lookback period starting from May 1, 2024, to May 31, 2024 which indicates that the deviation in transactional activity was minimal. The decision to autoclose this alert is supported by the fact that prior alerts for this customer have been reviewed and closed after investigation, and were deemed as false positives. This suggests that similar patterns of transaction activity in the past have not resulted in fraudulent behavior, reinforcing the conclusion that this alert does not indicate any new or concerning activity."
                }
            ]
            },
            {
            "role": "user",
            "content": [
                {
                "type": "text",
                "text": f"""
                            Alert Details:
                            {alert_details}
                            """
                }
            ]
            }
        ],
        temperature=0,
        max_tokens=2000,
        top_p=0.35,
        frequency_penalty=0,
        presence_penalty=0
        )

        narrative_text = response.choices[0].message.content.strip()
        return narrative_text
    except Exception as e:
        print(e)

def generate_narratives_for_autoclosure(df, alert_id_mapping_dict):

    df["ALERT_ID"] = (
                df["Alert_ID"].astype(int).map(alert_id_mapping_dict)
            )
    df = df[['ALERT_ID', 'ARS_TX', 'ACCT_ID', 'CUSTOMER_ID', 'DISPLAY_NM', 'CUST_TYP', 'LIST_PRIOR_ALERTS', 'AUTO_REASON', 'AUTO_CLOSE']]

    # List to hold the generated narratives
    narratives = []

    # Iterate over each row where AUTO_CLOSE is 1
    for index, row in df[df['AUTO_CLOSE'] == 1].iterrows():
        narrative = get_autoclosure_narrative(row)
        if narrative:
            narratives.append(narrative)
        else:
            narratives.append(None)  # To handle cases where narrative generation fails

    # Add the narratives as a new column to the DataFrame
    df['NARRATIVE'] = narratives

    return df

# def generate_narratives_for_autoclosure(df):
#     df = df[['Alert_ID', 'ARS_TX', 'ACCT_ID', 'CUSTOMER_ID', 'DISPLAY_NM', 'CUST_TYP', 'LIST_PRIOR_ALERTS', 'AUTO_REASON', 'AUTO_CLOSE']]

#     # Filter rows where AUTO_CLOSE is 1
#     auto_close_rows = df[df['AUTO_CLOSE'] == 1]

#     if auto_close_rows.empty:
#         print("No alerts found with AUTO_CLOSE = 1.")
#         return df  # Return the original DataFrame if no rows match

#     # List to hold the generated narratives
#     narratives = []

#     # Iterate over each row where AUTO_CLOSE is 1
#     for index, row in auto_close_rows.iterrows():
#         narrative = get_autoclosure_narrative(row)
#         print(narrative)
#         if narrative:
#             narratives.append(narrative)
#         else:
#             narratives.append(None)  # Handle cases where narrative generation fails

#     # Ensure the length of the narratives matches the filtered DataFrame
#     if len(narratives) != len(auto_close_rows):
#         raise ValueError("Mismatch between number of narratives generated and number of rows.")

#     # Add the narratives as a new column to the filtered DataFrame
#     auto_close_rows['NARRATIVE'] = narratives

#     # Merge the narratives back into the original DataFrame
#     df.update(auto_close_rows)

#     return df



# # autoCloseAlerts_df = pd.read_csv("final_ars_score.csv")
# autoCloseAlerts_df = pd.read_excel(r"/home/hackit/Desktop/TM/SCENARIO_SPARK/TS_TRXN_MONITORING_SPARK/final_ars_score.xlsx")
# # Generate narratives
# narratives = generate_narratives_for_autoclosure(autoCloseAlerts_df)
# narratives.to_csv("AutoCloseNarrative.csv")



# def
# alert_investigation_data = pd.DataFrame({
#     'AGE   ': [],
#     'ALERT_PRIORITY': [alert_txt_values],
#     'ALERT_RISK': [trxn_ids],
#     'ALERT_TX': [int(self.job_details['ALERT_TYPE_ID'])],
#     'AUTO_CLS_CT': ['TS_JOB_UST_ACTIVITY'],
#     'AUTO_REASN_FL': self.job_details['JOB_ID'],
#     'CASE_FL': 'N',
#     'CLOSE_DT': customer_id,
#     'CLS_ID': customer_name,
#     'CNTRY_KEY_ID': self.current_date.strftime('%Y-%m-%d'),
#     'CNTRY_TYPE_CD': f"{(self.job_details['SCENARIO_ID'])}",
#     'CONFIG_ID' : int([config['CONFIG_ID'] for config in self.job_details['SCENARIO_CONFIG']][0]),
#     'CREAT_TS' : [0],
#     'CUSTOMER_ID' : [0],
#     'CUST_DSPLY_NM' : [0],
#     'EXTRL_REF_ID' : [0],
#     'EXTRL_REF_LINK' : [str(self.thresholds)],
#     'EXTRL_REF_SRC_ID': [alert_txt_values],
#     'ORIG_OWNER_SEQ_ID': [round(row['TOTAL_TRXN_AMOUNT'],2)],
#     'OWNER_SEQ_ID': cur_mon_end_date.strftime('%Y-%m-%d'),
#     'REPORTED_BY': cur_mon_start_date.strftime('%Y-%m-%d'),
#     'SCENARIO_DISPL_NM': cur_mon_start_date.strftime('%Y-%m-%d'),
#     'SCENARIO_ID': cur_mon_start_date.strftime('%Y-%m-%d'),
#     'ALERT_ID': cur_mon_start_date.strftime('%Y-%m-%d'),
#     'ALERT_AUTO_CLS_ID': cur_mon_start_date.strftime('%Y-%m-%d'),
#     'CRIME_SUB_TYPE_ID': cur_mon_start_date.strftime('%Y-%m-%d'),
#     'CRIME_TYPE_ID': cur_mon_start_date.strftime('%Y-%m-%d'),
#     'DUE_DATE': cur_mon_start_date.strftime('%Y-%m-%d'),
#     'ALRT_ID': cur_mon_start_date.strftime('%Y-%m-%d'),
#     'AMOUNT': cur_mon_start_date.strftime('%Y-%m-%d'),
#     'ALERT_DATE': cur_mon_start_date.strftime('%Y-%m-%d'),
#     'LAST_ACTVY_TYPE_CD': cur_mon_start_date.strftime('%Y-%m-%d'),
#     'LAST_LINK_FL': cur_mon_start_date.strftime('%Y-%m-%d'),
#     'STATUS_ID': cur_mon_start_date.strftime('%Y-%m-%d')

# })