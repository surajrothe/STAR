import pandas as pd
import logging
from ts_fetcher.util import SQLQueryExecutor
from datetime import datetime
from ts_risk_scoring.prior_alert_scoring import PriorAlertScorer
from ts_risk_scoring.country_risk_scoring import CountryRiskScorer
from ts_risk_scoring.threshold_risk_scoring import ThresholdRiskScorer
from scenario_logger import logger
class AlertRiskScorer:
    def __init__(self, job_details: dict, thresholds: dict) -> None:
        self.logger = logger
        self.job_details = job_details
        self.sql_executor = SQLQueryExecutor()
        self.threshold_risk = self.job_details.get('THRESHOLD_LEVEL_FL', '')
        self.threshold = thresholds
        self.active_countries = None
        alert_scores_query_path = r'base_sql_queries/ARS_SCORING_BUCKET.sql'
        self.score_bucket = self.fetch_ars_scores(alert_scores_query_path)
        self.logger.info(f"{self.job_details['SCENARIO_NAME']} AlertRiskScoring has been initialized.")

    def fetch_ars_scores(self, ars_query_base_path: str) -> dict:
        """
        fetch_ars_scores() - Fetches the alert scores from the database and organizes them into a dictionary
        based on the SCR_GRP_TYPE column.

        Returns:
            dict: Dictionary containing the alert scores.
        """
        try:
            alert_scores_query = self.sql_executor.load_sql_query(ars_query_base_path)
            alert_score_query = f"""
                {alert_scores_query}
                WHERE
                A.SCENARIO_ID = '{self.job_details['SCENARIO_ID']}'
                AND (A.IS_ACTIVE IS NULL OR A.IS_ACTIVE = 'true');
            """
            alert_score_data = self.sql_executor.execute_query(alert_score_query)

            if alert_score_data.empty:
                return {}

            # Initialize the resulting dictionary
            result_dict = {}
            scr_grp_types = alert_score_data['SCR_GRP_TYPE'].unique()

            for grp_type in scr_grp_types:
                result_dict[grp_type] = {
                    "WEIGHTAGE": alert_score_data.loc[alert_score_data['SCR_GRP_TYPE'] == grp_type, 'WEIGHTAGE'].iloc[0],
                    "ATTRIBUTES": {}
                }
                if grp_type == "PRIOR_ALERT":
                    result_dict[grp_type]["ATTRIBUTES"] = {
                        'ALERT_STATUSES': []
                    }
                elif grp_type == "THRESHOLD":
                    result_dict[grp_type]["ATTRIBUTES"] = []
                elif grp_type == "CUST_BEHAV":
                    result_dict[grp_type]["ATTRIBUTES"] = {
                        "PARTY_CNTRY": []
                    }

            self.active_countries = []
            country_list = ['CNTRY_RESIDENCE', 'CNTRY_CITIZENSHIPSTS']

            for _, row in alert_score_data.iterrows():
                if row['ATTR_NM'] in country_list:
                    self.active_countries.append(row['ATTR_NM'])

                if row['SCR_GRP_TYPE'] == "THRESHOLD":
                    threshold_attribute = {
                        "ATTR_NM": row['ATTR_NM'],
                        "MIN_VALUE": row['MIN_VALUE'],
                        "MAX_VALUE": row['MAX_VALUE'],
                        "ATTR_SCORE": row['ATTR_SCORE']
                    }
                    result_dict["THRESHOLD"]["ATTRIBUTES"].append(threshold_attribute)

                elif row['SCR_GRP_TYPE'] == "PRIOR_ALERT":
                    if row['ATTR_NM'] == 'PREVIOUS_ALERT':
                        prior_attribute = {
                            "ATTR_NM": row['ATTR_NM'],
                            "ALERT_STATUS": row['MIN_VALUE'],
                            "ATTR_SCORE": row['ATTR_SCORE']
                        }
                        result_dict["PRIOR_ALERT"]["ATTRIBUTES"]['ALERT_STATUSES'].append(prior_attribute)
                    else:
                        alert_count_attribute = {
                            "ATTR_NM": row['ATTR_NM'],
                            "MIN_VALUE": row['MIN_VALUE'],
                            "MAX_VALUE": row['MAX_VALUE'],
                            "ATTR_SCORE": row['ATTR_SCORE']
                        }
                        result_dict["PRIOR_ALERT"]["ATTRIBUTES"]['ALERT_COUNT'].append(alert_count_attribute)

                elif row['SCR_GRP_TYPE'] == "CUST_BEHAV":
                    if row['ATTR_NM'] == "CNTRY_PARTY":
                        cust_behav_attribute = {
                            "ATTR_NM": row['ATTR_NM'],
                            "HIGHRISK_FLAG": row['MIN_VALUE'],
                            "ATTR_SCORE": row['ATTR_SCORE']
                        }
                        result_dict["CUST_BEHAV"]["ATTRIBUTES"]["PARTY_CNTRY"].append(cust_behav_attribute)

            self.logger.info(f"Job ID - {self.job_details['JOB_ID']} | Alert Scoring attributes dictionary created successfully!")
            return result_dict

        except FileNotFoundError as e:
            self.logger.error(f"Job ID - {self.job_details['JOB_ID']} | Error: File not found in AlertRiskScorer - {e}")
            raise
        except KeyError as e:
            self.logger.error(f"Job ID - {self.job_details['JOB_ID']} | Error: Missing key in alert scores data - {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Job ID - {self.job_details['JOB_ID']} | Error: Alert scoring attributes retrieval encountered an error - {e}")
            return {}

    def get_autoClosure(self,alert_scores_df:pd.DataFrame)-> pd.DataFrame:
        try:
            # get thsoe alerts whose score is below certain value
            filter_alerts = alert_scores_df[
                (alert_scores_df['ALERT_SCORE'] <= 35)
                &
                ~(alert_scores_df['LIST_PRIOR_ALERTS'].apply(lambda x: any(status in ['NEW', 'ASSIGNED', 'PARKED','ESCALATED'] for status in x))) # exlcude all alerts from autoclosure where prior alert is new assigned parked or escalated
                ]

            # Filtering the DataFrame
            filter_alerts = filter_alerts[
                filter_alerts['AUTO_REASON'].apply(lambda x: 'alert_deemed_as_false-posetive' in x) #even if the alert status is closed, it will included in autoclosure only if the AUTO_REASN_FL is "alert_deemed_as_false-posetive"
            ]


            if not filter_alerts.empty:
                filter_alerts['AUTO_CLOSE'] = 1  # those who are filtered out after applying the condition are assigned as 1 stating they will be autoclosed

                filter_alerts = filter_alerts[['Alert_ID','AUTO_CLOSE']]
                #filter_alerts.to_csv("auto.csv")
                return filter_alerts
            else:
                return pd.DataFrame({'Alert_ID': [], 'AUTO_CLOSE': []})
        except Exception as e:
            self.logger.error(f"Error in get_autoClosure: {e}")
            raise e

    def alert_risk_scoring(self, alert_data) -> pd.DataFrame:
        """
        alert_risk_scoring() - Dynamically runs the alert risk scoring logic based on the defined module for the scenario.
        """
        try:
            # Dictionary to store the intermediate scores from different modules
            score_dfs = {}



            # Loop through each module in the score_bucket and run the corresponding scorer
            for module, module_info in self.score_bucket.items():
                if module == 'PRIOR_ALERT':
                    prior_alert_base_query = r'base_sql_queries/PRIOR_ALERT.sql'
                    prior_alert_scorer = PriorAlertScorer(prior_alert_base_query, self.job_details, self.score_bucket, alert_data)
                    score_dfs['PRIOR_ALERT'] = prior_alert_scorer.run_prior_alert_scoring()
                elif module == 'THRESHOLD':
                    ars_config_file_path = r'ts_risk_scoring/threshold_scoring_config.json'
                    ars_threshold_config = self.sql_executor.load_config_from_json(ars_config_file_path)
                    ars_threshold_config = ars_threshold_config[self.job_details['SCENARIO_ID']]
                    threshold_risk_score = ThresholdRiskScorer(self.job_details, self.threshold, self.score_bucket, self.threshold_risk, ars_threshold_config, alert_data)
                    score_dfs['THRESHOLD'] = threshold_risk_score.run_threshold_risk_scoring()

                elif module == 'CUST_BEHAV':
                    country_risk_score = CountryRiskScorer(self.job_details, self.active_countries, self.score_bucket, alert_data)
                    score_dfs['CUST_BEHAV'] = country_risk_score.run_country_risk_scoring()



            # Initialize the alert_scores_df with the first DataFrame in the score_dfs dictionary
            alert_scores_df = list(score_dfs.values())[0]



            # Merge all the score dataframes together on 'Alert_ID'
            for df_key, df in score_dfs.items():
                if df_key != list(score_dfs.keys())[0]:  # Skip the first one since it's already initialized
                    alert_scores_df = pd.merge(alert_scores_df, df, on='Alert_ID', how='inner')

            # Calculate final ALERT_SCORE based on the available scores and their weightages
            def calculate_alert_score(row):
                score = row['THRESHOLD_SCORE']  # Directly add the threshold score
                for module, module_info in self.score_bucket.items():
                    if module != 'THRESHOLD':  # Apply weight only for non-threshold modules
                        weight = float(module_info['WEIGHTAGE']) / 100
                        score += weight * row[f"{module}_SCORE"]
                return score

            alert_scores_df['ALERT_SCORE'] = alert_scores_df.apply(calculate_alert_score, axis=1)


            def assign_severity(row):
                score = row['ALERT_SCORE']
                # Assign severity and color code based on the score
                # if 1<= score <= 10:
                #     return 'Recommended Autoclosure'  # Green color code
                if row['AUTO_CLOSE'] == 1:
                    return 'Recommended Autoclosure'
                else:
                    if 0 < score < 35:
                        return 'Low'  # Green color code
                    elif 35 <= score < 70:
                        return 'Medium' # Yellow color code
                    elif 70 <= score <= 90:
                        return 'High'  # Red color code
                    else:
                        return 'Critical'  # Red color code

            filtered_alerts = self.get_autoClosure(alert_scores_df=alert_scores_df)

            # alert_scores_df = alert_scores_df[['Alert_ID', 'THRESHOLD_SCORE', 'PRIOR_ALERT_SCORE', 'CUST_BEHAV_SCORE_MEDIAN']]
            final_scores = pd.merge(alert_scores_df, filtered_alerts, on='Alert_ID', how='outer')

            final_scores['ALERT_PRIORITY'] = final_scores.apply(assign_severity, axis=1)
            final_scores.to_csv('final_ars_score.csv', index=False)
            final_scores.to_excel('final_ars_score.xlsx', index=False)
            return final_scores

        except Exception as e:
            self.logger.error(f"Error during alert risk scoring: {e}")
            raise e


# # Example usage:
# if __name__ == "__main__":
#     config_path = r'/home/hackit/Desktop/TM/SCENARIO_SPARK/TS_TRXN_MONITORING_SPARK/authjson/config.json'
#     job_details = {
#         'scenario_id': 'TS_SCN_01',
#         'scenario_nm': 'xyv',
#         'JOB_ID': '2',
#         'THRESHOLD_LEVEL_FL': 'INACTIVE',
#         'SCENARIO_FOCUS': 'CUSTOMER'
#     }
#     # scenario 6
#     # threshold = {'CONFIG_ID': 2, 'THRESHOLD_SET_ID': 37, 'LOOKBACK PERIOD': '5', 'ACCOUNT AGE': '6', 'TRANSACTION COUNT IND': '20', 'TRANSACTION COUNT ORG': '20', 'DEBIT TRANS AMOUNT IND': '500000', 'DEBIT TRANS AMOUNT ORG': '200000', 'CREDIT TRANS AMOUNT IND': '500000', 'CREDIT TRANS AMOUNT ORG': '700000'}

#     # scenario 20
#     threshold = {'CONFIG_ID': 8, 'THRESHOLD_SET_ID': 1, 'LOOKBACK PERIOD': '20', 'FREQUENCY PERIOD': '10', 'HRG % AMOUNT IND': '70', 'HRG % AMOUNT ORG': '90', 'HRG TRXN AMOUNT IND': '5000', 'HRG TRXN AMOUNT ORG': '7000', 'HRG TRXN COUNT IND': '3', 'HRG TRXN COUNT ORG': '5', 'INDIVIDUAL TRXN AMOUNT IND': '500', 'INDIVIDUAL TRXN AMOUNT ORG': '1000'}
#     alert_data =  SQLQueryExecutor(config_path).execute_query("""
#         SELECT
#             TA.Alert_ID,
#             TA.ALERT_TX AS ARS_TX,
#             TAC.CUST_ID,
#             TAC.ACCT_ID,
#             TT.TRXN_ID,
#             TTP.TRXN_PARTY_ID,
#             TTP.PARTY_CNTRY as PARTY_CNTRY_NAME,
#             TC.CTZNSHIP_STS,
#             TC.CNTRY_OF_INCORP,
#             TC.CNTRY_OF_RESIDENCE,
#             TC.CUST_TYP
#         FROM TS_ALERT TA
#         JOIN TS_ALERT_CUST TAC ON TA.Alert_ID = TAC.Alert_ID
#         LEFT JOIN TS_ALERT_TRXN TAT ON TA.Alert_ID = TAT.Alert_ID
#         JOIN TS_TRXN TT ON TAT.TRXN_ID = TT.TRXN_ID
#         JOIN TS_TRXN_PARTY TTP ON TT.TRNX_PARTY_ID = TTP.TRXN_PARTY_ID
#         JOIN TS_CUST TC ON TAC.CUST_ID = TC.CUST_ID
#         WHERE TA.SCENARIO_ID = 'TS_SCN_01'
#         order by TA.Alert_ID

#         """)
#     # alert_data.to_csv("alert_data.csv", index=False)
#     # alert_data = pd.read_csv("alert_data.csv")
#     alert_risk_scorer = AlertRiskScorer(config_path, job_details, threshold, alert_data)
#     alert_risk_scorer.alert_risk_scoring()
