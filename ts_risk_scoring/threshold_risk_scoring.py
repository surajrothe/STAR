import pandas as pd
import ast
import json
import json
import json
from scenario_logger import logger
class ThresholdRiskScorer:
    def __init__(self,job_details:dict,threshold:dict,score_bucket:dict,threshold_risk:str,ars_threshold_config:dict,alert_data:pd.DataFrame) -> None:
        self.job_details = job_details
        self.threshold = threshold # get the threshold dictionary of the scenario
        self.score_bucket = score_bucket # scoring json for the ARS
        self.alert_data = alert_data # dataframe to apply scoring on
        self.threshold_risk = threshold_risk # defines whether to run threshold risk scoring on only IND/ ORG or HIGH/MED/LOW risk
        self.ars_threshold_config = ars_threshold_config
        self.logger = logger

    def calculate_deviation(self,value, threshold):
            # Check if the value is 0 to avoid division by zero
            if value <= 0:
                return 0

            # Calculate deviation from the threshold in percentage using the provided deviation function
            deviation_percentage = ((value - threshold) / threshold) * 100

            if deviation_percentage < 0:
                return 0
            return round(deviation_percentage, 2)



    def get_score(self, value, attributes, attr_nm, score_key):
        if value < 0:
            return 0
        if 0 <= value < 1:
            return 10

        attr_list = [entry for entry in attributes if entry.get('ATTR_NM') == attr_nm]
        last_score = None
        for entry in attr_list:
            if 'MIN_VALUE' in entry and 'MAX_VALUE' in entry:
                if float(entry['MIN_VALUE']) <= value <= float(entry['MAX_VALUE']):
                    return int(entry[score_key])  # Convert the score to an integer
                else:
                    last_score = int(entry[score_key])

        if last_score is not None:
            if last_score > 50:
                return last_score + 5
            else:
                return last_score - 5

        return 50


    # def get_score(value, attributes, attr_nm, score_key):
    #     if value < 0:
    #         return 0
    #     if 0 <= value < 1:
    #         return 10

    #     attr_list = [entry for entry in attributes if entry.get('ATTR_NM') == attr_nm]
    #     for entry in attr_list:
    #         if 'MIN_VALUE' in entry and 'MAX_VALUE' in entry:
    #             if float(entry['MIN_VALUE']) <= value <= float(entry['MAX_VALUE']):
    #                 return int(entry[score_key])  # Convert the score to an integer

    #     return 95

    def calculate_change(self,value,threshold):
        if value <= 0:
            return 0

        # calculate the change in number
        change = (value - threshold)
        if change < 0:
            return 0
        return change

    def threshold_alert_score(self, result_df, fields_config, thresholds_config):
        if not result_df.empty:
            try:
                # Assuming 'Alert_ID' is a column in result_df
                grouped = result_df.groupby('Alert_ID')

                # Parse ARS_TX column
                result_df['ARS_TX'] = grouped['ARS_TX'].transform(lambda x: x.apply(lambda y: ast.literal_eval(y) if isinstance(y, str) else y))
                result_df['CUST_TYPE'] = result_df['ARS_TX'].apply(lambda x: str(x.get('CUSTOMER TYPE')) if x.get('CUSTOMER TYPE') else None)
                result_df['CUSTOMER_RISK'] = result_df['ARS_TX'].apply(lambda x: str(x.get('CUSTOMER RISK')) if x.get('CUSTOMER RISK') else None)

                # Dynamically extract necessary fields from ARS_TX based on the fields_config
                for field_info in fields_config:
                    source_field = field_info['ars_tx_source_field']
                    result_column = field_info['data_column']
                    default_value = field_info.get('default_value', 0)

                    result_df[result_column] = result_df['ARS_TX'].apply(lambda x: str(x.get(source_field)) if x.get(source_field) else default_value)

                # Function to determine the correct threshold based on customer risk and account type
                def get_threshold(threshold_type, customer_risk, cust_type):

                    if self.threshold_risk == 'ACTIVE':
                        key = f"{threshold_type} {cust_type} {customer_risk}"

                    else:
                        key = f"{threshold_type} {cust_type}"

                    return float(self.threshold.get(key, 0))  # Default to 0 if key is not found

                # Iterate through the thresholds dynamically
                for threshold_info in thresholds_config:
                    threshold_name = threshold_info['threshold_name']
                    deviation_column = threshold_info['deviation_column']
                    score_column = threshold_info['score_column']
                    ars_attribute_name = threshold_info['ars_attribute_name']
                    data_column = threshold_info['data_column']
                    deviation_type = threshold_info['deviation_type']

                    # Apply the get_threshold function to each row to get the threshold value
                    result_df[threshold_info['threshold_column']] = result_df.apply(
                        lambda row: get_threshold(
                            eval(threshold_name[5:], {'row': row}) if threshold_name.startswith('eval') else threshold_name,
                            # eval(threshold_name) if isinstance(threshold_name, str) and 'if' in threshold_name else threshold_name,
                            row['CUSTOMER_RISK'], row['CUST_TYPE']),
                        axis=1
                    )

                    # Dynamically decide which method to use for calculating deviation
                    if deviation_type == 'calculate_deviation':
                        result_df[deviation_column] = result_df.apply(
                            lambda row: round(self.calculate_deviation(float(row[data_column]), threshold=row[threshold_info['threshold_column']]), 2),
                            axis=1
                        )
                    elif deviation_type == 'calculate_change':
                        result_df[deviation_column] = result_df.apply(
                            lambda row: round(self.calculate_change(float(row[data_column]), threshold=row[threshold_info['threshold_column']]), 2),
                            axis=1
                    )

                    # Calculate the score based on the deviation
                    result_df[score_column] = result_df[deviation_column].apply(
                        lambda x: self.get_score(x, self.score_bucket["THRESHOLD"]['ATTRIBUTES'], ars_attribute_name, score_key='ATTR_SCORE')
                    )

                # Dynamically calculate the final threshold score
                threshold_count = len(thresholds_config)
                weight_per_attr = (float(self.score_bucket['THRESHOLD']['WEIGHTAGE']) / 100) / threshold_count

                result_df['THRESHOLD_SCORE'] = sum(
                    (weight_per_attr * result_df[threshold_info['score_column']]) for threshold_info in thresholds_config
                ).round(2)
                # result_df.to_csv('threshold_score.csv', index=False	)
                return result_df

            except KeyError as e:
                self.logger.error(f"Key error in Threshold Risk scoring {e}")
                raise
            except ValueError as e:
                self.logger.error(f"Value error in Threshold Risk scoring {e}")
                raise
            except Exception as e:
                self.logger.error(f"Error in Threshold Risk Scoring: {e}")
        else:
            return pd.DataFrame()

    def run_threshold_risk_scoring(self) -> pd.DataFrame:
        try:
            fields_config = self.ars_threshold_config['fields_config']
            thresholds_config = self.ars_threshold_config['thresholds_config']
            threshold_score_df = self.threshold_alert_score(self.alert_data, fields_config, thresholds_config)
            threshold_score_df.drop_duplicates(subset=['Alert_ID'], inplace=True)
            # return threshold_score_df[['Alert_ID', 'ARS_TX', 'ACCT_ID', 'CUSTOMER_ID', 'DISPLAY_NM', 'CUST_TYP', 'LIST_PRIOR_ALERTS', 'AUTO_REASON', 'AUTO_CLOSE']]
            # threshold_score_df.to_csv('threshold_score.csv', index=False)
            return threshold_score_df
        except Exception as e:
            self.logger.error(f"Error in threshold_risk_scoring: {e}")
            raise e

