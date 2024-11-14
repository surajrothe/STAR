import pandas as pd
import logging
from ts_fetcher.util import SQLQueryExecutor
from scenario_logger import logger
class CountryRiskScorer:
    def __init__(self, job_details: dict, active_countries: list, score_bucket: dict, alert_data: pd.DataFrame) -> None:
        self.logger = logger
        self.sql_executor = SQLQueryExecutor()
        self.job_details = job_details
        self.active_countries = active_countries
        self.score_bucket = score_bucket
        self.alert_data = alert_data

    def get_country_risk_flags(self, dataframe:pd.DataFrame) -> pd.DataFrame:
        try:
            query = f"SELECT * FROM TS_COUNTRY"
            country_df = self.sql_executor.execute_query(query)

            # # Filter out rows where COUNTRY_NAME is None
            # country_df.dropna(subset=['COUNTRY_NAME'], inplace=True)

            # # Map each country id and its corresponding name from the country_df thus creating a dictionary
            # country_id_to_name = dict(zip(country_df['COUNTRY_ID'], country_df['COUNTRY_NAME']))

            # dataframe['CNTRY_OF_INCORP'].replace('',0, inplace=True)
            # dataframe['CNTRY_OF_RESIDENCE'].replace('',0, inplace=True)
            # # save each country names to its corresponding columns
            # dataframe['CNTRY_OF_RESIDENCE'] = (dataframe['CNTRY_OF_RESIDENCE'].astype('int64')).map(country_id_to_name)
            # dataframe['CNTRY_OF_INCORP'] = (dataframe['CNTRY_OF_INCORP'].astype('int64')).map(country_id_to_name)

            # print(f"dataframe: {dataframe}")

            def merge_country_flags(df:pd.DataFrame, column_name:str) -> pd.DataFrame:
                unique_country_df = country_df.drop_duplicates('COUNTRY_NAME')  # Remove duplicates
                df[column_name + '_HIGHRISK_FLAG'] = df[column_name].map(unique_country_df.set_index('COUNTRY_NAME')['HIGHRISK_FLAG'])
                return df

            # Merge for each column
            dataframe = merge_country_flags(dataframe, 'CNTRY_OF_RESIDENCE')
            dataframe = merge_country_flags(dataframe, 'CNTRY_OF_INCORP')
            dataframe = merge_country_flags(dataframe, 'PARTY_CNTRY_NAME')
            dataframe = merge_country_flags(dataframe, 'CTZNSHIP_STS')
            self.logger.info(f"Scenario - {self.job_details['SCENARIO_NAME']} | Country risk flags fetched successfully.")

            # dataframe.to_csv('country_risk_flags.csv', index=False)
            return dataframe
        except ValueError as ve:
            self.logger.info(f"Scenario - {self.job_details['SCENARIO_NAME']} | Value Error occurred while fetching country risk flags {ve}.")
        except KeyError as ke:
            self.logger.info(f"Scenario - {self.job_details['SCENARIO_NAME']} | Key Error occurred while fetching country risk flags {ke}.")
        except Exception as e:
            self.logger.info(f"Scenario - {self.job_details['SCENARIO_NAME']} | Error occurred while fetching country risk flags {e}.")
            # print(e)
            raise e

    def country_risk_score(self, row: pd.Series, citizenship_active_flag: bool, residence_active_flag:bool) -> float:
        try:
            cust_type = row['CUST_TYP']
            ctznship_highrisk_flag = row['CTZNSHIP_STS_HIGHRISK_FLAG']
            residence_highrisk_flag = row['CNTRY_OF_RESIDENCE_HIGHRISK_FLAG']
            incorp_highrisk_flag = row['CNTRY_OF_INCORP_HIGHRISK_FLAG']
            party_cntry_highrisk_flag = row['PARTY_CNTRY_NAME_HIGHRISK_FLAG']

            highrisk_Score = self.score_bucket['CUST_BEHAV']['ATTRIBUTES']['PARTY_CNTRY']

            # Accessing the scores for PARTY_CNTRY with HIGHRISK_FLAG 'N' and 'Y'
            score_flag_no = next(attr['ATTR_SCORE'] for attr in highrisk_Score if attr['HIGHRISK_FLAG'] == 'N')
            score_flag_yes = next(attr['ATTR_SCORE'] for attr in highrisk_Score if attr['HIGHRISK_FLAG'] == 'Y')

            cntry_score = 0
            frequency_of_Y = 0

            """Country Risk Scoring Logic Explained:

                Conditions applied only for customer type of IND (Individual)
                    Based on what customer has selected both citizenship and residence country to be compared with the transaction country , we will evaluate the score based on 3*8 matrix

                    if the customer has selected only one country, then only one country will be considered for comparison and calculation of scores
                    citizenship	residence	trxn cntry
                    Y	Y	Y
                    Y	N	Y
                    N	Y	Y
                    N	N	Y
                    Y	Y	N
                    Y	N	N
                    N	N	N
                    N	Y	N


                    all below combinations ending with Y (that is the trxn country/ party country has high risk flag of Y) will get higher score as they fit in the condition of higher risks
                    Y	Y	Y
                    Y	N	Y
                    N	Y	Y
                    N	N	Y

                    use this formula to calculate the score : Score(Y) - frequencey(Y) * score (N)

                    all ending with N (trxn cntry/ party cntry having highrisk flag as N) will get relatively lower score than the high risk flag Y
                    Y	Y	N
                    Y	N	N
                    N	N	N
                    N	Y	N

                    Score(N) + frequencey(Y) * score (N)

                Condition applied to customer type of ORG (Organisation)
                    if the customer is of type ORG, then only the country of incorporation will be considered for comparison and calculation of scores
                    incorporation country	trxn cntry
                    Y	Y
                    N	Y
                    Y   N
                    N   N

                    when trxn cntry has high risk flag as Y
                    score_flag_yes - frequency_of_Y * score_flag_no

                    when trxn cntry has high risk flas as N
                    score_flag_no + frequency_of_Y * score_flag_yes

                    Same formula is used for IND when only one country is active

                    """

            # CALCULATES THE CNTRY SCORE WHEN A SINGLE COUNTRY IS SET TO ACTIVE OR THERE IS ONLY ONE COUNTRY IN COMPARISON WITH PARTY COUNTRY
            def calculate_single_active_country_score(party_country_flag:str, score_flag_yes:float, frequency_of_Y:int, score_flag_no:float) -> int:
                active_cntry_score = 0
                if party_country_flag == 'Y':
                    active_cntry_score = score_flag_yes - frequency_of_Y * score_flag_no
                elif party_country_flag == 'N':
                    active_cntry_score = score_flag_no + frequency_of_Y * score_flag_yes
                return active_cntry_score

            # CALCULATES THE CNTRY SCORE WHEN A BOTH COUNTRIES ARE SET TO ACTIVE OR THERE ARE TWO COUNTRY IN COMPARISON WITH PARTY COUNTRY
            def calculate_both_active_country_score(party_country_flag: str, score_flag_yes:float, frequency_of_Y: int, score_flag_no: float) -> int:
                dual_active_cntry_score = 0
                if party_country_flag == 'Y':
                    dual_active_cntry_score =  score_flag_yes - frequency_of_Y * score_flag_no
                elif party_country_flag == 'N':
                    dual_active_cntry_score = score_flag_no + frequency_of_Y * score_flag_no
                return dual_active_cntry_score

            if cust_type == 'IND':
                frequency_of_Y = sum(1 for flag in [ctznship_highrisk_flag, residence_highrisk_flag] if flag == 'Y')

                if citizenship_active_flag and residence_active_flag:
                    cntry_score = calculate_both_active_country_score(party_cntry_highrisk_flag, score_flag_yes, frequency_of_Y, score_flag_no)

                elif citizenship_active_flag:
                    cntry_score = calculate_single_active_country_score(party_cntry_highrisk_flag, score_flag_yes, frequency_of_Y, score_flag_no)
                elif residence_active_flag:
                    cntry_score = calculate_single_active_country_score(party_cntry_highrisk_flag, score_flag_yes, frequency_of_Y, score_flag_no)

            elif cust_type == 'ORG':
                frequency_of_Y = sum(1 for flag in [incorp_highrisk_flag] if flag == 'Y')
                cntry_score = calculate_single_active_country_score(party_cntry_highrisk_flag, score_flag_yes, frequency_of_Y, score_flag_no)

            return round(int(cntry_score), 2)
        except KeyError as e:
            self.logger.info(f"Scenario - {self.job_details['SCENARIO_NAME']} | Key Error occurred while calculating country risk score {e}.")
            raise e
        except ValueError as e:
            self.logger.info(f"Scenario - {self.job_details['SCENARIO_NAME']} | Value Error occurred while calculating country risk score {e}.")
            raise e
        except IndexError as e:
            self.logger.info(f"Scenario - {self.job_details['SCENARIO_NAME']} | Index Error occurred while calculating country risk score {e}.")
            return 0.0
        except Exception as e:
            # Capture and return detailed error information
            error_info = {
                "error": str(e),
                "row_data": row.to_dict()
            }
            # print(e)
            self.logger.info(f"Scenario - {self.job_details['SCENARIO_NAME']} | Error Occurred while calculating country risk score {error_info}.")
            raise e

    def run_country_risk_scoring(self) -> pd.DataFrame:
        try:
            country_risk_df = self.get_country_risk_flags(dataframe=self.alert_data)

            citizenship_active_flag = False
            residence_active_flag = False
            if 'CNTRY_RESIDENCE' in self.active_countries:
                residence_active_flag = True
            if 'CNTRY_CITIZENSHIPSTS' in self.active_countries:
                citizenship_active_flag = True

            country_risk_df['CUST_BEHAV_SCORE'] = country_risk_df.apply(lambda row: self.country_risk_score(row, citizenship_active_flag, residence_active_flag), axis=1)

            country_risk_df = country_risk_df.groupby('Alert_ID')['CUST_BEHAV_SCORE'].max().reset_index()
            self.logger.info(f"Scenario - {self.job_details['SCENARIO_NAME']} | Country risk score calculated successfully.")
            return country_risk_df
        except Exception as e:
            self.logger.info(f"Scenario - {self.job_details['SCENARIO_NAME']} | Error occurred in Country Risk Scoring Module {e}.")
            raise e
