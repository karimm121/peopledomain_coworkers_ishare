from utils import time_decorator,access_secret_version,get_secrets
from loguru import logger
import json, requests
from vars import PAOM_TABLE_ASIS, RECORDS_LIMIT, DATASET, MISSING_KEY_ATT_DATA
import pandas as pd
from utils import write_db_table, replace_nan_to_null


#----------------Edit:17/06/2024----------------
def fetch_missing_data(df):
    columns = ['emailId', 'costCentre', 'managerEmployeeId', 'managerUid',
                'homeSite', 'uid', 'guid', 'companyRegion']

    # Filter the input dataframe to only include rows with missing data in any of the specified columns
    filtered_df = df[df[columns].isnull().any(axis=1)]
    if filtered_df.empty:
        return pd.DataFrame()

    # Convert the filtered dataframe to string and replace NaN values with null
    missing_attr_df = replace_nan_to_null(filtered_df.astype(str))
    return missing_attr_df
#-----------------------------------------------

class PersonAPI():
    def call_person_api(self):
        secrets = get_secrets()

        headers = {'Content-Type': 'application/json'}
        payload = json.dumps({
            "client_id": secrets[2],
            "client_secret": secrets[3],
            "audience": secrets[1],
            "grant_type": "client_credentials",
            "content-type": "application/x-www-form-urlencoded"
        })

        logger.info("Health check Person API....")
        
        try:
            response = requests.post(secrets[4], headers=headers, data=payload)
            response.raise_for_status()  # Raise an exception for HTTP errors
        except requests.exceptions.HTTPError as err:
            logger.error(f"API did not respond successfully! Error: {err}")
            return False, pd.DataFrame()
        
        access_token = response.json().get('access_token')
        headers.update({'x-api-key': secrets[2], 'Authorization': f'Bearer {access_token}'})
        
        paom_api_url = f"{secrets[0]['PAOM_PROD_ENDPOINT']}?recordLimit={RECORDS_LIMIT}&filter=employmentStatus=='active'"
        logger.info("Calling Person API without delta..!")
        
        try:
            response = requests.get(paom_api_url, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
        except requests.exceptions.HTTPError as err:
            logger.error(f"Failed to connect to API; Error: {err}")
            return False, pd.DataFrame()
        
        data = response.json().get('peopleDomain', {}).get('paom', {}).get('data', {})
        paom_api_data = data.get('persons', [])
        
        payload = {}  # Not needed next time, hence emptied
        while data.get('recordsFetched', 0) != 0:
            try:
                response = requests.get(f"{paom_api_url}&delta={data.get('delta')}", headers=headers)
                response.raise_for_status()  # Raise an exception for HTTP errors
                data = response.json().get('peopleDomain', {}).get('paom', {}).get('data', {})
                paom_api_data.extend(data.get('persons', []))
                logger.info(f"Record#: {len(paom_api_data)} @ Delta: [{data.get('delta')}] ")
            except requests.exceptions.HTTPError as err:
                logger.error(f"Failed to fetch delta data; Error: {err}")
                break

        df_paom = pd.DataFrame(paom_api_data)

        logger.info("Blocking RU Data...") 
        df_paom = df_paom.query("countryKey != 'RU'")
        
        #storing asis data to paom table asis
        logger.info(f"Saving AS-IS to table: {PAOM_TABLE_ASIS} with shape: {df_paom.shape} ")
        status = write_db_table(
            bq_dataset=DATASET,
            bq_tname=PAOM_TABLE_ASIS,
            dataframe=df_paom  # AS-IS
        )

        #storing missing data to paom table asis
        output_df = fetch_missing_data(df_paom)
        logger.info(f"Saving missing data into table: Missing_Key_Att_Data with shape: {output_df.shape} ")
        status = write_db_table(
            bq_dataset=DATASET,
            bq_tname=MISSING_KEY_ATT_DATA,
            dataframe=output_df
        )
        
        if status:
            return df_paom
        else:
            return pd.DataFrame()