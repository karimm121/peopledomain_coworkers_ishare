import time
import google.auth
import numpy as np
import pandas as pd
import datetime as dt
from loguru import logger
from functools import wraps
from google.cloud import bigquery
from google.cloud import secretmanager, storage
from vars import paom_date_format_tb,paom_date_format
from vars import DATASET_LOCATION,ALLOWED_EMAIL_DOMAINS,DATASET, paom_date_format_tb,common_PAOM_cols_l,mapping_dict
from vars import MASTER_DATASET,org_unit_mapping, country_list,poam_drop_cols_l,poam_drop_cols_l
from exceptions import emptyDataFrame

credential, project_id = google.auth.default()
def time_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Execution time for {func.__name__}: {execution_time} seconds")
        return result
    return wrapper

@time_decorator
def write_db_table(
        bq_dataset: str,
        bq_tname: str,
        dataframe: pd.DataFrame) -> bool:
    
    client = bigquery.Client()
    dataset_table = f"{project_id}.{bq_dataset}.{bq_tname}"
    dataframe = dataframe.sort_index(axis=1)
    dataframe = dataframe.astype(str)
    dataframe = replace_nan_to_null(dataframe)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(name=col, field_type="STRING")
            for col in dataframe.columns
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_dataframe(
        dataframe, dataset_table, job_config=job_config
    )
    try:
        job.result()  # Wait for the job to complete
        logger.info(
            f"Write [{bq_tname}] table with stats: {dataframe.shape} - OK!"
        )
        return True
    except Exception as e:
        logger.error(f"Error writing to BigQuery table [{bq_tname}]: {e}")
        return False

@time_decorator
def load_table_to_dataframe(query):
    df = pd.DataFrame()
    df = pd.read_gbq(query,
                        project_id=project_id,
                        location=DATASET_LOCATION,
                        credentials=credential)
    logger.info(f"Dataframe stats: [{df.shape}]")
    return True, df

@time_decorator
def get_managers(df_paom):  
    try:
        df_mgr = df_paom.merge(right=df_paom,
                                how='left',
                                left_on='ManagerEmployeeId',
                                right_on='EmployeeId',
                                suffixes=['', '_Manager']
                                )
        sub_cols_l = ['EmailAddress_Manager',
                        'FirstName_Manager', 'LastName_Manager']
        logger.info('Selecting required columns')
        df_refined = df_mgr[list(df_paom.columns) + sub_cols_l]
        drop_earlier_mgr_cols_l = [
            'ManagerFirstName',
            'ManagerLastName',
            'ManagerEmailAddress']
        df_refined.drop(columns=drop_earlier_mgr_cols_l, inplace=True)
        mgr_renamed_dict = {
            'EmailAddress_Manager': 'ManagerEmailAddress',
            'FirstName_Manager': 'ManagerFirstName',
            'LastName_Manager': 'ManagerLastName'
        }
        logger.info('Renaming columns')
        df_refined.rename(columns=mgr_renamed_dict, inplace=True)
        logger.info('Proceeding with refined DF')
        logger.info(
            'Find managers and define manager flag with Manager or Coworker')
        df_mgr_ext = df_paom.groupby(df_paom.ManagerEmployeeId).count()
        df_mgr_ext = df_mgr_ext.reset_index()
        df_mgr_ext = pd.DataFrame(df_mgr_ext[df_mgr_ext.EmployeeId > 1][[
            'ManagerEmployeeId', 'EmployeeId']])
        df_mgr_ext['ManagerFlag_'] = '1'
        df_mgr_ext.rename(
            columns={'EmployeeId': 'ReporteeCounts'}, inplace=True)
        df_mgr_ext.drop(['ReporteeCounts'], axis=1, inplace=True)
        df_refined.loc[df_refined.EmployeeId.isin(
            df_mgr_ext.ManagerEmployeeId), 'ManagerFlag_'] = '1'
        df_refined['ManagerFlag_'].fillna('0', inplace=True)

        return True, df_refined[common_PAOM_cols_l].drop_duplicates()
    except Exception as ex:
        logger.critical(
            f"Exeption: {ex}")
        return False, pd.DataFrame()

@time_decorator
def get_country_retail_managers(df):
    df.loc[df.EmployeeId.isin(df.query(
        "JobTitle.str.contains('Country Retail Manager', na=False) ").EmployeeId), 'isCountryRetailManager'] = 'Yes'
    crm_count = df[df.isCountryRetailManager ==
                    'Yes'].EmployeeId.count()
    logger.info(
        f"Country Retail Manager Count: {crm_count}  out of total: {df.shape}  ")
    return True, df

# Code added for offboarding list processing to ishare integration - Neeraj
@time_decorator
def read_db_table_query(query):
    df = pd.DataFrame()
    client = bigquery.Client()
    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe()
    logger.info(f"Dataframe populated; stats: [{df.shape}]")
    return df

@time_decorator
def get_offboarding_list_data(table_name):
    df = pd.DataFrame()
    sql = f'select * from {project_id}.{DATASET}.{table_name}'
    df = read_db_table_query(sql)
    return df

def access_secret_version(secret_id):
    version_id = "latest"
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode('UTF-8')

def get_secrets():
    PAOM_PROD_BASE_URL = PAOM_PROD_CLIENT_ID = PAOM_PROD_CLIENT_SECRET = POAM_PROD_TOKEN_ENDPOINT = ''
    secret_id_l = ["PAOM_PROD_BASE_URL", "PAOM_PROD_CLIENT_ID",
                    "PAOM_PROD_CLIENT_SECRET", "PAOM_PROD_ENDPOINT",
                    "PAOM_PROD_TOKEN_URL"]
    secret_id_values_l = []

    for index, secret_id in enumerate(secret_id_l):
        secret_id_values_l.append(
            access_secret_version(secret_id))

    client_ids_dict = dict(zip(secret_id_l, secret_id_values_l))

    PAOM_PROD_BASE_URL = client_ids_dict["PAOM_PROD_BASE_URL"]
    PAOM_PROD_CLIENT_ID = client_ids_dict["PAOM_PROD_CLIENT_ID"]
    PAOM_PROD_CLIENT_SECRET = client_ids_dict["PAOM_PROD_CLIENT_SECRET"]
    POAM_PROD_TOKEN_ENDPOINT = client_ids_dict["PAOM_PROD_TOKEN_URL"]
    return client_ids_dict, PAOM_PROD_BASE_URL, PAOM_PROD_CLIENT_ID, PAOM_PROD_CLIENT_SECRET, POAM_PROD_TOKEN_ENDPOINT

@time_decorator
def paom_raw_data_refinement(paom_raw_data_df):
    logger.info('refining data based on employee id')
    paom_raw_data_df = paom_raw_data_df[paom_raw_data_df['employeeId'].str.contains(
        r'^\d{8}$', na=False)]

    logger.info('Refining data based on email address domain names')
    allowed_domains = [domain.lower().strip()
                        for domain in ALLOWED_EMAIL_DOMAINS.split(',')]
    paom_raw_data_df = paom_raw_data_df[paom_raw_data_df["emailId"].str.split(
        "@").str[-1].str.lower().isin(allowed_domains)]

    logger.info("Refining data based on termination date")
    terminated_df = paom_raw_data_df[pd.to_datetime(
        paom_raw_data_df.terminationDate, errors='coerce') <= pd.to_datetime('today')]
    paom_raw_data_df = paom_raw_data_df.drop(
        paom_raw_data_df[paom_raw_data_df['employeeId'].isin(terminated_df['employeeId'])].index)

    logger.info(
        'Refining data based on cdsUpdateTimeStamp or cdsCreateTimeStamp')
    paom_raw_data_df['emailId'] = paom_raw_data_df['emailId'].str.lower()
    grouped_email = paom_raw_data_df.groupby('emailId').filter(lambda x: len(
        x) > 1).sort_values(['cdsUpdateTimeStamp', 'cdsCreateTimeStamp'], ascending=False)
    filtered = grouped_email.drop_duplicates('emailId', keep='first')
    filtered_email = list(filtered["emailId"])
    temp_df = paom_raw_data_df[~paom_raw_data_df['emailId'].isin(
        filtered_email)]
    paom_raw_data_df = pd.concat(
        [temp_df, filtered], ignore_index=True)
    return paom_raw_data_df


def get_tenures(tenure_age):
    tenure_ranges = [
        (0, 1, '< 1 month'),
        (1, 3, '1-3 months'),
        (4, 6, '4-6 months'),
        (7, 12, '7-12 months'),
        (13, 24, '1-2 years'),
        (25, 60, '3-5 years'),
        (61, 120, '6-10 years'),
        (121, 180, '11-15 years'),
        (181, float('inf'), '> 15 years'),  # Use float('inf') for open-ended range
    ]

    for lower_bound, upper_bound, tenure_label in tenure_ranges:
        if lower_bound <= tenure_age <= upper_bound:
            return tenure_label

    return ''  # Return an empty string if tenure age doesn't fall into any category

def get_age_group(age):
  age_ranges = [
      (0, 20, '< 20 years'),
      (21, 25, '21-25 years'),
      (26, 30, '26-30 years'),
      (31, 35, '31-35 years'),
      (36, 40, '36-40 years'),
      (41, 45, '41-45 years'),
      (46, 50, '46-50 years'),
      (51, 55, '51-55 years'),
      (56, float('inf'), '> 55 years'),  # Use float('inf') for open-ended range
  ]

  for lower_bound, upper_bound, age_label in age_ranges:
      if lower_bound <= age <= upper_bound:
          return age_label

  return ''  # Return an empty string if age doesn't fall into any category


def age_validation(age):
    if age == 0:
        return ''
    elif age < 15 or age > 90:
        return 'check age'
    else:
        return str(age)
        
def set_age_and_tenures(df_paom_final):
    logger.info(
        "Setting Age, AgeGroup, Tenure, TenureGroup on final DF")
    # Get Emp's age and agegroup
    df_paom_final["Age"] = (
        (dt.datetime.today() -
            pd.to_datetime(
            df_paom_final.BirthDate,
            format=paom_date_format_tb,
            errors='coerce')) /
        np.timedelta64(
            1,
            'Y')).round().fillna(0).astype(int)
    df_paom_final['AgeGroup'] = df_paom_final.Age.apply(get_age_group)

    # Tenure_Age calc
    df_paom_final["Tenure"] = (
        (dt.datetime.today() -
            pd.to_datetime(
            df_paom_final.StartDateInIngka,
            format=paom_date_format_tb,
            errors='coerce')) /
        np.timedelta64(
            1,
            'M')).round().fillna(0).astype(int)
    df_paom_final['TenureGroup'] = df_paom_final.Tenure.apply(get_tenures)
    df_paom_final['Tenure'] = np.where(
        df_paom_final['StartDateInIngka'].values is None,
        'check start date',
        df_paom_final['Tenure'])
    df_paom_final['TenureGroup'] = np.where(
        df_paom_final['StartDateInIngka'].values is None,
        'check start date',
        df_paom_final['TenureGroup'])
    df_paom_final["Age"] = df_paom_final["Age"].apply(age_validation)
    df_paom_final["AgeGroup"] = np.where(
        df_paom_final["Age"] == 'check age', '', df_paom_final["AgeGroup"])
    df_paom_final["Tenure"] = np.where(
        df_paom_final['Tenure'] == 0, '', df_paom_final['Tenure'])

    return True, df_paom_final

@time_decorator
def set_master_data_new(df_paom):
    # [ Set BUSINESS UNIT NAME ]
    buname_sql = f"select BusinessUnitCode ,BusinessUnitType ,BusinessUnitName from `{project_id}.{MASTER_DATASET}.BusinessUnitData` "
    status, bu_codes_df = load_table_to_dataframe(buname_sql)
    if not status:
        raise emptyDataFrame

    logger.info("Setting businessUnitName")
    # exclude RU from businessUnitName calculation
    ru_data = df_paom.query("Country == 'RU'").copy()
    other_data = df_paom.query("Country != 'RU'").copy()
    df_paom_l = list(df_paom.columns)
    other_data.drop(
        columns=['BusinessUnitName'],
        inplace=True)  # Dropping placeholder
    other_data = pd.merge(other_data, bu_codes_df, on=[
        'BusinessUnitCode', 'BusinessUnitType'], how='left')
    other_data = other_data[df_paom_l]
    df_paom_master = pd.concat(
        [other_data, ru_data], ignore_index=True)
    df_paom_master = df_paom_master.drop_duplicates(
        subset='EmployeeId', keep="first")

    return True, df_paom_master


def calculate_contract_type(row):
    if not row['ContractType']:
        return None
    try:
        employmentPercentage = float(str(row['ContractType']))
        if employmentPercentage < 100.0:
            return '1'
        else:
            return '0'
    except ValueError:
        return None

def rename_columns(df_paom, header_list):
    logger.info(f"Finalizing PAOM Df for Effectory contract")
    df_paom.rename(columns=header_list, inplace=True)

    return True, df_paom[common_PAOM_cols_l]

# This function will accept organization unit and based on that it will return business area and business subarea
def get_business_area_n_sub_area_by_org_unit(org_unit):
    try:
        return org_unit_mapping[org_unit]
    except KeyError: 
        return []

def refine_asis_dataframe(df_paom):    
    # Updating the 'departmentName' column for rows where the 'countryKey' is in the list of 
    # allowed countries by calling the method 'get_department_name_n_code' and assigning the returned value to the
    # 'departmentName' column.
    df_paom.loc[df_paom['countryKey'].isin (country_list), 'departmentName'] = df_paom.apply(lambda row: f"{row['departmentName']} ({row['departmentCode']})"
                                if pd.notna(row['departmentCode']) and pd.notna(row['departmentName'])
                                else (row['departmentName'] if pd.notna(row['departmentName']) else (f"({row['departmentCode']})" if pd.notna(row['departmentCode']) else '')), axis=1)
    logger.info(f"<---------Refining dataframe------->")
    # Process only Coworkers, no consultants.
    logger.info("Replacing empty empid with personid")
    df_paom.loc[(df_paom['employeeId'].isnull()) & (df_paom['personId'].notnull()) & (df_paom['emailId'].notnull() & (df_paom['countryKey']!='US')), 'employeeId'] = df_paom.personId
    df_paom = df_paom.query(
        "employmentType != 'CONSULTANT' & employeeId.notnull() & emailId.notnull()").copy()
    df_paom['employmentType'] = df_paom['employmentType'].apply(
        replace_values)
    df_paom['BusinessUnit'] = df_paom.businessUnitType + df_paom.businessUnit 
    df_paom = paom_raw_data_refinement(df_paom)
 
    if 'birthPlace' not in df_paom.columns:
        poam_drop_cols_l.remove("birthPlace")

    df_paom.drop(poam_drop_cols_l, axis=1, inplace=True)
    df_paom['costCentre'] = df_paom.costCentre
    df_paom['managerFlag'] = np.where(
        df_paom.managerFlag, '1', '0')
    df_paom['SubEmploymentGroupName'] = df_paom.employeeSubGroup
    df_paom.rename(columns=mapping_dict, inplace=True)
    df_paom['Business_Area'] = ''
    df_paom['BusinessUnitName'] = '<To be derived>'
    df_paom['SubDepartment'] = ''
    df_paom['ManagerFirstName'] = ''
    df_paom['ManagerLastName'] = ''
    df_paom['ManagerEmailAddress'] = ''
    df_paom['Age'] = ''
    df_paom['AgeGroup'] = ''
    df_paom['Tenure'] = ''
    df_paom['TenureGroup'] = ''
    df_paom['isCountryRetailManager'] = ''
    df_paom['CostCentreFull'] = ''
    df_paom.PreferedLanguage = df_paom.PreferedLanguage.str.upper()
    df_paom['CDSCreateDate'] = pd.to_datetime(
        df_paom.CDSCreateDate, errors='coerce').dt.strftime(
        paom_date_format_tb)
    df_paom['CDSCreateDate'] = df_paom['CDSCreateDate'].fillna(
        np.nan).replace([np.nan], [None])
    df_paom['DataSource'] = 'PAOM'
    df_paom = df_paom[common_PAOM_cols_l].astype(
        str).replace('nan', np.nan)
    logger.info("Changing the date format in line with iShare format")
    df_paom['BirthDate'] = pd.to_datetime(
        df_paom.BirthDate,
        format=paom_date_format,
        errors='coerce').dt.strftime(
        paom_date_format_tb)
    df_paom['StartDateInIngka'] = pd.to_datetime(
        df_paom.StartDateInIngka,
        format=paom_date_format,
        errors='coerce').dt.strftime(
        paom_date_format_tb)
    df_paom['StartDateInIngka'].fillna(df_paom.CDSCreateDate, inplace=True)
    df_paom['CostCentreFull'] = df_paom.CostCentre
    logger.info(
        f"Truncating costCenter and storing last 4 digits as legit cost center.")
    df_paom['CostCentre'] = df_paom.CostCentre.str[-4:]
    logger.info("Calculating contract type")
    df_paom['ContractType'] = df_paom.apply(
        calculate_contract_type, axis=1)
    # Fill missing StartDateInIngka field values using cdsCreateTimeStamp
    df_paom['StartDateInIngka'] = np.where(
        df_paom['StartDateInIngka'].values is None,
        df_paom['CDSCreateDate'],
        df_paom['StartDateInIngka'])
    logger.info(f"PersonAPI Data stats: {df_paom.shape} ")

    return df_paom

def austria_changes(df):
    df.loc[(df['Country'] == 'AT') & (df['BusinessUnitType'] == 'CDC') & (df['BusinessUnitCode'] == '452'), 'BusinessUnitName'] = 'DC Wels'
    df.loc[(df['Country'] == 'AT') & (df['BusinessUnitType'] == 'DT') & (df['BusinessUnitCode'] == '318'), 'BusinessUnitName'] = 'DC Wels'
    df.loc[(df['BusinessUnitType'] == 'CFF') & (df['BusinessUnitCode'] == '023'), 'BusinessUnitName'] = 'CFF - Customer Fulfilment'
    df.loc[(df['Country'] == 'AT') & (df['BusinessUnitName'] == 'Klagenfurt') & (df['BusinessUnitCode'] == '155'), 'BusinessUnitName'] = 'Österreich Süd'
    df.loc[(df['Country'] == 'AT') & (df['BusinessUnitName'] == 'Graz') & (df['BusinessUnitCode'] == '387'), 'BusinessUnitName'] = 'Österreich Süd'
    return df 

def us_changes(df):
    # for US chosenFirstName
    df.loc[df['Country'] == 'US','FirstName'] = df.loc[df['Country'] == 'US',
                                                    'chosenFirstName'].fillna(df['FirstName'])

    return df

def add_empty_columns_to_person_dataframe(df):
    df['BusinessArea'] = ''
    df['BusinessSubArea'] = ''
    df['Organisational_Level_01'] = ''
    df['Organisational_Level_02'] = ''
    df['Organisational_Level_03'] = ''
    df['Organisational_Level_04'] = ''
    df['Organisational_Level_05'] = ''
    df['Organisational_Level_06'] = ''
    df['Organisational_Level_07'] = ''
    df['Organisational_Level_08'] = ''
    df['Organisational_Level_09'] = ''
    df['Organisational_Level_10'] = ''
    df['Organisational_Level_11'] = ''
    df['Organisational_Level_12'] = ''
    df['Organisational_Level_13'] = ''
    df['Organisational_Level_14'] = ''
    df['Organisational_Level_15'] = ''
    df['Organisational_Level_16'] = ''
    df['Organisational_Level_17'] = ''
    df['US_PersonnelAreaCode'] = ''
    df['US_PersonnelAreaText'] = ''
    df['US_PersonnelSubAreaCode'] = ''
    df['US_PersonnelSubAreaText'] = ''
    df['US_DepartmentName'] = ''
    df['SE_IshareKey'] = ''
    df['SE_UnitName'] = ''
    df['SE_WorkareaName'] = ''
    df['CH_OrganisationalKey'] = ''
    df['CH_OrganisationalKeyText'] = ''
    df['AT_CostCentreGroup'] = ''
    df['ShoppingCenter_name'] = ''
    df['Record_Status'] = ''
    return df 

def replace_values(value):
    if value != 'CONSULTANT':
        return 'COWORKER'
    return value

def replace_nan_to_null(df):
    nan_values = [np.nan, 'NaN', 'nan', 'NaT','None']
    for col in df.columns:
        if df[col].dtype == 'object':  # if column is of object type
            df[col].replace(nan_values, '', inplace=True)
        else:
            df[col].replace(nan_values, None, inplace=True)
    return df
