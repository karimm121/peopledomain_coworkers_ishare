# -------------------------------------------------------------------------------------------
# Change date       Author              Desc                                         Vesion
# 2024.03.07        Neeraj Sharma       Refactoring                                  1.0
# -------------------------------------------------------------------------------------------
 
import datetime as dt
import os, time
import math
import numpy as np
import pandas_gbq
from dotenv import load_dotenv, dotenv_values
import traceback
from pandas.io import gbq
import warnings
import pandas as pd
# from customLogger import getloggere
from loguru import logger
from vars import OFFBOARDING_LIST_VIEW,PAOM_TABLE,DATASET#,config
from utils import project_id, refine_asis_dataframe, austria_changes,add_empty_columns_to_person_dataframe,us_changes
from utils import time_decorator, get_country_retail_managers, get_managers, load_table_to_dataframe, write_db_table
from utils import paom_raw_data_refinement,get_tenures, get_age_group,set_age_and_tenures,get_age_group,set_master_data_new
from utils import get_offboarding_list_data,get_business_area_n_sub_area_by_org_unit, replace_nan_to_null
# from exceptions import credetailsNotFound,invokingPAOMAPIFailed, emptyDataFrame
from personapi import PersonAPI 
warnings.filterwarnings("ignore")

# logger = getlogger()
@time_decorator
def main(request):
    try:
        logger.info("Pulling coworkers data from Person API, starts...")
        p = PersonAPI()
        df_paom_data = p.call_person_api()
        df_paom_data = refine_asis_dataframe(df_paom_data)
    except Exception as ex:
        logger.error(f"Exception occurred while pulling data from Person API: {ex}")
        return "Failure", 500

    df_paom_data = us_changes(df_paom_data)

    logger.info(f"Getting manager details and its flag")
    status, df_paom_with_MGR = get_managers(df_paom_data)

    logger.info(f"Getting Country Retail manager details")
    status, df_paom_with_CRM = get_country_retail_managers(
        df_paom_with_MGR)  # --original

    logger.info("Setting age and tenures.")
    status, df_paom_at = set_age_and_tenures(df_paom_with_CRM)

    logger.info("Setting master data ie businessunitname etc")
    status, df_paom_final = set_master_data_new(df_paom_at)

    logger.info(
        f"Missing = Final [{df_paom_final.shape[0]} - Actual {df_paom_data.shape[0]} = {df_paom_final.shape[0] - df_paom_data.shape[0]}]")

    # Remove duplicates - to be thoroughly verified if best record is kept.
    df_paom_final = df_paom_final[~df_paom_final.EmployeeId.duplicated()]

    # Verify if any dups left - usually no record should be shown
    df_paom_final[df_paom_final['EmployeeId'].duplicated() ==
                    True].sort_values('EmployeeId')

    df = df_paom_final.copy()  # Temp code, change below with df

    del df_paom_final, df_paom_data, df_paom_with_MGR, df_paom_at, df_paom_with_CRM


    logger.info("Calculating business area and business sub area")
    
    # adding new column with default values
    df = add_empty_columns_to_person_dataframe(df)

    col = df.columns.values.tolist()
    new_df_list = []

    companycode_index = col.index('CompanyCode')
    businessUnitType_index = col.index('BusinessUnitType')
    function_index = col.index('Function')
    orgUnitAbbreviation_index = col.index('orgUnitAbbreviation')
    organizationUnit_index = col.index('OrganizationUnit')
    businessarea_index = col.index('BusinessArea')
    businesssubarea_index = col.index('BusinessSubArea')
    for df_row in df.values:
        try:
            # calculate using company code
            if df_row[companycode_index] == '2303':
                df_row[businessarea_index] = 'Ingka AB'
                df_row[businesssubarea_index] = 'Ingka AB'
            else:
                df_row[businessarea_index] = 'Retail'
                df_row[businesssubarea_index] = ''

            # calculate using orgUnitAbbreviation
            if df_row[orgUnitAbbreviation_index] == 'C_BS':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Business Steering'
            elif df_row[orgUnitAbbreviation_index] == 'C_BS_BD':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Business Steering'
            elif df_row[orgUnitAbbreviation_index] == 'C_BS_GB':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Business Steering'
            elif df_row[orgUnitAbbreviation_index] == 'C_BS_MS_E2E':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Business Steering'
            elif df_row[orgUnitAbbreviation_index] == 'C_BS_RO':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Business Steering'
            elif df_row[orgUnitAbbreviation_index] == 'C_BS_VC&I':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Business Steering'
            elif df_row[orgUnitAbbreviation_index] == 'C_COMM':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'Group Communication'
            elif df_row[orgUnitAbbreviation_index] == 'C_DIGI':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'Group Digital'
            elif df_row[orgUnitAbbreviation_index] == 'C_DIGITMA':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'Group Digital'
            elif df_row[orgUnitAbbreviation_index] == 'C_FIN':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Group Finance'
            elif df_row[orgUnitAbbreviation_index] == 'C_GO':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Global Operations'
            elif df_row[orgUnitAbbreviation_index] == 'C_L&G':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Group Legal & Governance'
            elif df_row[orgUnitAbbreviation_index] == 'C_MGMT':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'Ingka Management'
            elif df_row[orgUnitAbbreviation_index] == 'C_P&C':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'Group People & Culture'
            elif df_row[orgUnitAbbreviation_index] == 'C_PROC':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Group Procurement'
            elif df_row[orgUnitAbbreviation_index] == 'C_R&C':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Business Risk & Compliance'
            elif df_row[orgUnitAbbreviation_index] == 'C_REAL':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Group Real Estate'
            elif df_row[orgUnitAbbreviation_index] == 'C_SD&I':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'Group Strategy, Development, and Innovation'
            elif df_row[orgUnitAbbreviation_index] == 'C_SUST':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Group Sustainability'
            elif df_row[orgUnitAbbreviation_index] == 'IC_SUST':
                df_row[businessarea_index] = 'Ingka Centres'
                df_row[businesssubarea_index] = 'IC - Sustainability'
            elif df_row[orgUnitAbbreviation_index] == 'IC_C&D':
                df_row[businessarea_index] = 'Ingka Centres'
                df_row[businesssubarea_index] = 'IC - Commercial & Digital'
            elif df_row[orgUnitAbbreviation_index] == 'IC_CENTRES':
                df_row[businessarea_index] = 'Ingka Centres'
                df_row[businesssubarea_index] = 'IC - Management'
            elif df_row[orgUnitAbbreviation_index] == 'IC_CFO':
                df_row[businessarea_index] = 'Ingka Centres'
                df_row[businesssubarea_index] = 'IC - CFO'
            elif df_row[orgUnitAbbreviation_index] == 'IC_COMMS':
                df_row[businessarea_index] = 'Ingka Centres'
                df_row[businesssubarea_index] = 'IC - Communications'
            elif df_row[orgUnitAbbreviation_index] == 'IC_E&D':
                df_row[businessarea_index] = 'Ingka Centres'
                df_row[businesssubarea_index] = 'IC - Expansion & Development'
            elif df_row[orgUnitAbbreviation_index] == 'IC_MP':
                df_row[businessarea_index] = 'Ingka Centres'
                df_row[businesssubarea_index] = 'IC - Operations Meeting Place'
            elif df_row[orgUnitAbbreviation_index] == 'IC_OPS':
                df_row[businessarea_index] = 'Ingka Centres'
                df_row[businesssubarea_index] = 'IC - Operations Service Office'
            elif df_row[orgUnitAbbreviation_index] == 'IC_P&C':
                df_row[businessarea_index] = 'Ingka Centres'
                df_row[businesssubarea_index] = 'IC - People & Culture'
            elif df_row[orgUnitAbbreviation_index] == 'NL_AMSHULT':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Global Support'
            elif df_row[orgUnitAbbreviation_index] == 'NL_LEIDEN':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Global Support'
            elif df_row[orgUnitAbbreviation_index] == 'SE_HUBHULT':
                df_row[businessarea_index] = 'Group Functions'
                df_row[businesssubarea_index] = 'CFO - Global Support'
            elif df_row[orgUnitAbbreviation_index] == 'R_CFF':
                df_row[businessarea_index] = 'Retail Support Functions'
                df_row[businesssubarea_index] = 'Customer Fulfilment'
            elif df_row[orgUnitAbbreviation_index] == 'R_EXP':
                df_row[businessarea_index] = 'Retail Support Functions'
                df_row[businesssubarea_index] = 'Expansion'
            elif df_row[orgUnitAbbreviation_index] == 'R_F&CS':
                df_row[businessarea_index] = 'Retail Support Functions'
                df_row[businesssubarea_index] = 'Fulfilment & Core Services'
            elif df_row[orgUnitAbbreviation_index] == 'R_OMP':
                df_row[businessarea_index] = 'Retail Support Functions'
                df_row[businesssubarea_index] = 'Omni Meeting Points'
            elif df_row[orgUnitAbbreviation_index] == 'R_G&M':
                df_row[businessarea_index] = 'Retail Support Functions'
                df_row[businesssubarea_index] = 'Growth & Marketing'
            elif df_row[orgUnitAbbreviation_index] == 'R_MS':
                df_row[businessarea_index] = 'Retail Support Functions'
                df_row[businesssubarea_index] = 'Market Support'
            elif df_row[orgUnitAbbreviation_index] == 'R_OTHER':
                df_row[businessarea_index] = 'Retail Support Functions'
                df_row[businesssubarea_index] = 'Other'
            elif df_row[orgUnitAbbreviation_index] == 'R_RETAIL':
                df_row[businessarea_index] = 'Retail Support Functions'
                df_row[businesssubarea_index] = 'Retail Management'
            elif df_row[orgUnitAbbreviation_index] == 'C_INVESTMENT':
                df_row[businessarea_index] = 'Ingka Investments'
                df_row[businesssubarea_index] = 'Ingka Investments'
            elif df_row[orgUnitAbbreviation_index] == 'EE_1083':
                df_row[businessarea_index] = 'Ingka Investments'
                df_row[businesssubarea_index] = 'Ingka Investments'
            elif df_row[orgUnitAbbreviation_index] == 'LT_1086':
                df_row[businessarea_index] = 'Ingka Investments'
                df_row[businesssubarea_index] = 'Ingka Investments'
            elif df_row[orgUnitAbbreviation_index] == 'LV_1085':
                df_row[businessarea_index] = 'Ingka Investments'
                df_row[businesssubarea_index] = 'Ingka Investments'
            elif df_row[orgUnitAbbreviation_index] == 'RO_1079':
                df_row[businessarea_index] = 'Ingka Investments'
                df_row[businesssubarea_index] = 'Ingka Investments'
            elif df_row[orgUnitAbbreviation_index] == 'RO_1082':
                df_row[businessarea_index] = 'Ingka Investments'
                df_row[businesssubarea_index] = 'Ingka Investments'
            elif df_row[orgUnitAbbreviation_index] == 'RO_1084':
                df_row[businessarea_index] = 'Ingka Investments'
                df_row[businesssubarea_index] = 'Ingka Investments'

            # calculate business area and business subarea by organization unit
            business_area_subarea = get_business_area_n_sub_area_by_org_unit(df_row[organizationUnit_index])                
            if len(business_area_subarea) > 0:
                df_row[businessarea_index] = business_area_subarea[0]
                df_row[businesssubarea_index] = business_area_subarea[1]

            # calculate using BusinessUnitType for Retail business areas
            if df_row[businessarea_index] == 'Retail':
                if df_row[businessUnitType_index] == 'STO':
                    df_row[businesssubarea_index] = 'Store'
                elif df_row[businessUnitType_index] == 'CSC':
                    df_row[businesssubarea_index] = 'Customer Service Centre'
                elif df_row[businessUnitType_index] == 'SO':
                    df_row[businesssubarea_index] = 'Service Office'
                elif df_row[businessUnitType_index] == 'ITO':
                    df_row[businesssubarea_index] = 'IT Office'
                elif df_row[businessUnitType_index] == 'DT':
                    df_row[businesssubarea_index] = 'Distribution Centre'
                elif df_row[businessUnitType_index] == 'DT3':
                    df_row[businesssubarea_index] = 'Distribution Centre'
                elif df_row[businessUnitType_index] == 'CDC':
                    df_row[businesssubarea_index] = 'Customer Distribution Centre'
                elif df_row[businessUnitType_index] == 'BSU':
                    df_row[businesssubarea_index] = 'Business Support Unit'
                elif df_row[businessUnitType_index] == 'PSU':
                    df_row[businesssubarea_index] = 'Property Support Unit'
                elif df_row[businessUnitType_index] == 'SHC':
                    df_row[businesssubarea_index] = 'Shopping Centre'
                elif df_row[businessUnitType_index] == 'BSC':
                    df_row[businesssubarea_index] = 'Business Service Centre'
                elif df_row[businessUnitType_index] == 'DSU':
                    df_row[businesssubarea_index] = 'Distribution Support Unit'
                elif df_row[businessUnitType_index] == 'IMS':
                    df_row[businesssubarea_index] = 'IMS Office'
                elif df_row[businessUnitType_index] == 'PCU':
                    df_row[businesssubarea_index] = 'Property Cluster Unit'
                elif df_row[businessUnitType_index] == 'CFF':
                    df_row[businesssubarea_index] = 'Distribution Support Unit'
                elif df_row[businessUnitType_index] == 'DSO':
                    df_row[businesssubarea_index] = 'Distribution Service Office'
                elif df_row[businessUnitType_index] == 'OFB':
                    df_row[businesssubarea_index] = 'Office Building'
                elif df_row[businessUnitType_index] == 'TSO':
                    df_row[businesssubarea_index] = 'Trading Service Office'
                elif df_row[businessUnitType_index] == 'FR7':
                    df_row[businesssubarea_index] = 'Franchisor'
                elif df_row[businessUnitType_index] == 'FR':
                    df_row[businesssubarea_index] = 'Franchisor'
                elif df_row[businessUnitType_index] == 'SUP':
                    df_row[businesssubarea_index] = 'Supplier'
                elif df_row[businessUnitType_index] == 'CSO':
                    df_row[businesssubarea_index] = 'Centres Service Office'
                elif df_row[businessUnitType_index] == 'ORB':
                    df_row[businesssubarea_index] = 'Office and Residential Building'
                elif df_row[businessUnitType_index] == 'DCC':
                    df_row[businesssubarea_index] = 'Distribution Centre Components'
                elif df_row[businessUnitType_index] == 'AIR':
                    df_row[businesssubarea_index] = 'Airport'
                elif df_row[businessUnitType_index] == 'CAR':
                    df_row[businesssubarea_index] = 'Carrier'
                elif df_row[businessUnitType_index] == 'CMP':
                    df_row[businesssubarea_index] = 'Customer Meeting Point'
                elif df_row[businessUnitType_index] == 'COM':
                    df_row[businesssubarea_index] = 'Company'
                elif df_row[businessUnitType_index] == 'CP':
                    df_row[businesssubarea_index] = 'Consolidation Point'
                elif df_row[businessUnitType_index] == 'DTI':
                    df_row[businesssubarea_index] = 'Distribution Centre IMS'
                elif df_row[businessUnitType_index] == 'EQL':
                    df_row[businesssubarea_index] = 'Equipment Re-loading'
                elif df_row[businessUnitType_index] == 'ESP':
                    df_row[businesssubarea_index] = 'External Service Provider'
                elif df_row[businessUnitType_index] == 'FCT':
                    df_row[businesssubarea_index] = 'Factory'
                elif df_row[businessUnitType_index] == 'FDC':
                    df_row[businesssubarea_index] = 'Food Distribution Centre'
                elif df_row[businessUnitType_index] == 'FOR':
                    df_row[businesssubarea_index] = 'Forestry'
                elif df_row[businessUnitType_index] == 'FSU':
                    df_row[businesssubarea_index] = 'Food Supplier'
                elif df_row[businessUnitType_index] == 'HOT':
                    df_row[businesssubarea_index] = 'Hotel'
                elif df_row[businessUnitType_index] == 'ICS':
                    df_row[businesssubarea_index] = 'IKEA Component Supplier'
                elif df_row[businessUnitType_index] == 'IMT':
                    df_row[businesssubarea_index] = 'Intermodal Terminal'
                elif df_row[businessUnitType_index] == 'ISV':
                    df_row[businesssubarea_index] = 'Indirect Material and Service Vendor'
                elif df_row[businessUnitType_index] == 'ITP':
                    df_row[businesssubarea_index] = 'Internal Transit Point'
                elif df_row[businessUnitType_index] == 'LAB':
                    df_row[businesssubarea_index] = 'Laboratory'
                elif df_row[businessUnitType_index] == 'LND':
                    df_row[businesssubarea_index] = 'Land'
                elif df_row[businessUnitType_index] == 'LSC':
                    df_row[businesssubarea_index] = 'Local Service Centre'
                elif df_row[businessUnitType_index] == 'MUS':
                    df_row[businesssubarea_index] = 'Museum'
                elif df_row[businessUnitType_index] == 'PPP':
                    df_row[businesssubarea_index] = 'Pallet Purchase Point'
                elif df_row[businessUnitType_index] == 'PPT':
                    df_row[businesssubarea_index] = 'Pallet Platform'
                elif df_row[businessUnitType_index] == 'PRC':
                    df_row[businesssubarea_index] = 'Pallet Recycling Point'
                elif df_row[businessUnitType_index] == 'PRP':
                    df_row[businesssubarea_index] = 'Pallet Repair Point'
                elif df_row[businessUnitType_index] == 'PRT':
                    df_row[businesssubarea_index] = 'Port'
                elif df_row[businessUnitType_index] == 'PSP':
                    df_row[businesssubarea_index] = 'Pallet Sales Point'
                elif df_row[businessUnitType_index] == 'QSC':
                    df_row[businesssubarea_index] = 'Quality Support Centre'
                elif df_row[businessUnitType_index] == 'REI':
                    df_row[businesssubarea_index] = 'Real Estate Investment'
                elif df_row[businessUnitType_index] == 'RGR':
                    df_row[businesssubarea_index] = 'Retail Goods in Transit Receiver'
                elif df_row[businessUnitType_index] == 'RMS':
                    df_row[businesssubarea_index] = 'Raw Material Supplier'
                elif df_row[businessUnitType_index] == 'RUP':
                    df_row[businesssubarea_index] = 'Responsible Unit for Development/Maintain Products'
                elif df_row[businessUnitType_index] == 'SCP':
                    df_row[businesssubarea_index] = 'Pallet Scrapping Account'
                elif df_row[businessUnitType_index] == 'SSC':
                    df_row[businesssubarea_index] = 'Shared Service Centre'
                elif df_row[businessUnitType_index] == 'SUP':
                    df_row[businesssubarea_index] = 'Supplier'
                elif df_row[businessUnitType_index] == 'TA':
                    df_row[businesssubarea_index] = 'Trading Agent'
                elif df_row[businessUnitType_index] == 'WDF':
                    df_row[businesssubarea_index] = 'Windfarm'
                else:
                    df_row[businesssubarea_index] = 'Check Business Unit Type'

            # calculate for Board Members using Function
            if df_row[function_index] == 'Board Members':
                df_row[businessarea_index] = 'Ingka Board'
                df_row[businesssubarea_index] = 'Ingka Board'

            new_df_list.append(df_row)
        except Exception as ex:
            logger.error(f"Exception occured: {ex}")
            return "Failure", 500


    df = pd.DataFrame(new_df_list, columns=col)
    logger.info(
        "completed calculating business area and business sub area")
    new_df_list = ''

    # updating BusinessUnitName in required format
    df['BusinessUnitName'] = df['BusinessUnitName'].str.lower().str.removeprefix("ikea").str.strip(
    ).str.title().str.replace('Ikea', 'IKEA', regex=False).str.replace(r'Ii\b', 'II', regex=True)

    df['Gender'] = df['Gender'].str.title()
    df.replace({'Gender': {'Unknown': 'Undeclared',
                'Non-Binary': 'Nonbinary'}}, inplace=True)

    # [ Cuuntry Specific Changes]
    # For AT
    df = austria_changes(df)
    
    #JobCode 40001364 (if the CostCentre is CC 6126 or CC 6606) needs to be mapped AT_CostCentreGroup to Trainees & Apprentices AT
    df.loc[(df['JobCode'] == '40001364') & ((df['CostCentre'] == '6126') | (df['CostCentre'] == '6606')), 'AT_CostCentreGroup'] = 'Trainees & Apprentices AT'

    df['EmailAddress'] = df['EmailAddress'].str.lower()
    df_offboarding = get_offboarding_list_data(OFFBOARDING_LIST_VIEW)

    merged_df = df_offboarding.merge(df, on='EmailAddress', how='right').astype(str)

    # Adding IKEA_Global to faciliate Effectory to calculate on Global IKEA level scores
    merged_df['IKEA_Global'] = 'IKEA Global'
    merged_df = replace_nan_to_null(merged_df)

    logger.info(
        f"Saving PAOM Refined Data to table: {PAOM_TABLE} with shape: df info , {df.shape} ")

    status = write_db_table(
        bq_dataset=DATASET,
        bq_tname=PAOM_TABLE,
        dataframe=merged_df  # Processed # with Offboarding changes
    )

    logger.info("Program ended Successfully!")
    return "Success!"

# temporary