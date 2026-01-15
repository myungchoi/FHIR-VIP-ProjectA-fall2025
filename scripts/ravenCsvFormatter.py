import pandas as pd
import numpy as np
from datetime import date
import os


# Source files
original_file_loc = '../data/Milwaukee_county_records - milwaukee_county_records.csv - actual drug deaths.csv'
raven_template_loc = '../data/Target-MDI-To-EDRS-Template.csv'

# Resulting csv output file
file_runtime = date.today().strftime("%Y-%m-%d")
output_file = f'MILWAUKEE_TO_RAVEN_{file_runtime}.csv'
output_file_one_row = f'MILWAUKEE_TO_RAVEN_{file_runtime}_ONE_ROW.csv'
output_path = '../results/'

RAVEN_MAP = {
    "BASEFHIRID": "CaseIdentifier",
    "SYSTEMID": None,
    "MDICASEID": "CaseNum",
    "EDRSCASEID": None,
    "FIRSTNAME": None,
    "MIDNAME": None,
    "LASTNAME": None,
    "AGE": "Age",
    "AGEUNIT": None,
    "RACE": "Race",
    "GENDER": "Sex",
    "ETHNICITY": None,
    "BIRTHDATE": None,
    "MRNNUMBER": None,
    "JOBTITLE": None,
    "INDUSTRY": None,
    "LANGUAGE": None,
    "MARITAL": None,
    "POSSIBLEID": None,
    "CAUSEA": "CauseA",
    "CAUSEB": "CauseB",
    "CAUSEC": None,
    "CAUSED": None,
    "OSCOND": "CauseOther",
    "MANNER": "Mode",
    "DISPMETHOD": None,
    "CHOWNINJURY": None,
    "DURATIONA": None,
    "DURATIONB": None,
    "DURATIONC": None,
    "DURACTIOND": None,
    "CASENOTES": None,
    "ATWORK": None,
    "JOBRELATED": None,
    "REPORTDATE": None,
    "REPORTTIME": None,
    "FOUNDDATE": None,
    "FOUNDTIME": None,
    "CDEATHDATE": "DeathDate",
    "EVENTDATE": "EventDate",
    "EVENTTIME": None,
    "PRNDATE": None,
    "PRNTIME": None,
    "EXAMDATE": None,
    "CINJDATE": None,
    "CINJTIME": None,
    "CINJDATEEARLY": None,
    "CINJDATELATE": None,
    "CDEATHESTABLISHEMENTMETHOD": None,
    "CIDATEFLAG": None,
    "CDEATHFLAG": None,
    "CDEATHTIME": None,
    "LKADATE": None,
    "LKATIME": None,
    "CASEYEAR": "death_year",
    "ATHOSPDATE": None,
    "ATHOSPTIME": None,
    "RESSTRET": None,
    "RESCITY": "DeathCity",
    "RESCOUNTY": None,
    "RESSTATE": "DeathState",
    "RESZIP": "DeathZip",
    "RESCOUNTRY": None,
    "DEATHLOCATION": "DeathAddr",
    "DEATHLOCATIONTYPE": None,
    "INJURYLOCATION": None,
    "FOUNDADDR_STREET": None,
    "FOUNDADDR_CITY": None,
    "FOUNDADDR_COUNTY": None,
    "FOUNDADDR_STATE": None,
    "FOUNDADDR_ZIP": None,
    "EVENTPLACE": None,
    "EVEENTADDR_STREET": None,
    "EVENTADDR_CITY": None,
    "EVENTADDR_COUNTY": None,
    "EVENT_ADDR_STATE": None,
    "EVENTADDR_ZIP": None,
    "PRNPLACE": None,
    "PRNSTREET": None,
    "PRNCITY": None,
    "PRNCOUNTY": None,
    "PRNSTATE": None,
    "PRNZIP": None,
    "DISP_PLACE": None,
    "DISP_STREET": None,
    "DISP_CITY": None,
    "DISP_STATE": None,
    "DISP_ZIP": None,
    "CINJPLACE": None,
    "CINJSTREET": None,
    "CINJCITY": None,
    "CINJCOUNTY": None,
    "CINJSTATE": None,
    "CINJZIP": None,
    "RESNAME": None,
    "LKAWHERE": None,
    "HOSPNAME": None,
    "SCENEADDR_STREET": None,
    "SCENEADDR_CITY": None,
    "SCENEADDR_COUNTY": None,
    "SCENEADDR_STATE": None,
    "SCENEADDR_ZIP": None,
    "CERTIFIER_NAME": None,
    "CERTIFIER_TYPE": None,
    "SURGERY": None,
    "SURGDATE": None,
    "SURGREASON": None,
    "HCPROVIDER": None,
    "AUTOPSYPERFORMED": None,
    "AUTOPSYRESULTSAVAILABLE": None,
    "AUTOPSY_OFFICENAME": None,
    "AUTOPSY_STREET": None,
    "AUTOPSY_CITY": None,
    "AUTOPSY_COUNTY": None,
    "AUTOPSY_STATE": None,
    "AUTOPSY_ZIP": None,
    "CUSTODY": None,
    "PREGNANT": None,
    "TOBACCO": None,
    "CAUTOPSY": None,
    "TRANSPORTATION": None,
    "MENAME": None,
    "MEPHONE": None,
    "MELICENSE": None,
    "ME_STREET": None,
    "ME_CITY": None,
    "ME_COUNTY": None,
    "ME_STATE": None,
    "ME_ZIP": None,
    "AUTOPUSED": None,
    "PRONOUNCERNAME": None,
    "CERTIFIER_IDENTIFIER": None,
    "CERTIFIER_IDENTIFIER_SYSTEM": None
}


# Method to transform CSV file into Raven MDI format based on specified mapping
def format_csv_to_raven(csv_file, raven_file, mapping, output_loc):
    """
        Maps columns from a source CSV into the format of a target (Raven MDI) CSV and saves the result.

        Parameters
        ----------
        csv_file : str or Path
            Path to the source CSV file containing raw data.
        raven_file : str or Path
            Path to the Raven-format CSV file that defines the desired output schema.
        mapping : dict
            A dictionary mapping Raven column names (keys) to source column names (values).
        output_loc : str or Path
            Path where the formatted CSV will be written.

        Returns
        -------
        None
            The function writes the mapped DataFrame to `output_loc` and does not return anything.
        """
    # Convert to pandas dataframes
    source_df = pd.read_csv(csv_file, dtype={'DeathZip': 'Int64', 'death_year': 'Int64'})
    raven_df = pd.read_csv(raven_file)

    # Map columns
    new_data = {}
    for target_col in raven_df.columns:
        source_col = mapping.get(target_col)
        if source_col and source_col in source_df.columns:
            new_data[target_col] = source_df[source_col]
        else:
            new_data[target_col] = None

    raven_df = pd.DataFrame(new_data)

    raven_df['CDEATHDATE'] = pd.to_datetime(raven_df['CDEATHDATE'], errors='coerce')
    raven_df['CDEATHTIME'] = raven_df['CDEATHDATE'].dt.strftime("%H:%M:%S")

    # Convert date columns to m/d/YYYY (importer expects this format)
    for col in ['CDEATHDATE', 'EVENTDATE']:
        raven_df[col] = pd.to_datetime(raven_df[col], errors='coerce').dt.strftime('%-m/%-d/%Y')

    # Adjust Age Data Type
    # - corrected. in MDI and VRDR FHIR IGs, they support years, months, and days for unit. We should not convert
    #   this unit. Year must be integer. And, if 7 months are there for example, then it should be 7 months,
    #   not 1 year.
    #raven_df['AGEUNIT'] = 'Years'
    raven_df['AGE_STR'] = raven_df['AGE']
    #cond1 = raven_df['AGE_STR'].str.contains('Year', na=False)
    #cond2 = raven_df['AGE_STR'].str.contains('Month', na=False)
    #cond3 = raven_df['AGE_STR'].str.contains('Day', na=False)

    #raven_df['AGE'] = np.select(
    #    [cond1, cond2, cond3],
    #    [
    #        (raven_df['AGE_STR'].str.split().str[0].astype(float)),
    #        (raven_df['AGE_STR'].str.split().str[0].astype(float) / 12).round(2),
    #        (raven_df['AGE_STR'].str.split().str[0].astype(float) / 365).round(2),
    #    ],
    #    default=raven_df['AGE_STR']
    #)
    raven_df['AGE'] = raven_df['AGE_STR'].str.split().str[0]
    raven_df['AGEUNIT'] = raven_df['AGE_STR'].str.split().str[1]

    raven_df['RESZIP']  = raven_df['RESZIP'].replace(to_replace = "\\.0+$",value = "", regex = True)

    # Export as csv
    raven_df.to_csv(output_loc, index=False)

    #Export one row
    # raven_df.head(1).to_csv(output_loc, index=False)


# ********* Main Driver *****************
try:
    os.makedirs(output_path, exist_ok=True)

    # Export full CSV
    format_csv_to_raven(original_file_loc, raven_template_loc, RAVEN_MAP, f'{output_path}{output_file}')

    #Export one row
    # format_csv_to_raven(original_file_loc, raven_template_loc, RAVEN_MAP, f'{output_path}{output_file_one_row}')

    print(f'File Created: {output_file}')
except Exception as e:
    print(f'File Creation Error: {e}')
