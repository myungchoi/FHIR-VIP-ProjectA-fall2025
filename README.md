# Raven FHIR MDI Toolkit

## Introduction

A compact toolkit to transform CSV death investigation records into MDI-FHIR resources and also extract data from a Raven FHIR Server for analysis. This is best used alongside the Raven FHIR Server ([raven-fhir-server](https://github.com/MortalityReporting/raven-fhir-server)) and the Raven Import and Submit API ([raven-import-and-submit-api](https://github.com/MortalityReporting/raven-import-and-submit-api)).

Key features
- Convert CSV death investigation records to Raven MDI CSV (date normalization, age standardization, schema-aligned columns)
- Split and upload CSV chunks to the Import API ([raven-import-and-submit-api](https://github.com/MortalityReporting/raven-import-and-submit-api)) → persists to Raven FHIR Server ([raven-fhir-server](https://github.com/MortalityReporting/raven-fhir-server))
- Extract analysis fields from FHIR into a CSV for BI tools

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
  - [Generate Raven MDI CSV (`scripts/ravenCsvFormatter.py`)](#generate-raven-mdi-csv-scriptsravencsvformatterpy)
  - [Split and Upload CSV (`scripts/splitAndUpload.py`)](#split-and-upload-csv-scriptssplitanduploadpy)
  - [Extract FHIR Data for Analysis (`scripts/extractFhirData.py`)](#extract-fhir-data-for-analysis-scriptsextractfhirdatapy)
- [Project Overview](#project-overview)

## Prerequisites

Before running any scripts in this repository, ensure the following Raven services are installed and running:

- Raven FHIR Server: see setup instructions and Docker usage in the official repository: [MortalityReporting/raven-fhir-server](https://github.com/MortalityReporting/raven-fhir-server)
- Raven Import and Submit API: install and run the CSV import/upload service: [MortalityReporting/raven-import-and-submit-api](https://github.com/MortalityReporting/raven-import-and-submit-api)

These components provide the FHIR endpoint (e.g., `http://localhost:8080/mdi-fhir-server/fhir`) and the CSV upload endpoint (e.g., `http://localhost/raven-import-and-submit-api/upload-csv-file`) that the scripts in this repo expect.

## Installation

Use Python 3.10+ and install dependencies (virtual environment recommended):

```
# python -m venv .venv
# source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Generate Raven MDI CSV (`scripts/ravenCsvFormatter.py`)

Purpose
- Map your county CSV into the Raven MDI Import Template and emit a cleaned, schema-conformant CSV.

Edit these lines to point to your data/template and output location
```
# Source files
original_file_loc = '../data/<your_county_source>.csv'
raven_template_loc = '../data/Target-MDI-To-EDRS-Template.csv'

# Resulting csv output file
output_path = '../results/'
```

Adjust column mapping to match your source headers
- Update values in `RAVEN_MAP` to reference your source column names when available; leave `None` where not applicable.
```
RAVEN_MAP = {
    "BASEFHIRID": "CaseIdentifier",
    "SYSTEMID": None,
    "MDICASEID": "CaseNum",
    "EDRSCASEID": None,
    "FIRSTNAME": None,
    ...
}
```
- Only columns present in the Raven template are emitted; missing columns are filled with empty values and will not be mapped when importing to the Raven FHIR Server.

Transformations applied
- Dates in `CDEATHDATE` and `EVENTDATE` are coerced and formatted as m/d/YYYY; `CDEATHTIME` is derived from `CDEATHDATE` when available.
- Age strings like “N Years/Months/Days” are normalized to years and `AGEUNIT` is set to `Years`.

Run
```
python scripts/ravenCsvFormatter.py
```

Output
- `results/<raven_output_prefix>_YYYY-MM-DD.csv` (e.g., `results/COUNTY_TO_RAVEN_YYYY-MM-DD.csv`)

Current sample mapping
- The provided `RAVEN_MAP` is configured for the included Milwaukee County sample file and maps fields such as `Age`, `Sex`, `DeathDate`, `EventDate`, `DeathCity/State/Zip/Addr`, and cause/manner fields into Raven columns.

### Split and Upload CSV (`scripts/splitAndUpload.py`)

Purpose
- Split a large Raven MDI CSV into manageable chunk files and upload each chunk to the Raven Import and Submit API, which forwards the records into the Raven FHIR Server. Includes retry with exponential backoff.

Edit these lines for your environment or leave as is for the default local configuration.
```
URL = 'http://<host>/raven-import-and-submit-api/upload-csv-file'
AUTH = ('<client>', '<secret>')
TYPE_FIELD = { 'type': 'mdi' }

SOURCE_CSV = Path('../results/<your_raven_file>.csv')
OUT_DIR = Path('../results/split_upload_chunks')

CHUNK_SIZE = 400       # adjust based on reliability/performance
MAX_RETRIES = 3
RETRY_BACKOFF = 2.0    # exponential backoff base
```

Run
```
python scripts/splitAndUpload.py
```

Behavior
- Writes chunk files to `results/split_upload_chunks/` and removes them after success or cleanup.
- Sends each chunk to the Import API; the Import API processes and persists records to the Raven FHIR Server.
- Stops on first failed upload, reports progress, and cleans up temporary chunk files.
 - You should see entries in your fhir server: if local, open `http://localhost:8080/mdi-fhir-server/` and browse Patients/Composition/Bundle resources; or replace host accordingly (e.g., `http://<host>:8080/mdi-fhir-server/`).

### Extract FHIR Data for Analysis (`scripts/extractFhirData.py`)

Purpose
- Page through the Raven FHIR Server Bundle endpoint and extract fields for analysis (gender, ZIP, death year, cause, manner) into a CSV (e.g., for Tableau). 
- This script works with any data already present in the FHIR server, regardless of how the records were ingested.

Edit these lines to match your environment
```
# Output location and filename
output_path = '../results/'
output_file = 'tableau_analysis_csv_<tag>_YYYY-MM-DD.csv'  # or any name you prefer

# Authentication for FHIR server
AUTH = ('<client>', '<secret>')

# FHIR endpoint and network settings (inline in the first request)
requests.get('http://<host>:8080/mdi-fhir-server/fhir/Bundle', auth=AUTH, timeout=10)
# replace host/port if not local
```

Advanced customization
- Current fields extracted: Gender, Patient Zip Code, Death Year (LOINC 81956-5), Cause of Death Part 1 (LOINC 69453-9), Manner of Death (LOINC 69449-7).
- Add more Patient fields: extend `extract_patient_resource` to pull additional attributes present on Patient.
```
def extract_patient_resource(currDict: dict, patient_resource: dict):
    currDict["Gender"] = patient_resource.get("gender")
    addr0 = (patient_resource.get("address") or [{}])[0]
    currDict["Patient Zip Code"] = addr0.get("postalCode")
    # Example additions:
    currDict["Patient City"] = addr0.get("city")
    currDict["Patient State"] = addr0.get("state")
    currDict["Birth Date"] = patient_resource.get("birthDate")
```
- Add more Observation-derived fields: add LOINC cases in `extract_observation_resource`.
```
def extract_observation_resource(currDict: dict, observation_resource: dict):
    code = get_loinc_code(observation_resource)
    if code == "81956-5":
        val = observation_resource.get("valueDateTime")
        currDict["Death Year"] = val[:4] if val else None
    elif code == "69453-9":
        val = observation_resource.get("valueCodeableConcept")
        currDict["Cause of Death Part 1"] = val.get("text") if val else None
    elif code == "69449-7":
        vcc = observation_resource.get("valueCodeableConcept", {})
        coding0 = (vcc.get("coding") or [{}])[0]
        currDict["Manner of Death"] = coding0.get("display")
    # Example: add ICD-10 underlying cause if provided via LOINC/CodeableConcept
    elif code == "XXXXX-X":
        vcc = observation_resource.get("valueCodeableConcept", {})
        currDict["Underlying Cause (ICD-10)"] = (vcc.get("text") or "")
```
- Update LOINC mapping: if your server uses different codes, replace the code strings above with your codes.

Output
- `results/tableau_analysis_csv_YYYY-MM-DD.csv`

Run
```
python scripts/extractFhirData.py
```

Notes
- The extractor looks for LOINC codes 81956-5 (Death Year), 69453-9 (Cause Part 1), 69449-7 (Manner). Update `get_loinc_code`/`extract_observation_resource` if your server differs.

## Project Overview

This repository supports the FHIR for Medical Death Investigation (MDI) initiative. The goal is to streamline transformation of death investigation records into FHIR compliant formats to enable consistent, shareable, and analyzable health data across systems.

Key components include
- Column mapping and standardization from heterogeneous county CSVs to the Raven import schema using Python scripts.
- Data cleaning and formatting for key fields such as dates, identifiers, and event codes to ensure schema compliance.
- Chunking and uploading large CSV datasets to the Raven Import API for scalable ingestion.
- Extracting and exporting analysis-ready CSVs for further analysis and visualization (e.g., Tableau).

End-to-end workflow example:

Source CSV in `data/` → 2) Raven MDI CSV in `results/` → 3) Split and upload to Raven Import API → 4) Extract selected fields from FHIR server to an analysis CSV.
