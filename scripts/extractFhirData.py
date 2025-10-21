import requests
from datetime import date
import pandas as pd


file_runtime = date.today().strftime("%Y-%m-%d")
output_file = f'tableau_analysis_csv_{file_runtime}.csv'
output_path = '../results/'

AUTH = ("client","secret")

def extract_patient_resource(currDict: dict, patient_resource: dict):
    currDict["Gender"] = patient_resource.get("gender")
    addr0 = (patient_resource.get("address") or [{}])[0]
    currDict["Patient Zip Code"] = addr0.get("postalCode")

def get_loinc_code(obs_resource: dict):
    codings = (obs_resource.get("code", {}).get("coding") or [])

    for c in codings:
        if c.get("system") == "http://loinc.org" and "code" in c:
            return c["code"]
    return None

def extract_observation_resource(currDict: dict, observation_resource: dict):
    code = get_loinc_code(observation_resource)
    if (code == "81956-5"):
        val = observation_resource.get("valueDateTime")
        currDict["Death Year"] = val[:4] if val else None
    elif (code == "69453-9"):
        val = observation_resource.get("valueCodeableConcept")
        currDict["Cause of Death Part 1"] = val.get("text") if val else None
    elif (code == "69449-7"):
        vcc = observation_resource.get("valueCodeableConcept", {})
        coding0 = (vcc.get("coding") or [{}])[0]
        currDict["Manner of Death"] = coding0.get("display")


# Main Driver
try:
    response = requests.get('http://localhost:8080/mdi-fhir-server/fhir/Bundle', auth=AUTH, timeout=10)
    response.raise_for_status()
except Exception as e:
    print(f"API GET Error: {e}")
    raise

currentPageData = response.json()
rows = []
page = 1
while currentPageData:
    entries = currentPageData.get("entry",[])
    if not entries:
        print(f"[Page {page}] No entries found, stopping")
        break

    for entry in currentPageData["entry"]:
        currDict = {}
        for bundle in entry["resource"]["entry"]:
                if (bundle["resource"]["resourceType"] == "Patient"):
                    extract_patient_resource(currDict, bundle["resource"])
                elif (bundle["resource"]["resourceType"] == "Observation"):
                    extract_observation_resource(currDict, bundle["resource"])
        rows.append(currDict)
    print(f"[Page {page}] Finished  --> Moving on to Next Page")
    nextUrl = next((link["url"] for link in currentPageData.get("link", []) if link["relation"] == "next"), None)
    if not nextUrl:
        print("[INFO] No Next page link found -- Script Complete.")
        break
    page+=1
    try:    
        response = requests.get(nextUrl, auth=AUTH, timeout=10)
        response.raise_for_status()
        currentPageData = response.json()
    except Exception as e:
        print(f"Paging GET Error: {e}")
        break

extracted_data = pd.DataFrame(rows)
extracted_data.to_csv(f'{output_path}{output_file}', index=False)
print(f"Done --> {output_path}{output_file}")
