import csv
import os
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from concurrent.futures import ThreadPoolExecutor
import json
import pprint

def get_csv_row_count(filename="result.csv"):
    """Gets the number of data rows in a CSV file (excluding the header)."""
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        return 0
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            # Count rows, subtract 1 for the header
            row_count = sum(1 for row in reader) - 1
            return max(0, row_count)  # Ensure count is not negative
    except Exception as e:
        print(f"Could not read {filename}: {e}")
        return 0



def retrive_company_data(company_id):
    url = "https://www.difc.com/api/handleRequest"
    payload_data = {
        "slug": f"/CRM/public-register?recordId={company_id}",
        "method": "GET"
    }
    json_payload_string = json.dumps(payload_data)
    response = requests.post(url, data=json_payload_string)
    if response.status_code == 200:
        response_json = response.json().get("Data").get("DIFCData").get("PublicRegistry")[-1]
        data = {
            "registration_number": response_json.get("RegisteredNumber", "Not Found"),
            "company_name": " | ".join(name.get("Name", "Not Found") for name in response_json.get("EntityName", [])),
            "entity_status": response_json.get("EntityStatus", "Not Found"),
            "business_type": response_json.get("CompanyType", "Not Found"),
            "type_of_license": response_json.get("TypeOfLicense", "Not Found"),
            "legal_structure": response_json.get("TypeOfEntity", "Not Found"),
            "date_of_registration": response_json.get("DateOfIncorporation", "Not Found"),
            "date_of_dissolution": response_json.get("DateOfDissolution", "Not Found"),
            "trading_name": " | ".join(name.get("TradeName", "Not Found") for name in response_json.get("TradingName", [])),
            "dnfbp": response_json.get("DNFBP", "Not Found"),
            "data_protection_officer_appointed": response_json.get("AppointedDataProtectionOfficer", "Not found"),
            "telephone" : " | ".join(contact.get("telephoneno", "Not Found") for contact in response_json.get("Telephone", [])),
            "activities": " | ".join(acrivity.get("Activity", "Not Found") for acrivity in response_json.get("LicencedActivity", [])),
            "registered_office": " | ".join(office.get("OfficeAddress", "Not Found") for office in response_json.get("RegisteredOfficeAddress", [])),
            "financial_year_end": response_json.get("FinancialYearEnd", "Not Found"),
            "share_capital": " | ".join(share.get("AccountShareInfo", "Not Found") for share in response_json.get("AccountSharesInfo", [])),
            "directors": " | ".join(dirctor.get("DirectorName", "Not Fount") for dirctor in response_json.get("Director", [])),
            "shareholders": " | ".join(shareholder.get("ShareholderName", "Not Found") for shareholder in response_json.get("ShareHolder", [])),
            "auditor": " | ".join(auditor.get("NameofAuditorFirm", "Not Found") for auditor in response_json.get("Auditor", [])),
            "company_secretary": " | ".join(secretary.get("SecretaryName", "Not Found") for secretary in response_json.get("CompanySecretary", [])),
            "marketing_data": f"{response_json.get("MarketingFields", {}).get("Email", "Not Found")} | {response_json.get("MarketingFields", {}).get("Telephonenumber", "Not Found")}",
            "membership_stuff": str(response_json.get("Membershipstuff", "Not Found"))
        }
        return data
    else:
        raise Exception(f"Cannot get company data : {company_id}, url : {url}")
    

def batch_fetch_campany(company_ids):
    with ThreadPoolExecutor() as executor:
        result= list(executor.map(retrive_company_data, company_ids))
    return result



def write_to_csv(data, file_name="result.csv"):
    """Writes a list of dictionaries to a CSV file with a specific column order."""
    if not data:
        print("No data to write.")
        return

    # Define the exact order of columns for the CSV file.
    fieldnames = [
        "registration_number",
        "company_name",
        "entity_status",
        "business_type",
        "type_of_license",
        "legal_structure",
        "date_of_registration",
        "date_of_dissolution",
        "trading_name",
        "dnfbp",
        "data_protection_officer_appointed",
        "telephone",
        "activities",
        "registered_office",
        "financial_year_end",
        "share_capital",
        "directors",
        "shareholders",
        "auditor",
        "company_secretary",
        "marketing_data",
        "membership_stuff"
    ]

    # Check if the file is new or empty to decide whether to write the header.
    write_header = not os.path.exists(file_name) or os.path.getsize(file_name) == 0

    try:
        with open(file_name, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)

            if write_header:
                writer.writeheader()

            writer.writerows(data)
            print(f"Successfully wrote {len(data)} rows to {file_name}")
    except IOError as e:
        print(f"I/O error while writing to {file_name}: {e}")
