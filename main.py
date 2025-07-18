# import packages
import math
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import json
from helper import get_csv_row_count, batch_fetch_campany, write_to_csv
from concurrent.futures import ThreadPoolExecutor
from colorama import Fore, Back, Style


url = 'https://www.difc.com/api/handleRequest'

# Define the headers
headers = {
    'sec-ch-ua-platform': '"macOS"',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'Content-Type': 'text/plain;charset=UTF-8',
    'sec-ch-ua-mobile': '?0',
    'Accept': '*/*',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'host': 'www.difc.com',
}

# Fetch the number of existing rows to use as the starting offset
offset = get_csv_row_count()

print(Fore.GREEN,f"Found {offset} existing rows in result.csv. Starting from there.")
current_page = int(offset/10)

till_page_number = int(input(f"how many page you want to scrap from page {current_page} : "))

for i in range(till_page_number):
    payload_data = {
        "name": "",
        "licenseType": "",
        "licenseNo": "",
        "status": "",
        "offset": offset,
        "slug": "/CRM/public-register",
        "method": "POST"
    }
    json_payload_string = json.dumps(payload_data)

    response = requests.post(url, headers=headers, data=json_payload_string)
    if response.status_code != 200:
        print("Error while loading the page")
        print("Stopped when offset : {offset}")
        break
    
    try:
        company_list = response.json().get("Data").get("companyList")
    except Exception as e:
        print("Exception :", e)
        print("Stopped when offset : {offset}")
        break
    company_ids = [comp.get("Id") for comp in company_list]
    
    print(f"Extracted for {offset} offset, page : {current_page+i}")
    print("Batch fetching: ",company_ids)
   
    # go through company_ids and retrive data
    # batch processing, batch 10
    companies_data = batch_fetch_campany(company_ids)

    # write companie data to resutl.
    write_to_csv(companies_data)
    print(f"Written : {(current_page*10) + ((i+1)*10)} Records : Success")

    # increase offset to 10 to fetch next page
    offset += 10