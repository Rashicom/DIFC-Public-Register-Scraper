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
from helper import get_csv_row_count, batch_fetch_campany, write_to_csv, batch_fetch_company_ids
from concurrent.futures import ThreadPoolExecutor
from colorama import Fore, Back, Style


url = 'https://www.difc.com/api/handleRequest'

# Fetch the number of existing rows
fetched_companies_count = get_csv_row_count()

print(Fore.GREEN,f"Found {fetched_companies_count} existing rows in result.csv. Starting from there.")
last_company_number = f"{fetched_companies_count:04d}"

target_count = int(input(f"how many records you want to scrap from companie id {fetched_companies_count} : "))

batch = int(target_count/10)
for i in range(batch):

    # list of companie numbers with fetch in this batch of 10
    batch_company_numbers = [f"{((int(last_company_number)+j+1)+(i*10)):04d}" for j in range(10)]

    # fetch associated companey_ids for company numbers
    try:
        batch_company_ids = batch_fetch_company_ids(batch_company_numbers)
    except Exception as e:
        print(Fore.RED,f"Error : {e}")
        break
    
    print(f"Extracted company_ids for batch number: {i+1}")
    print(Fore.YELLOW,f"Batch : {batch_company_ids}")
   
    # go through company_ids and retrive data
    # batch processing, batch 10
    companies_data = batch_fetch_campany(batch_company_ids)

    # write companie data to resutl.
    write_to_csv(companies_data)
    print(Fore.GREEN,f"Written : {fetched_companies_count+((i+1)*10)} Records : Success")