import os
import subprocess
import logging
import pandas as pd
import json
from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.decorators import task
from airflow.decorators import branch_python
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import codecs
import re
import traceback
from webdriver_manager.chrome import ChromeDriverManager

from google.cloud import storage
from airflow.providers.google.cloud.transfers import local_to_gcs
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCS_BUCKET = 'gcs_bucket_for_my_astro'
GCS_OBJECT_NAME = 'scrape_results.csv'
LOCAL_FILE_PATH = '/usr/local/airflow/scrape_results.csv'
BIGQUERY_DATASET_NAME = 'your_bigquery_dataset_name'
BIGQUERY_TABLE_NAME = 'your_bigquery_table_name'

default_args = {
    'owner': 'Elliot',
    'depends_on_past': False,
    'retries': 0,
    'catchup': False,
    'max_active_runs': 1,
}

with DAG(
	dag_id="astro_dag_04",
	schedule_interval="@daily",
	start_date=datetime(2024,1,1),
	end_date=datetime(2024,1,1),
	catchup=True
) as dag:
    
    date_string = "02/02/2024"

    @task
    def scrape_events_for_date(date_string, **kwargs):
        try:
            options = Options()
            options.page_load_strategy = 'eager'
            driver=webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            wait = WebDriverWait(driver, 20)
            driver.get('https://www.investing.com/economic-calendar/')

            try:
                time.sleep(10) 
                clickable_element = driver.find_element(By.XPATH, '//*[@id="PromoteSignUpPopUp"]/div[2]/i')
                clickable_element.click()
            except:
                pass

            element_id = 'datePickerToggleBtn'  
            clickable_element = wait.until(EC.element_to_be_clickable((By.ID, element_id)))
            clickable_element.click()

            time.sleep(1) 
            input_element = driver.find_element(By.ID, "startDate")
            input_element.clear()
            input_element.send_keys(date_string)

            time.sleep(1) 
            input_element = driver.find_element(By.ID, "endDate")
            input_element.clear()
            input_element.send_keys(date_string)

            clickable_element = driver.find_element(By.ID, "applyBtn")
            clickable_element.click()

            time.sleep(1)
            clickable_element = driver.find_element(By.ID, "filterStateAnchor")
            clickable_element.click()

            time.sleep(1) 
            xpath_locator = '//*[@id="calendarFilterBox_country"]/div[1]/a[2]'
            clickable_element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_locator)))
            clickable_element.click()

            time.sleep(2) 
            clickable_element = driver.find_element(By.CSS_SELECTOR, "input#country5")
            clickable_element.click()

            time.sleep(2) 
            clickable_element = driver.find_element(By.CSS_SELECTOR, "input#importance3")
            clickable_element.click()

            time.sleep(2) 
            clickable_element = driver.find_element(By.ID, "ecSubmitButton")
            clickable_element.click()

            time.sleep(10)
            event_items = driver.find_elements(By.CLASS_NAME, 'js-event-item')
            times = []
            events = []
            acts = []
            fores = []
            for item in event_items:
                logging.info(f'***** text: {item.text} ****')
                logging.info(f'***** inner HTML: {item.get_attribute("innerHTML")} ****')
                event_time = item.find_element(By.CLASS_NAME, 'js-time').get_attribute('innerHTML').strip()
                times.append(event_time)
                
                event = item.find_element(By.CLASS_NAME, 'event').get_attribute('title').strip()
                event = event.split("on ")[-1]
                events.append(event)
                
                act = item.find_element(By.CLASS_NAME, 'act').get_attribute('innerHTML').strip()
                acts.append(act)
                
                fore = item.find_element(By.CLASS_NAME, 'fore').get_attribute('innerHTML').strip()
                fores.append(fore)
            driver.quit()

            df = pd.DataFrame({
                'Time': times,
                'Event': events,
                'Act': acts,
                'Forecast': fores
            })
            df.to_csv(LOCAL_FILE_PATH, index=False)
            error_message = None
            logging.info('**** dataframe was made ****')
        except Exception as e:
            df = pd.DataFrame()
            df.to_csv(LOCAL_FILE_PATH, index=False)
            error_message = str(e)  
            logging.info(f'**** error: {error_message} ****')
        kwargs['ti'].xcom_push(key='error_message', value=error_message)

    @task.branch
    def decide_next_step(**kwargs):
        error_message = kwargs['ti'].xcom_pull(task_ids='extract_data', key='error_message')
        return 'handle_error' if error_message else 'upload_data_to_warehouse'

    @task
    def handle_error(**kwargs):
        pass

    upload_csv_to_gcs = local_to_gcs(
        task_id='upload_csv_to_gcs',
        src=LOCAL_FILE_PATH,
        dst=GCS_OBJECT_NAME,
        bucket=GCS_BUCKET,
        gcp_conn_id='gcp_conn'
    )

    # scrape_events_for_date("02/02/2024") >> decide_next_step() >> [handle_error(), upload_csv_to_gcs]
    scrape_events_for_date("02/02/2024") 