from airflow.decorators import task
from datetime import timedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
# from urllib.request import urlopen  # it appears urllib may return errors in airflow
import requests
from zipfile import ZipFile
from io import BytesIO
import re
import os
import numpy as np
import time
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def sort_dates(link_text_arr):
    date_array = [re.findall(r'\d+', linktext.split('/')[-1])[0]
                  for linktext in link_text_arr]
    sort_arr = np.argsort(date_array)
    return list(np.array(link_text_arr)[sort_arr])

def get_download_link(links, filename):
    if not os.path.exists(filename):
        f = open(filename, 'x')
        f.close()
        return links[0]
    else:
        with open(filename) as f:
            uploaded = f.readlines()
            if uploaded == []:  # check the case where the file is created but nothing has been added
                return links[0]
            else:
                uploaded = [linestring.strip() for linestring in uploaded]
        
        latest_idx = links.index(uploaded[-1])
        if latest_idx == len(links)-1:
            logger.warning('No new files available')
            return Exception('No new files available.')
        else:
            return links[latest_idx+1]
    
def save_link_file(link_name):
    # with urlopen(link_name) as resp:
    #     zfile = ZipFile(BytesIO(resp.read()))
    download_folder = '/Library/PostgreSQL/15/'
    with requests.get(link_name) as resp:
        zfile = ZipFile(BytesIO(resp.content))
        fname = zfile.namelist()[0]  # also loads a __MACOSX/* file
        logger.warning(fname)
        zfile.extract(fname, download_folder)
    
    return download_folder + fname

def divvy_download_func(ti, *args, uploaded_filename=None, **kwargs):
    file_url = 'https://divvy-tripdata.s3.amazonaws.com/index.html'
    driver = webdriver.Firefox()
    driver.get(file_url)
    time.sleep(4)
    
    links = []
    for year in range(2021, datetime.now().year):
        links_year = driver.find_elements(By.PARTIAL_LINK_TEXT, str(year))
        links.extend([link.get_attribute('href') for link in links_year])
    
    try:
        links_thisyear = driver.find_elements(By.PARTIAL_LINK_TEXT,
                                              str(datetime.now().year))
        links.extend([link.get_attribute('href') for link in links_thisyear])
    except NoSuchElementException:
        logger.debug(f'No {datetime.now().year} data yet', exc_info=True)
    
    driver.close()
    links = sort_dates(links)
    download_link = get_download_link(links, uploaded_filename)
    logger.warning(download_link)
    saved_filename = save_link_file(download_link)

    ti.xcom_push(value={'saved_filename': saved_filename,
                  'download_link': download_link}, key='filename_link')

def save_progress_delete_file(ti, *args, uploaded_filename=None, **kwargs):
        uploaded_url = ti.xcom_pull(task_ids=['divvy_download'], key='filename_link')[0]['download_link']
        with open(uploaded_filename, 'a') as f:
            f.write(uploaded_url + '\n')
        filename = ti.xcom_pull(task_ids=['divvy_download'], key='filename_link')[0]['saved_filename']
        os.remove(filename)