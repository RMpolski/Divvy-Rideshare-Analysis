from airflow.decorators import task
from datetime import timedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
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

def extract_data_links(url, start_year, end_year):
    """Sets up a Selenium Firefox page, which is required since the table is stored
    in a javascript object that must be loaded on a page (and not just in the raw html),
    and extracts the data links in the table in the year range from start_year to end_year.
    
    Parameters
    url: (str) the url of the data link table
    start_year: (int) the year where data is first taken from the webpage
    end_year: (int) the last year where data is extracted from the table
    
    Returns: list of link urls"""
    driver = webdriver.Firefox()
    driver.get(url)
    time.sleep(4)  # time to allow for the page to fully load
    
    links = []
    for year in range(start_year, end_year):
        links_year = driver.find_elements(By.PARTIAL_LINK_TEXT, str(year))
        links.extend([link.get_attribute('href') for link in links_year])
    
    # The current year may not have data yet, so handle the exception where it's not found
    try:
        links_thisyear = driver.find_elements(By.PARTIAL_LINK_TEXT,
                                              str(datetime.now().year))
        links.extend([link.get_attribute('href') for link in links_thisyear])
    except NoSuchElementException:
        logger.debug(f'No {datetime.now().year} data yet', exc_info=True)
        
    driver.close()
    return links

def sort_dates(link_text_arr):
    """Sorts the URL strings provided in link_text_arr, which appear
    in the last directory (/) with a format like YYYYMM
    
    Returns: list of links sorted by date"""
    date_array = [re.findall(r'\d+', linktext.split('/')[-1])[0]
                  for linktext in link_text_arr]
    sort_arr = np.argsort(date_array)
    return list(np.array(link_text_arr)[sort_arr])

def get_download_link(links, filename):
    """Provides the next sequential data link url to download, given the list 
    of links sorted by date and a file that contains links previously uploaded to Postgres.
    This function contains some of the main logic that allows for airflow's catchup=True
    option to download the desired data sequentially.
    
    Parameters
        links: list of links obtained from the divvy website, sorted by date
        filename: str filename of already-downloaded data links
        
    Returns: A single download link string we want to download next."""
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
    """Given the link_name string we want to download, this downloads and unzips
    the .csv file into a place where Postgres has permissions to access it.
    The folder is hard-coded into this function.
    
    Returns: the full path (str) of the downloaded .csv file"""
    download_folder = '/Library/PostgreSQL/15/'
    with requests.get(link_name) as resp:
        zfile = ZipFile(BytesIO(resp.content))
        fname = zfile.namelist()[0]  # also loads a __MACOSX/* file
        logger.warning(fname)
        zfile.extract(fname, download_folder)
    
    return download_folder + fname

def divvy_download_func(ti, *args, uploaded_filename=None, **kwargs):
    """The Python Callable function that runs in the Airflow task divvy_download.
    Parameters
    ti: airflow task instance required for xcom operations
    uploaded_filename: filename containing the data links already uploaded to Postgres.
                       Must be specified, but it worked best with the set up when a kwarg.
    
    Pushes to xcom a dict with the saved .csv filename (full path), and the link to the
    downloaded data file."""
    file_url = 'https://divvy-tripdata.s3.amazonaws.com/index.html'
    links = extract_data_links(file_url, 2021, datetime.now().year)
    links = sort_dates(links)
    download_link = get_download_link(links, uploaded_filename)
    saved_filename = save_link_file(download_link)

    ti.xcom_push(value={'saved_filename': saved_filename,
                  'download_link': download_link}, key='filename_link')

def save_progress_delete_file(ti, *args, uploaded_filename=None, **kwargs):
    """The Python Callable for the save_delete_file airflow task. Saves the link to the recently
    uploaded dataset to uploaded_filename and deletes the downloaded .csv file to save space.
    
    Note that the xcom_pull operation returns a 1-length list, so we have to index it to get the dict.
    
    Parameters
    ti: the task instance required for xcom operations
    uploaded_filename: the full filename to the .csv file to delete"""
    uploaded_url = ti.xcom_pull(task_ids=['divvy_download'], key='filename_link')[0]['download_link']
    with open(uploaded_filename, 'a') as f:
        f.write(uploaded_url + '\n')
    filename = ti.xcom_pull(task_ids=['divvy_download'], key='filename_link')[0]['saved_filename']
    os.remove(filename)