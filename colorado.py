from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from requests import get
# from clint.textui import progress
import re
import pandas as pd
from pathlib import Path
from time import sleep

INPUT_FILENAME = Path('input/colorado_links.csv')
OUTPUT_FOLDERNAME = Path('output/')
START_ROW = 5


def main():
    df = pd.read_csv(INPUT_FILENAME, skiprows=START_ROW).dropna(axis=1, how='all')
    assert 'API' in df.columns and 'Docs' in df.columns, \
        f'Missing columns "API" or "Docs" in row {START_ROW} of {INPUT_FILENAME}'

    if 'status' not in df.columns:
        df = df.assign(status='pending')
    df.status.fillna('pending', inplace=True)

    # Open browser
    driver = webdriver.Chrome()

    # iterate over rows
    for i, (url, api) in df[['Docs', 'API']].copy().iterrows():
        # open page
        try:
            driver.get(url)
        except WebDriverException:
            print(f'\n\nCOULD NOT OPEN')
            print(i)
            print(f'URL: {url}')
            print(f'API: {api}')
            raise

        try:
            # Check the download link in each row
            for row in driver.find_elements_by_tag_name('tr'):
                try:
                    elem = row.find_element_by_link_text('Download')
                    download_url = elem.get_attribute('href')

                    reply = get(download_url, stream=True)
                    attachment = reply.headers.get('content-disposition')
                    if attachment:
                        filename = re.search(r'filename="(.*?)"', attachment)
                        if filename:
                            filename = filename.group(1).strip()
                            if not (filename.lower().endswith('.tif')
                                    or filename.lower().endswith('.pdf')):
                                print(i)
                                print(f'URL: {url}')
                                print(f'API: {api}')
                                print('\t"' + filename + '"')
                                download_file(download_url, OUTPUT_FOLDERNAME / filename)

                except NoSuchElementException:
                    pass

            # if 'y' != input('continue (y/n)'):
            #     break
        except:
            print(f'\n\nERROR: DRIVER CLOSED')
            print(i)
            print(f'URL: {url}')
            print(f'API: {api}')
            driver.close()
            raise


def download_file(url, file_path):
    reply = get(url, stream=True)
    with open(file_path, 'wb') as file:
        for chunk in reply.iter_content(chunk_size=1024):
            if chunk:
                file.write(chunk)


if __name__ == '__main__':
    main()
