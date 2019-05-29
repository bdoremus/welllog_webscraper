"""
Scan through the html links to colorado's website, and save any .las files

TODO:
    Add logging
    Check that API matches file
    Figure out how to handle files without extensions
    Figure out how to handle non-las files
"""

from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from requests import get

# Error handling
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from urllib3.exceptions import MaxRetryError
from requests.exceptions import ConnectionError, ReadTimeout

# from clint.textui import progress
import re
import pandas as pd
from pathlib import Path
from sys import stdout
from datetime import datetime as dt

INPUT_FILENAME = Path('input/colorado_links.csv')
OUTPUT_FOLDERNAME = Path('output/colorado')
START_ROW = 5


def main(df=pd.DataFrame(), start_index=0):
    if df.shape == (0, 0):
        df = load_df()

    assert 'API' in df.columns and 'Docs' in df.columns, \
        f'Missing columns "API" or "Docs" in row {START_ROW} of {INPUT_FILENAME}'

    if 'status' not in df.columns:
        df = df.assign(status='pending')
    df.status.fillna('pending', inplace=True)

    vc = df.status.value_counts()
    print(vc[vc > 1])
    print(
        f'{100 - 100 * ((df.status == "pending") | df.status.str.contains("error")).sum() / df.shape[0]:.2f}% complete')

    # Start at index given
    df = df[df.index >= start_index]

    # Open browser
    driver = webdriver.Chrome()

    # Iterate over entries (web addresses)
    try:
        for i, (url, api) in df[['Docs', 'API']][(df.status == 'pending') |
                                                 df.status.str.contains('error')].copy().iterrows():
            stdout.write(f"\r{i}   ")
            stdout.flush()

            try:
                # open page
                driver.get(url)
                found_files = []

                # See if the last row is a list of page numbers
                last_row = driver.find_elements_by_tag_name('tr')[-1]
                last_row_list = last_row.text.split(' ')
                if last_row_list[0] == '1':
                    # Multiple pages
                    for page_num in last_row_list:
                        stdout.write(f"\r{i}.{page_num}")
                        stdout.flush()
                        if page_num != '1':
                            # Advance to the next page
                            last_row.find_element_by_link_text(page_num).send_keys(Keys.RETURN)
                            last_row = driver.find_elements_by_tag_name('tr')[-1]
                        found_files += check_rows(driver, f'{i}.{page_num}', url, api)
                else:
                    # Only one page
                    found_files += check_rows(driver, i, url, api)

                df.at[i, 'status'] = found_files if found_files else 'no files found'

            # For all captured errors, keep going!
            except (TimeoutError, MaxRetryError, ConnectionError, ReadTimeout, WebDriverException, IndexError) as e:
                df.at[i, 'status'] = f'ERROR: {e}'
                print(f'\nERROR: {e}')
                print(f'\t{url}')
            except Exception as e:
                df.at[i, 'status'] = f'unhandled error: {e}'
                print(f'\nUnhandled error: {e}')
                print(f'\t{url}')
                print(f'\t{dt.now()}')
                raise
    finally:
        print('\nUNHANDLED ERROR ENCOUNTERED.  SAVE AND QUIT.')
        df.to_csv(INPUT_FILENAME, index=False)
        driver.quit()


def load_df():
    df = pd.read_csv(INPUT_FILENAME)
    if 'Docs' in df.columns and 'API' in df.columns:
        return df
    df = pd.read_csv(INPUT_FILENAME, skiprows=START_ROW).dropna(axis=1, how='all')
    return df


def check_rows(driver, i, url, api):
    found_files = []
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
                    if filename.lower().endswith('.las'):
                        print(f'\n\tDownloading LAS file')
                        print(f'\tURL: {url}')
                        print(f'\tAPI: {api}')
                        print(f'\tFilename: {filename}')
                        download_file(download_url, OUTPUT_FOLDERNAME / filename)
                        found_files += [filename]
                    elif not (filename.lower().endswith('.tif')
                              or filename.lower().endswith('.pdf')
                              or filename.lower().endswith('.xls')
                              or filename.lower().endswith('.xlsx')
                              or filename.lower().endswith('.doc')
                              or filename.lower().endswith('.docx')
                              or filename.lower().endswith('.xml')):
                        print(f'\n\tUNEXPECTED FILE TYPE (still downloading)')
                        print(f'\tURL: {url}')
                        print(f'\tAPI: {api}')
                        print(f'\tFilename: \{filename}')
                        download_file(download_url, OUTPUT_FOLDERNAME / filename)
                        found_files += [filename]

        except NoSuchElementException:
            pass

    return found_files


def download_file(url, file_path):
    # TODO: Check that the API given matches the API in the LAS file
    reply = get(url, stream=True)
    with open(file_path, 'wb') as file:
        for chunk in reply.iter_content(chunk_size=1024):
            if chunk:
                file.write(chunk)


if __name__ == '__main__':
    main()
