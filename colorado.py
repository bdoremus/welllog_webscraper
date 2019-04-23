from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from requests import get
# from clint.textui import progress
import re
import pandas as pd
from pathlib import Path
from sys import stdout

INPUT_FILENAME = Path('input/colorado_links.csv')
OUTPUT_FOLDERNAME = Path('output/')
START_ROW = 5


def main():
    df = load_df()

    assert 'API' in df.columns and 'Docs' in df.columns, \
        f'Missing columns "API" or "Docs" in row {START_ROW} of {INPUT_FILENAME}'

    if 'status' not in df.columns:
        df = df.assign(status='pending')
    df.status.fillna('pending', inplace=True)

    # Open browser
    driver = webdriver.Chrome()

    # Iterate over entries (web addresses)
    try:
        for i, (url, api) in df[['Docs', 'API']][df.status == 'pending'].copy().iterrows():
            stdout.write(f"\r{i}   ")
            stdout.flush()
            # open page
            try:
                driver.get(url)
            except WebDriverException:
                print(f'\n\nCOULD NOT OPEN')
                print(i)
                print(f'URL: {url}')
                print(f'API: {api}')
                raise

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
                    check_rows(driver, f'{i}.{page_num}', url, api)
            else:
                # Only one page
                check_rows(driver, i, url, api)

            df.loc[i, 'status'] = 'complete'
    finally:
        df.to_csv(INPUT_FILENAME, index=False)


def load_df():
    df = pd.read_csv(INPUT_FILENAME)
    if 'Docs' in df.columns and 'API' in df.columns:
        return df
    df = pd.read_csv(INPUT_FILENAME, skiprows=START_ROW).dropna(axis=1, how='all')
    return df


def check_rows(driver, i, url, api):
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
                            or filename.lower().endswith('.pdf')
                            or filename.lower().endswith('.xls')
                            or filename.lower().endswith('.xlsx')
                            or filename.lower().endswith('.xml')):
                        print(i)
                        print(f'URL: {url}')
                        print(f'API: {api}')
                        print('\t"' + filename + '"')
                        download_file(download_url, OUTPUT_FOLDERNAME / filename)

        except NoSuchElementException:
            pass

    # if 'y' != input('continue (y/n)'):
    #     break


def download_file(url, file_path):
    reply = get(url, stream=True)
    with open(file_path, 'wb') as file:
        for chunk in reply.iter_content(chunk_size=1024):
            if chunk:
                file.write(chunk)


if __name__ == '__main__':
    main()
