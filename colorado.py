"""
Scan through the html links to colorado's website, and save any .las files
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

    print(df.status.value_counts())
    print(f'{100*(df.status == "complete").sum() / df.shape[0]:.2f}% complete')

    # Start at index given
    df = df[df.index >= start_index]

    # Open browser
    driver = webdriver.Chrome()

    # Iterate over entries (web addresses)
    try:
        for i, (url, api) in df[['Docs', 'API']][df.status != 'complete'].copy().iterrows():
            stdout.write(f"\r{i}   ")
            stdout.flush()

            # open page
            driver.get(url)

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
                    check_rows(driver, f'{i}.{page_num}', url, api)
            else:
                # Only one page
                check_rows(driver, i, url, api)

            df.loc[i, 'status'] = 'complete'

    # For all captured errors, keep going!
    except WebDriverException as e:
        # Can't open page
        df.loc[i, 'status'] = "can't open page (WebDriverException)"
        print(f'\nERROR: {e}\nRestarting...')
        main(df, i+1)
    except TimeoutError as e:
        num_prior_timeouts = re.search(r'timeout error \(\d+\)', df.loc[i, 'status'])
        if num_prior_timeouts:
            num_prior_timeouts = int(num_prior_timeouts.group(0))
            df.loc[i, 'status'] = f'timeout error ({num_prior_timeouts + 1})'

            # try up to 5 times
            print(f'\nERROR {e}\nRestarting...')
            if num_prior_timeouts < 5:
                main(df, i)
            else:
                main(df, i+1)
    except MaxRetryError as e:
        df.loc[i, 'status'] = 'max retry error'
        print(f'\nERROR: {e}\nRestarting...')
        main(df, i+1)
    except ConnectionError as e:
        df.loc[i, 'status'] = 'general connection error'
        print(f'\nERROR: {e}\nRestarting...')
        main(df, i+1)
    except ReadTimeout as e:
        df.loc[i, 'status'] = 'read timeout error'
        print(f'\nERROR: {e}\nRestarting...')
        main(df, i+1)
    except Exception as e:
        df.loc[i, 'status'] = f'error without except: {e}'
        print(f'\nUnhandled error: {e}\nQuitting...')
        raise
    finally:
        print(f'\n\n{dt.now()}')
        df.to_csv(INPUT_FILENAME, index=False)
        driver.quit()


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
                    if filename.lower().endswith('.las'):
                        print(f'\n{i} LAS file found: downloading')
                        print(f'URL: {url}')
                        print(f'API: {api}')
                        print('\t"' + filename + '"')
                        download_file(download_url, OUTPUT_FOLDERNAME / filename)
                    elif not (filename.lower().endswith('.tif')
                            or filename.lower().endswith('.pdf')
                            or filename.lower().endswith('.xls')
                            or filename.lower().endswith('.xlsx')
                            or filename.lower().endswith('.xml')):
                        print(f'\n{i} UNEXPECTED FILE TYPE (still downloading)')
                        print(f'URL: {url}')
                        print(f'API: {api}')
                        print('\t"' + filename + '"')
                        download_file(download_url, OUTPUT_FOLDERNAME / filename)

        except NoSuchElementException:
            pass


def download_file(url, file_path):
    # TODO: Check that the API given matches the API in the LAS file
    reply = get(url, stream=True)
    with open(file_path, 'wb') as file:
        for chunk in reply.iter_content(chunk_size=1024):
            if chunk:
                file.write(chunk)


if __name__ == '__main__':
    main()
