import argparse
import datetime
import time
import glob
import pickle
from collections import OrderedDict
from scraping_common import *
from save_scrape_data import *
from selenium.webdriver.common.action_chains import ActionChains
import time
import re
import csv
UPLOAD_FOLDER = '/home/johnh/upload/FanDuel_NBA.csv'

def extract_slate_sport(driver):
    """Selects the function specified for sport."""
    res = ''
    data = ''
    res = extract_slate_NBA(driver)
    print(res)
    insert_nba_slates_into_db(res, "NBA")

def check_cookies_exists(filename):
    """Check if a cookies.pkl file is already on folder.
    Returns True if so.
    """
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    if os.path.isfile(filename):
        return True
    return False


def load_cookies(driver, filename):
    """Loads cookies from file and adds it to WebDriver.
    """
    cookies = pickle.load(open(filename, 'rb'))
    for cookie in cookies:
        driver.add_cookie(cookie)
    print('Cookies Loaded.')

def save_cookies(driver, filename):
    """Saves the webdriver cookies to a file.
    """
    pickle.dump(driver.get_cookies(), open(filename, 'wb'))
    print('Cookies Saved.')

def login_fantasy_alarm(driver):
    """Makes login to fantasy sports.
    """
    email = 'johnscotthayse@gmail.com'
    passwd = 'fantasyfun2019'

    wait = WebDriverWait(driver, 10)
    driver.get('https://www.fantasyalarm.com/pga/projections')

    try:
        skip_button = driver.find_element_by_xpath(
            '//a[contains(@class, "introjs-button introjs-skipbutton")]')
        ActionChains(driver).move_to_element(skip_button).click().perform()
        time.sleep(0.5)
        print('Clicked')
    except NoSuchElementException:
        print("Error skip button.")

    try:
        login_dropdown_button = wait.until(
            EC.presence_of_element_located((By.XPATH,
            '//a[contains(@class, " dropdown-toggle")]'))
        )
        ActionChains(driver).move_to_element(login_dropdown_button)\
            .click().perform()
        time.sleep(0.5)
        print("AAA")
    except NoSuchElementException:
        print("Error skip button.")
    
    try:
        email_textfield = driver.find_elements_by_xpath(
            '//input[contains(@name, "email")]')[0]
        ActionChains(driver).move_to_element(email_textfield).click().send_keys(email)\
            .perform()
        time.sleep(0.5)
        print('BBB')
    except NoSuchElementException:
        print('Error inserting email')

    try:
        passwd_textfield = driver.find_element_by_xpath(
            '//input[contains(@name, "password")]')
        ActionChains(driver).move_to_element(passwd_textfield).click().send_keys(passwd)\
            .perform()
        time.sleep(0.5)
        print('CCC')
    except NoSuchElementException:
        print('Error inserting email')
    
    try:
        login_button = driver.find_element_by_xpath(
            '//button[contains(@class, "btn btn-block blue uppercase") and '\
                'contains(@type, "submit")]')
        ActionChains(driver).move_to_element(login_button).click().perform()
        time.sleep(10)
        print('DDD')
    except NoSuchElementException:
        print('Error login.')
    print('Logged in.')


def open_fantasy_alarm(driver):
    """Makes login into fantasyalarm.com if a cookie file doesn't exist on
    the same folder.
    """
    cookie_existing = check_cookies_exists('driver_cookies_nba.pkl')

    if(cookie_existing):
        driver.get('https://www.fantasyalarm.com/pga/projections')
        load_cookies(driver, 'driver_cookies_nba.pkl')
    else:
        login_fantasy_alarm(driver)
        save_cookies(driver, 'driver_cookies_nba.pkl')

def extract_slate_NBA(driver):
    """Extracts the slates list from sport.
    """
    wait = WebDriverWait(driver, 10)
    driver.get('https://www.fantasyalarm.com/nba/projections/daily/FD/')
    
    try:
        csv_button = wait.until(
            EC.presence_of_element_located((By.XPATH, 
            "//span[contains(text(), 'CSV')]/parent::a"))
        )
        ActionChains(driver).move_to_element(csv_button).click().perform()
        time.sleep(2)
    except NoSuchElementException:
        print('Download button not found.')
    except Exception as e:
        print('Not downloaded due to:\n{}'.format(e))
    
    # Extracts data from file and deletes it
    slate_data = extract_csv_data()
    slate_ids = extract_slates_ids()
    return_row = []
    for i in slate_data:
        for j in slate_ids:
            if(i[0] == j[0]):
              i[4] = j[1]
        return_row.append(i)
    return return_row

def extract_slates_ids():
    data = list()
    with open(UPLOAD_FOLDER,'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        i = 0
        for row in csv_reader:
            if i != 0:
               new_row = list()
               new_row.append(row[0])
               new_row.append(row[3])
               data.append(new_row)
            i = i + 1
    return data

def extract_csv_data():
    """Opens downloaded file, loads its content to Python object 
    and delete it.
    """
    data = list()
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    for file in glob.glob("*.csv"):
        with open(file, mode='r', encoding='utf8') as csv_file:
            csv_reader = csv.reader(csv_file)
            i = 0
            for row in csv_reader:
                if i != 0:
                   new_row = list()
                   new_row.append(row[0])
                   price_value = row[15].replace("$","")
                   if(price_value == ""):
                     new_row.append('0')
                   else: new_row.append(price_value)
                   new_row.append(row[1])
                   new_row.append(row[2])
                   new_row.append('325689-' + str(10000 + i))
                   if(row[1] != "-" and row[2] != "-" and row[1]!="" and row[2]!=""):data.append(new_row)
                i = i + 1
        os.remove(file)
    return data

if __name__ == "__main__":
    
    driver = get_geckodriver()
    driver.set_window_position(0, 0)
    driver.set_window_size(1920, 1080)

    try:
        open_fantasy_alarm(driver)
        extract_slate_sport(driver)
    except Exception as e:
        print('Stopped due to: \n{}'.format(e))
    finally:
        driver.close()