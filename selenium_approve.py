from selenium import webdriver
from dotenv import load_dotenv
from time import sleep
import platform
import os

from selenium.webdriver.common.by import By

load_dotenv()
username = os.getenv("SOH_USERNAME")
password = os.getenv("PASSWORD")
# print(username, password)

DRIVER_PATH = {"Linux": r'./chromedriver_linux64/chromedriver',
               "Windows": r".\chromedriver_win32\chromedriver.exe",
               "Darwin": r""}


def approve_Oauth(url, channel_name, driver=DRIVER_PATH.get(platform.system(), "")):
    print(url, channel_name, driver)

    driver = webdriver.Chrome(driver)
    # driver = webdriver.Firefox(profile)

    # url = "https://accounts.google.com/o/oauth2/auth?response_type=code&client_id=384361443029-b7l17ovd7b6ubjaudgdc8bcv7r05hh6l.apps.googleusercontent.com&redirect_uri=http://localhost:8080&scope=https://www.googleapis.com/auth/yt-analytics.readonly+https://www.googleapis.com/auth/yt-analytics-monetary.readonly&state=38b352bd542442a13531b3aad3daf263b7696711c64447ed1305c69899a2bfbd"
    # option = webdriver.ChromeOptions()
    # option.binary_location = r'C:\Program Files (x86)\BraveSoftware\Brave-Browser\Application\brave.exe'
    # driver = webdriver.Chrome(executable_path=r'.\chromedriver_win32\chromedriver.exe')

    driver.get(url)
    driver.maximize_window()
    sleep(1)
    driver.find_element(By.ID, "identifierId").send_keys(username)
    sleep(2)
    driver.find_element(By.ID, "identifierNext").click()
    sleep(2)
    driver.find_element(By.NAME, "password").send_keys(password)
    sleep(2)
    driver.find_element(By.ID, "passwordNext").click()
    sleep(2)
    # driver.find_element(By.XPATH, "//div[@class='w1I7fb'][text()[contains(.,'希望之聲粵語頻道')]").click()
    driver.find_element(
        By.XPATH, f"//*[text()[contains(.,'{channel_name}')]]").click()
    sleep(3)
    driver.find_element(
        By.XPATH, "//span[text()[contains(.,'Continue')]]").click()
    sleep(2)
    driver.find_element(
        By.XPATH, "//span[text()[contains(.,'Allow')]]").click()
    sleep(5)
    print("Logged in successfully")
