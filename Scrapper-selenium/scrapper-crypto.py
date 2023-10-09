from time import sleep
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

chrome_options = webdriver.ChromeOptions()
chrome_options.headless = True
chrome_options.add_experimental_option("detach", True)
driver = webdriver.Chrome(options=chrome_options)
driver.get("https://www.etoro.com/fr/markets/btc")
print("avant le sleep")
sleep(5)
print("après le sleep")

element = driver.find_element(By.CLASS_NAME, "instrument-price")
graph_tab = driver.find_element(By.XPATH, "/html/body/app-root/et-layout-main/div/div[2]/div[2]/div[2]/div/ui-layout/ng-view/et-market-page/div/div[1]/et-tabs/nav/a[2]")

graph_tab.click()

while True:
    print(element.text)
    sleep(2)