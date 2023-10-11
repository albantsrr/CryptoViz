from time import sleep
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from kafka import KafkaProducer
from selenium.common.exceptions import WebDriverException

producer = KafkaProducer(bootstrap_servers='172.18.0.5:9092')

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
chrome_options.add_argument('--window-size=1920,1080')
chrome_options.add_experimental_option("detach", True)

driver = webdriver.Chrome(executable_path='../drivers/chromedriver', options=chrome_options)
driver.get("https://www.etoro.com/fr/markets/btc")

sleep(10)

element = driver.find_element(By.CLASS_NAME, "instrument-price")

if element:
    while True:
        try:
            print(element.text)
            producer.send('bitcoin_topic', key="btc_key".encode("utf-8"), value=str(element.text).encode("utf-8"))
            producer.flush()
            sleep(1)
        except WebDriverException as e:
            print("Erreur WebDriverException : ", e)
            print("Le fenêtre a crash, tentative de réexecution en cours ...")
            driver.refresh()
            sleep(5)
            element = driver.find_element(By.CLASS_NAME, "instrument-price")
            sleep(2)