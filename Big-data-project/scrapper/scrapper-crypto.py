from time import sleep
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

chrome_options = webdriver.ChromeOptions()
chrome_options.headless = False
chrome_options.add_experimental_option("detach", True)

driver = webdriver.Chrome(executable_path='../drivers/chromedriver', options=chrome_options)
driver.get("https://www.etoro.com/fr/markets/btc")

sleep(5)

element = driver.find_element(By.CLASS_NAME, "instrument-price")

last_court_btc = None

while True:
    if last_court_btc != element.text:
        last_court_btc = element.text
        producer.send('bitcoin_topic', key="btc_key".encode("utf-8"), value=str(element.text).encode("utf-8"))
        producer.flush()
    sleep(1)