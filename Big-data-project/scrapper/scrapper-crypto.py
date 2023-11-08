
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from kafka import KafkaProducer
from selenium.common.exceptions import WebDriverException

# Initialisation du producteur kafka
producer = KafkaProducer(bootstrap_servers='5.156.135.86:9092')

# Paramétrage des options chrome
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--no-sandbox') 

# Paramétrage de l'affichage vituel
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument(f'--display=:99')

# Paramétrage des options chrome
chrome_options.add_argument("--headless")
chrome_options.add_argument('''user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) 
                            AppleWebKit/537.36 (KHTML, like Gecko) 
                            Chrome/58.0.3029.110 Safari/537.36''')

chrome_options.add_argument('--window-size=1920,1080')
chrome_options.add_experimental_option("detach", True)

# Initialisation du driver chrome
driver = webdriver.Chrome(executable_path="../drivers/chromedriver", options=chrome_options)

# Chargement de la page Etoro.com du cours du bitcoin
driver.get("https://www.etoro.com/fr/markets/btc")

sleep(10)

# Récupération de l'élément du DOM contenant le cours du bitcoin actualisé toutes les secondes.
element = driver.find_element(By.CLASS_NAME, "instrument-price")

# Lecture du cours du bitcoin et production d'un message dans le topic kafka
if element:
    while True:
        try:
            print(element.text)
            producer.send('bitcoin_topic', key="btc_key".encode("utf-8"), 
                          value=str(element.text).encode("utf-8"))
            producer.flush()
            sleep(30)
        except WebDriverException as e:
            print("Erreur WebDriverException : ", e)
            print("Le fenêtre a crash, tentative de réexecution en cours ...")
            driver.refresh()
            sleep(5)
            element = driver.find_element(By.CLASS_NAME, "instrument-price")
            sleep(2)
