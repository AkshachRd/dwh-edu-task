from os import environ

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, update

from db import rub_rates

# Переменные окружения
load_dotenv()
CURRENCY_API_KEY = environ.get("CURRENCY_API_KEY")
DB_USERNAME = environ.get("DB_USERNAME")  # TODO: Вот это можно убрать, но я пока не буду
DB_PASSWORD = environ.get("DB_PASSWORD")  # TODO: Это тоже
DB_SERVER = environ.get("DB_SERVER")
DB_NAME = environ.get("DB_NAME")
DB_DRIVER = environ.get("DB_DRIVER")

# Работа с БД
db_connection_uri = 'mssql+pyodbc://{db_server}/{db_name}?driver={db_driver}'.format(
    db_server=DB_SERVER,
    db_name=DB_NAME,
    db_driver=DB_DRIVER
)
engine = create_engine(db_connection_uri, echo=True)
conn = engine.connect()

# Работа с API
url = "https://currency-converter5.p.rapidapi.com/currency/convert"
headers = {
    'x-rapidapi-key': CURRENCY_API_KEY,
    'x-rapidapi-host': "currency-converter5.p.rapidapi.com"
}


# Работа с ETL
def update_currencies_to_rub_rates():
    querystring = {"format": "json", "from": "RUB", "to": "RUB, USD, EUR, CNY", "amount": "1"}
    response = requests.request("GET", url, headers=headers, params=querystring).json()

    # TODO: Вопрос! Я не знаю, нужно ли было тут всё разделять, но решил, что так будет лучше)
    rates = response["rates"]
    usd_rate = rates["USD"]["rate"]
    eur_rate = rates["EUR"]["rate"]
    cny_rate = rates["CNY"]["rate"]

    # TODO: Обернуть все stmt в одну транзакцию
    # Изменение курса валют
    for rate_id, rate in enumerate([usd_rate, eur_rate, cny_rate], 2):
        stmt = (
            update(rub_rates).
            where(rub_rates.c.id == rate_id).
            values(rate=(rate**(-1)))
        )
        conn.execute(stmt)
