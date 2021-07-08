from os import environ

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, update

from db import currency_to_rub_rate, currency_to_usd_rate, Table

# Переменные окружения
load_dotenv()
CURRENCY_API_KEY = environ.get("CURRENCY_API_KEY")
DB_USERNAME = environ.get("DB_USERNAME")
DB_PASSWORD = environ.get("DB_PASSWORD")
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
currencies = ["RUB", "USD", "EUR", "CNY"]


# TODO: отделить запросы к API в отдельную функцию
def update_currency_rate(db_table: Table, from_currency_code: str, to_currency_codes: list[str]) -> None:
    """Gets currencies rates and puts it into the Data Marts

    :returns: None
    """

    from_currency_code = from_currency_code.upper()
    to_currency_codes = [to_currency_code.upper() for to_currency_code in to_currency_codes]

    querystring = {"format": "json", "from": from_currency_code, "to": ', '.join(to_currency_codes), "amount": "1"}
    response = requests.request("GET", url, headers=headers, params=querystring).json()

    rates = response["rates"]

    # TODO: Обернуть все stmt в одну транзакцию
    # Изменение курса валют
    for currency_code in to_currency_codes:
        stmt = (
            update(db_table).
            where(db_table.c.currency_code == currency_code).
            values(rate=(rates[currency_code]["rate"] ** (-1)))
        )
        conn.execute(stmt)


def update_currencies_rates(to_currencies: list[str]) -> None:
    update_currency_rate(currency_to_rub_rate, "RUB", to_currencies)
    update_currency_rate(currency_to_usd_rate, "USD", to_currencies)


def update_rates_history():
    return None
