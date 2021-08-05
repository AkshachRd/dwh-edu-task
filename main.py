from os import environ

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, insert, select, and_, exc

from db import Table, currency_to_currency_rate

# Переменные окружения
load_dotenv()
CURRENCY_API_KEY = environ.get("CURRENCY_API_KEY")
DB_USERNAME = environ.get("DB_USERNAME")
DB_PASSWORD = environ.get("DB_PASSWORD")
DB_SERVER = environ.get("DB_SERVER")
DB_NAME = environ.get("DB_NAME")
DB_DRIVER = environ.get("DB_DRIVER")

# Подключение БД
db_connection_uri = 'mssql+pyodbc://{db_server}/{db_name}?driver={db_driver}'.format(
    db_server=DB_SERVER,
    db_name=DB_NAME,
    db_driver=DB_DRIVER
)
engine = create_engine(db_connection_uri, echo=True)
conn = engine.connect()

# Подключение к API
url = "https://currency-converter5.p.rapidapi.com/currency/convert"
headers = {
    'x-rapidapi-key': CURRENCY_API_KEY,
    'x-rapidapi-host': "currency-converter5.p.rapidapi.com"
}

# Работа с ETL
currencies = ["RUB", "USD", "EUR", "CNY"]


def get_rates(from_currency_code: str, to_currency_codes: list[str]) -> dict:
    """Makes an API request to receive fresh currencies rates

    :param from_currency_code:
    :param to_currency_codes:
    :return:
    """

    querystring = {"format": "json", "from": from_currency_code, "to": ', '.join(to_currency_codes), "amount": "1"}
    response = requests.request("GET", url, headers=headers, params=querystring).json()

    rates = response["rates"]
    return rates


def insert_fresh_rate(db_table: Table, rates: dict, from_currency_code: str, to_currency_code: str) -> None:
    """Inserts a currency's fresh rate into Data Store

    :param db_table:
    :param rates:
    :param from_currency_code:
    :param to_currency_code:
    :return:
    """

    try:
        # Взятие данных из БД для повторного использования
        select_names_stmt = (
            select([db_table]).
            where(and_(db_table.c.from_currency_code == from_currency_code,
                       db_table.c.to_currency_code == to_currency_code))
        )
        result = conn.execute(select_names_stmt).fetchone()

        # Внесение новых курсов валют в БД
        insert_stmt = (
            insert(db_table).values(
                from_currency_code=from_currency_code,
                to_currency_code=to_currency_code,
                rate=rates[to_currency_code]["rate"],
                from_currency_en_name=result[db_table.c.from_currency_en_name],
                from_currency_ru_name=result[db_table.c.from_currency_ru_name],
                to_currency_en_name=result[currency_to_currency_rate.c.to_currency_en_name],
                to_currency_ru_name=result[currency_to_currency_rate.c.to_currency_ru_name]
            )
        )
        conn.execute(insert_stmt)
    except exc.SQLAlchemyError:
        raise


def update_currency_to_currency_rate(db_table: Table, from_currency_codes: list[str], to_currency_codes: list[str]) -> None:
    """Gets currencies rates and puts it into the Core

    :param db_table:
    :param from_currency_codes:
    :param to_currency_codes:
    :returns: None
    """

    from_currency_codes = [from_currency_code.upper() for from_currency_code in from_currency_codes]
    to_currency_codes = [to_currency_code.upper() for to_currency_code in to_currency_codes]

    for from_currency_code in from_currency_codes:
        rates = get_rates(from_currency_code, to_currency_codes)

        for to_currency_code in to_currency_codes:
            insert_fresh_rate(db_table, rates, from_currency_code, to_currency_code)


update_currency_to_currency_rate(currency_to_currency_rate, currencies, currencies)
