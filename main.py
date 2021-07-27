from os import environ

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, insert, select, and_, exc
from sqlalchemy.orm import Session

from db import Table, currency_to_currency_rate

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


def update_currency_to_currency_rate(db_table: Table, from_currency_codes: list[str], to_currency_codes: list[str]) -> None:
    """Gets currencies rates and puts it into the Data Store

    :returns: None
    """

    session = Session(engine)

    from_currency_codes = [from_currency_code.upper() for from_currency_code in from_currency_codes]
    to_currency_codes = [to_currency_code.upper() for to_currency_code in to_currency_codes]

    try:
        for from_currency_code in from_currency_codes:
            querystring = {"format": "json", "from": from_currency_code, "to": ', '.join(to_currency_codes), "amount": "1"}
            response = requests.request("GET", url, headers=headers, params=querystring).json()

            rates = response["rates"]

            # Изменение курса валют
            for to_currency_code in to_currency_codes:
                select_names_stmt = (
                    select([db_table]).
                    where(and_(db_table.c.from_currency_code == from_currency_code,
                               db_table.c.to_currency_code == to_currency_code))
                )
                result = conn.execute(select_names_stmt).fetchone()

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
                session.execute(insert_stmt)
        session.commit()
    except exc.SQLAlchemyError:
        session.rollback()
        raise
    finally:
        session.close()
