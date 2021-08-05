from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

import requests
from sqlalchemy import insert, select, and_, exc, create_engine, Table
from db import currency_to_currency_rate

from typing import List
from datetime import datetime

db_connection_uri = 'mssql+pyodbc://{db_server}/{db_name}?driver={db_driver}'.format(
    db_server=Variable.get("db_server"),
    db_name=Variable.get("db_name"),
    db_driver=Variable.get("db_driver")
)
engine = create_engine(db_connection_uri, echo=True)
conn = engine.connect()

currencies = ["RUB", "USD", "EUR", "CNY"]


def _extract_data(from_currency_codes: List[str], to_currency_codes: List[str]) -> dict:
    """Makes an API request to receive fresh currencies rates

    :param from_currency_codes:
    :param to_currency_codes:
    :return:
    """

    from_currency_codes = [from_currency_code.upper() for from_currency_code in from_currency_codes]
    to_currency_codes = [to_currency_code.upper() for to_currency_code in to_currency_codes]

    rates = {}
    for from_currency_code in from_currency_codes:
        querystring = {"format": "json", "from": from_currency_code, "to": ', '.join(to_currency_codes), "amount": "1"}
        response = requests.request("GET", "https://currency-converter5.p.rapidapi.com/currency/convert", headers={
                                                        'x-rapidapi-key': Variable.get("currency_api_key"),
                                                        'x-rapidapi-host': "currency-converter5.p.rapidapi.com"
                                                    },
                                    params=querystring).json()

        rates[from_currency_code] = response["rates"]
    return rates


def _load_data(ti, db_table: Table) -> None:
    """Inserts a currency's fresh rate into Data Store

    :param db_table:
    :param rates:
    :param from_currency_code:
    :param to_currency_code:
    :return:
    """
    
    rates = ti.xcom_pull(task_ids="transform_data")
    
    for from_currency_code in currencies:
        for to_currency_code in currencies:   
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
                        rate=rates[from_currency_code][to_currency_code],
                        from_currency_en_name=result[db_table.c.from_currency_en_name],
                        from_currency_ru_name=result[db_table.c.from_currency_ru_name],
                        to_currency_en_name=result[db_table.c.to_currency_en_name],
                        to_currency_ru_name=result[db_table.c.to_currency_ru_name]
                    )
                )
                conn.execute(insert_stmt)
            except exc.SQLAlchemyError:
                raise


def _transform_data(ti, db_table: Table) -> Dict:
    """Transforms currencies rates into more useful form

    :param db_table:
    :param from_currency_codes:
    :param to_currency_codes:
    :returns: None
    """

    rates = ti.xcom_pull(task_ids="extract_data")
    transformed_rates = {}
    for from_currency_code, response in rates.items():
        for to_currency_code in currencies:
            transformed_rates[from_currency_code] = response[to_currency_code]["rate"]
    
    return transformed_rates


with DAG("dwh_edu_task", start_date=datetime(2021, 8, 5), schedule_interval="0 0 * * *", catchup=False) as dag:
    extract_data = PythonOperator(task_id="extract_data", python_callable=_extract_data, op_kwargs={
        'from_currency_codes': currencies,
        'to_currency_codes': currencies
    })
    transform_data = PythonOperator(task_id="transform_data", python_callable=_transform_data, op_kwargs={
        'db_table': currency_to_currency_rate
    })
    load_data = PythonOperator(task_id="load_data", python_callable=_load_data, op_kwargs={
        'db_table': currency_to_currency_rate
    })

    extract_data >> transform_data >> load_data
