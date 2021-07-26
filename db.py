import datetime
from sqlalchemy import Table, Column, String, MetaData, Numeric, Date


metadata = MetaData()
currency_to_currency_rate = Table('currency_to_currency_rate', metadata,
                                  Column('date', Date, primary_key=True, default=datetime.datetime.now().date()),
                                  Column('from_currency_code', String(3), primary_key=True),
                                  Column('to_currency_code', String(3), primary_key=True),
                                  Column('rate', Numeric(10, 4)),
                                  Column('from_currency_en_name', String(255)),
                                  Column('from_currency_ru_name', String(255)),
                                  Column('to_currency_en_name', String(255)),
                                  Column('to_currency_ru_name', String(255))
                                  )
