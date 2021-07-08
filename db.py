from sqlalchemy import Table, Column, String, MetaData, Numeric, Integer

metadata = MetaData()
currency_to_rub_rate = Table('currency_to_rub_rate', metadata,
                             Column('id', Integer, primary_key=True, autoincrement=True),
                             Column('currency_code', String),
                             Column('currency_name', String),
                             Column('rate', Numeric)
                             )

currency_to_usd_rate = Table('currency_to_usd_rate', metadata,
                             Column('id', Integer, primary_key=True, autoincrement=True),
                             Column('currency_code', String),
                             Column('currency_name', String),
                             Column('rate', Numeric)
                             )
