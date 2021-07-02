from sqlalchemy import Table, Column, String, MetaData, Numeric, Integer

metadata = MetaData()
rub_rates = Table('rub_rates', metadata,
                  Column('id', Integer, primary_key=True, autoincrement=True),
                  Column('currency_name', String),
                  Column('rate', Numeric)
                  )

usd_rates = Table('usd_rates', metadata,
                  Column('id', Integer, primary_key=True, autoincrement=True),
                  Column('currency_name', String),
                  Column('rate', Numeric)
                  )
