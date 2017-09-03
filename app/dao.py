#!/usr/bin/env python
"""
Simple Data Access Object
"""

import pandas as pd
import sqlalchemy


class SimpleDao(object):
    """Simplified Data Access Object to query Postgres databases
    """

    def __init__(self, host, port, user, password, db, schema='public'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.schema = schema
        self.engine = sqlalchemy.create_engine(self.get_connection_string())

    def get_connection_string(self):
        return 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(self.user, self.password, self.host, self.port, self.db)

    def get_engine(self):
        return self.engine

    def run_query(self, query, autocommit=True):
        with self.engine.connect() as conn:
            if self.schema != 'public':
                conn.execute("SET search_path TO public, {}".format(self.schema))
            conn.execute(sqlalchemy.text(query).execution_options(autocommit=autocommit))

    def download_from_query(self, query):
        with self.engine.connect() as conn:
            if self.schema != 'public':
                conn.execute("SET search_path TO public, {}".format(self.schema))
            return pd.read_sql(query, conn)

    def upload_from_dataframe(self, df, table_name, if_exists='replace'):
        df.to_sql(table_name, self.engine, schema=self.schema, if_exists=if_exists, index=False, chunksize=1000000)


if __name__ == '__main__':
    dao = SimpleDao('localhost', '5432', 'postgres', 'postgres', 'kaggle-wttsf')
