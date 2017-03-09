from includes.database import Database
import config
import os
import psycopg2
from retrying import retry

current_path = os.getcwd()
schema = config.db["schema"]
database = Database(dbname=config.db["dbname"], user=config.db["user"],
                    password=config.db["password"], schema=config.db["schema"],
                    host=config.db['host'], port=config.db['port'],
                    insert_count=config.db['insert_count'])



@retry(stop_max_delay=10000, wait_fixed=100, retry_on_exception=lambda e: isinstance(e, Exception))
def init_database():
    USER = 'postgres'
    PWD = 'postgres'
    connect = psycopg2.connect(user=USER, host=config.db['host'], password=PWD)
    connect.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = connect.cursor()
    cur.execute('CREATE USER ' + config.db['user'])
    cur.execute('ALTER ROLE ' + config.db['user'] + ' WITH CREATEDB')
    cur.execute('CREATE DATABASE ' + config.db['dbname'] + ' OWNER ' + config.db['user'])
    cur.execute('ALTER USER ' + config.db['user'] + ' WITH ENCRYPTED PASSWORD ' + config.db['password'])
    cur.close()
    connect.close()


@retry(stop_max_delay=10000, wait_fixed=100, retry_on_exception=lambda e: isinstance(e, Exception))
def init_schema():
    connect = psycopg2.connect(user=config.db['user'], host=config.db['host'],
                               password=config.db['password'])
    connect.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = connect.cursor()
    cur.execute('CREATE SCHEMA ' + config.db['schema'])
    cur.close()
    connect.close()
