# Python Modules
import os
from configparser import ConfigParser
# Project Modules

# Postgresql Module
import psycopg2
import pymysql

class PostgresqlSourceConnection:

    @staticmethod
    def db_instance(source_db):
    
        try:
            connection = psycopg2.connect(
                database=source_db['database'],
                user=source_db['username'],
                password=source_db['password'],
                host=source_db['host'],
                port=source_db['port'])
            return connection
        except Exception as e:
            print(e) 
            return e  




class MySqlSourceConnection:

    @staticmethod
    def db_instance(source_db):
        try:
            connection = pymysql.connect(
                user=source_db['username'],
                passwd=source_db['password'],
                host=source_db['host'],
                port=source_db['port'],
                database=source_db['database']
                )
            return connection
        except Exception as e:
            print(e)  
            return e  


class SqlSourceConnection:

    @staticmethod
    def db_instance(source_db):
        try:
            connection = pymysql.connect(
                user=source_db['username'],
                passwd=source_db['password'],
                host=source_db['host'],
                port=source_db['port'],
                database=source_db['database']
                )
            return connection
        except Exception as e:
            print(e)  
            return e  




