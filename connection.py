from credentials import dic_credentials
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Create a Class Connection
class Conn:

    def __init__(self, name='test'):
        self.name = name

    def __str__(self):
        return self.name

    def connect_db(self):
        '''
        Call this method to connect to AWS RDS database using the psycopg2 module.
        Returns an object with the stablished connection
        '''
        try:
            conn = psycopg2.connect(
                host=dic_credentials['host'],
                database=dic_credentials['database'],
                port=dic_credentials['port'],
                user=dic_credentials['user'],
                password=dic_credentials['password']
            )
            conn.autocommit = True
            self.conn = conn
            self.cursor = conn.cursor()
            print('Connection successfull')
        
        except Exception as e:
            print(f'Connection failed\nError near at: {e}')

    def execute(self, query):
        print('Executing query...')
        self.cursor.execute(query)
        print('Successfull execution')

    def insert(self, query, tuple_values):
       '''
       Query must have the sinaxys of "INSERT INTO table_name VALUES %s"
       '''
       print('Inserting data...')
       execute_values(self.cursor, query, tuple_values)
       print(f'Successfull insert: {len(tuple_values)} rows')

    def select(self, query):
        print('Selecting data...')
        return pd.read_sql(query, self.conn)
    
    def close(self):
        try:
            print('Closing connection...')
            self.conn.close()
            print('Connection closed')
        except Exception as e:
            print('Error at closing connection: {e}')