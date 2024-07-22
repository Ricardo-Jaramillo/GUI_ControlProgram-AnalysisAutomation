from credentials import dic_credentials
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Create a Class Connection
class Conn:

    def __init__(self, name='test'):
        self.name = name
        self.connect_db()

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
        print(query)
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
        print(query)
        return pd.read_sql(query, self.conn)
    
    def close(self):
        try:
            print('Closing connection...')
            self.drop_temporal_tables('All')
            self.conn.close()
            print('Connection closed')
        except Exception as e:
            print('Error at closing connection: {e}')

    # Function to drop temporal tables
    def drop_temporal_tables(self, *args):
        if len(args) == 1 and isinstance(args[0], list):
            table_names = args[0]
        else:
            table_names = args

        if table_names[0] == 'All':
            table_names = ['#PRODUCTOS', '#PO', '#PO_AGG', '#NUM_TX', '#NUM_UNIDADES', '#TX_MEDIO', '#DATOS_CLIENTE', '#PO_ENVIOS', '#LISTAS_ENVIO']
            
        for table_name in table_names:
            print(f'Dropping temporal table: {table_name}...')
            self.cursor.execute(query=f'DROP TABLE IF EXISTS {table_name}')
            print('Table dropped')
        return
    
    def override_table(self, table_name, query):
        print('Overriding table...')
        self.drop_temporal_tables(table_name)
        self.execute(query)
        return

    def validate_if_table_exists(self, table_name):
        # Validate if table exists, if not, catch exception
        try:
            self.select(query=f'SELECT 1 FROM {table_name} LIMIT 1')
            print('Table exists')
            return True
        except Exception as e:
            print('Table does not exist')
            return False
        
