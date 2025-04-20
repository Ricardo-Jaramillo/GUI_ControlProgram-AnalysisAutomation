import pandas as pd
import psycopg2
import yaml
from psycopg2.extras import execute_values
from util.functions.path import (
    get_file_path,
    get_neighbor_path
)
from util.functions.path import (
    credentials_str,
    functions_str,
    config_str
)

path = get_file_path(
            credentials_str,
            dir_path=get_neighbor_path(__file__, functions_str, config_str)
        )

# Create a Class Connection
class Conn:

    def __init__(self, name='test'):
        self.name = name
        self.connect_db()

    def __str__(self):
        return self.name

    def get_schema(self):
        '''
        Get schema from yaml file
        '''
        with open(path, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials['sql']['schema']

    def connect_db(self):
        '''
        Call this method to connect to AWS RDS database using the psycopg2 module.
        Returns an object with the stablished connection
        '''
        try:            
            with open(path, 'r') as file:
                credentials = yaml.safe_load(file)

            conn = psycopg2.connect(
                host=credentials['credentials']['host'],
                database=credentials['credentials']['database'],
                port=credentials['credentials']['port'],
                user=credentials['credentials']['user'],
                password=credentials['credentials']['password']
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

    def insert(self, table_name, df):
        '''
        Insert a DataFrame into a table in the database
            Parameters:
            table_name (str): The name of the table to insert the data
            df (DataFrame): The DataFrame to insert into the table
        '''
        with open(path, 'r') as file:
            credentials = yaml.safe_load(file)
        schema = self.get_schema()

        query = f'INSERT INTO {schema}.{table_name} VALUES %s'
        data = list(df.itertuples(index=False, name=None))
       
        print('Inserting data...')
        execute_values(self.cursor, query, data)
        print(f'Successfull insert: {len(data)} rows')

    def select(self, query):
        print('Selecting data...')
        print(query)
        return pd.read_sql(query, self.conn)
    
    def close(self):
        try:
            print('Closing connection...')
            # self.drop_temporal_tables('All')
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
            table_names = ['#PRODUCTOS', '#PO', '#PO_AGG', '#NUM_TX', '#NUM_UNIDADES', '#TX_MEDIO', '#DATOS_CLIENTE', '#PO_ENVIOS',
                           '#LISTAS_ENVIO', '#MON_RAD_DESC', '#MON_RAD', '#MON_RAD_DESC', '#MON_RAD_PRODUCTOS',
                           '#MON_RAD_CAT', '#MON_RAD_PRODUCTO', '#MON_RAD_MARCA', '#MON_RAD_FUNNEL_CLIENTES', '#MON_RAD_EVO', '#MON_RAD_SEGMENTADO'
                           ]
            
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
        
