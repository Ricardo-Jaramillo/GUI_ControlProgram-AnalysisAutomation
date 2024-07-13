from datetime import datetime
from pandas.tseries.offsets import MonthEnd
from credentials import dic_credentials
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from connection import Conn
from productos import Productos
from publicos_objetivo import PublicosObjetivo

# Create a Class to handle the Monetizacion data that inherits from the Conn class
class Monetizacion(Conn, Productos):
    
    def __init__(self, name='Monetizacion'):
        Conn.__init__(self, name=name)
        Productos.__init__(self)
        self.po = PublicosObjetivo()
        # self.rad = Radiografia()
        # self.ds = DataScience()

    def generar_productos(self, skus, marcas, proveedores, clases='', subclases='', prod_type_desc=''):
        # Set variables
        self.set_products(skus=skus, marcas=marcas, proveedores=proveedores, clases=clases, subclases=subclases, prod_type_desc=prod_type_desc)
        # Create Productos table
        self.create_tabla_productos(self)

    def generar_po(self, tiendas, is_online, condicion, inicio, termino):
        # Set variables
        self.po.set_pos_variables(tiendas=tiendas, is_online=is_online, condicion=condicion, inicio=inicio, termino=termino)
        # Create PO table
        self.po.create_table_pos_temporal(self)
    
    def generar_rad(self):
        # Set variables
        # self.rad.set_rad_variables()
        # Create RAD table
        # self.rad.create_table_rad_temporal(self)
        pass
    
    def ejecutar_ds(self):
        # self.ds.create_analisis_ds()
        pass