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

    def extraer_po(self):
        self.po.extraer_po()
    
    def generar_rad(self):
        self.rad.generar_rad()
    
    def ejecutar_ds(self):
        self.ds.generar_analisis_ds()