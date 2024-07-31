import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta

# Create a Class to handle the Radiografia data
class Campana():
    def __init__(self):
        pass
        # self.set_camp_variables()
    
    def set_camp_variables(self, nombre):
        self.nombre = nombre

    # def get_campanas(self, conn):
    #     query = "SELECT * FROM CHEDRAUI.MON_CAMP_DESC ORDER BY NOMBRE"
    #     return conn.select(query)