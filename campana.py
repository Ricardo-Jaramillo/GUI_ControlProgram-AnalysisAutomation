import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta

# Create a Class to handle the Radiografia data
class Campana():
    def __init__(self):
        self.set_camp_variables()
    
    def set_camp_variables(self):
        self.dict_tablas_sql = {'Descripción': 'MON_CAMP_DESC',
                                'Listas': 'MON_CAMP_LIST',
                                'Cupones': 'MON_CAMP_COUPON',
                                'Ofertas': 'MON_CAMP_OFFER_CODE'}

    def get_campana_info(self, conn, nombre):
        codigo_campana = conn.select(f"SELECT CODIGO_CAMPANA FROM CHEDRAUI.MON_CAMP_DESC WHERE NOMBRE = '{nombre}'").iloc[0,0]  
        lis_df = []
        lis_queries = [        
            f"SELECT CODIGO_CAMPANA, NOMBRE, DESCRIPCION, CANALES, COSTO_PROMOCIONAL, CONDICION_COMPRA, VIGENCIA_INI, VIGENCIA_FIN FROM CHEDRAUI.MON_CAMP_DESC WHERE CODIGO_CAMPANA = '{codigo_campana}'"
            ,f"SELECT CODIGO_CAMPANA, LIST_NAME, LIST_ID, LIST_ID_GC, CANAL, SEGMENTO FROM CHEDRAUI.MON_CAMP_LIST WHERE CODIGO_CAMPANA = '{codigo_campana}'"
            ,f"SELECT * FROM CHEDRAUI.MON_CAMP_COUPON WHERE CODIGO_CAMPANA = '{codigo_campana}'"
            ,f"SELECT * FROM CHEDRAUI.MON_CAMP_OFFER_CODE WHERE CODIGO_CAMPANA = '{codigo_campana}'"
        ]

        for query in lis_queries:
            lis_df.append(conn.select(query))

        return lis_df
    
    def guardar_info_campana(self, conn, nombre_campana, table_name, df):
        table_name = self.dict_tablas_sql[table_name]
        # Borrar la información de la campaña si ya existe
        conn.execute(f"DELETE CHEDRAUI.{table_name} WHERE CODIGO_CAMPANA = (SELECT CODIGO_CAMPANA FROM CHEDRAUI.MON_CAMP_DESC WHERE NOMBRE = '{nombre_campana}')")
        # Inserta la información de la campaña
        conn.insert(table_name, df)

    def eliminar_info_campana(self, conn, nombre_campana):
        codigo_campana = conn.select(f"SELECT CODIGO_CAMPANA FROM CHEDRAUI.MON_CAMP_DESC WHERE NOMBRE = '{nombre_campana}'").iloc[0,0]  
        lis_queries = [        
            f"DELETE CHEDRAUI.MON_CAMP_DESC WHERE CODIGO_CAMPANA = '{codigo_campana}'"
            ,f"DELETE CHEDRAUI.MON_CAMP_LIST WHERE CODIGO_CAMPANA = '{codigo_campana}'"
            ,f"DELETE CHEDRAUI.MON_CAMP_COUPON WHERE CODIGO_CAMPANA = '{codigo_campana}'"
            ,f"DELETE CHEDRAUI.MON_CAMP_OFFER_CODE WHERE CODIGO_CAMPANA = '{codigo_campana}'"
        ]

        for query in lis_queries:
            conn.execute(query)
