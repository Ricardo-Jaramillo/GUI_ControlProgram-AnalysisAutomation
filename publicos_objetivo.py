from credentials import dic_credentials
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from connection import Conn
from productos import Productos

# Create a Class to handle the Monetizacion data that inherits from the Conn class
class PublicosObjetivo():
    
    def __init__(self):
        self.df_pos_agg = pd.DataFrame()
        self.set_pos_variables()
    
    def set_pos_variables(self, tiendas='', is_online=0, condicion=0, inicio='', termino=''):
        self.tiendas = tiendas
        self.is_online = is_online
        self.condicion = condicion
        self.inicio = inicio
        self.termino = termino
    
    def get_query_create_pos_temporal(self, table_name='#PO'):
        # Obtener las fechas de inicio y fin de la campaña quitando el día
        campana_ini = self.inicio[:7]
        campana_fin = self.termino[:7]

        # Restar 1, 2, 3, 4, 6, 7, 12 meses a la fecha de inicio de la campaña
        campana_ini_1 = (pd.to_datetime(self.inicio) - pd.DateOffset(months=1)).strftime('%Y-%m-%d')[:7]
        campana_ini_2 = (pd.to_datetime(self.inicio) - pd.DateOffset(months=2)).strftime('%Y-%m-%d')[:7]
        campana_ini_3 = (pd.to_datetime(self.inicio) - pd.DateOffset(months=3)).strftime('%Y-%m-%d')[:7]
        campana_ini_4 = (pd.to_datetime(self.inicio) - pd.DateOffset(months=4)).strftime('%Y-%m-%d')[:7]
        campana_ini_6 = (pd.to_datetime(self.inicio) - pd.DateOffset(months=6)).strftime('%Y-%m-%d')[:7]
        campana_ini_7 = (pd.to_datetime(self.inicio) - pd.DateOffset(months=7)).strftime('%Y-%m-%d')[:7]
        campana_ini_12 = (pd.to_datetime(self.inicio) - pd.DateOffset(months=12)).strftime('%Y-%m-%d')[:7]

        campana_ini, campana_fin, campana_ini_1, campana_ini_2, campana_ini_3, campana_ini_4, campana_ini_6, campana_ini_7, campana_ini_12

        if self.condicion:
            query_tx_elegibles = f'''
                DROP TABLE IF EXISTS #TX_ELEGIBLES;
                CREATE TABLE #TX_ELEGIBLES AS (
                SELECT
                    CUSTOMER_CODE_TY
                    ,MAX(CASE WHEN VENTA >= '{self.condicion}' THEN 1 ELSE 0 END) IND_ELEGIBLE
                FROM (
                    SELECT
                    CUSTOMER_CODE_TY
                    ,INVOICE_NO
                    ,SUM(SALE_NET_VAL) VENTA
                    FROM FCT_SALE_LINE
                    INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
                    WHERE IND_MARCA = 1
                    AND LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_12}' AND '{campana_fin}'
                    AND BUSINESS_TYPE = 'R'
                    AND SALE_NET_VAL > 0
                    GROUP BY 1,2
                )
                GROUP BY 1
                HAVING IND_ELEGIBLE = 1
                );
            '''
        else:
            query_tx_elegibles = ''

        query_pos_temporal = query_tx_elegibles + \
        f'''
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} AS (
            WITH
            __PROD_MARCA AS (
                SELECT DISTINCT
                PRODUCT_CODE
                FROM #PRODUCTOS
                WHERE IND_MARCA = 1
            )
            ,__PROD_COM AS (
                SELECT DISTINCT
                PRODUCT_CODE
                FROM #PRODUCTOS
                WHERE IND_MARCA = 0
            )
            --INDICADORES PARA POS
            ,__IND_PO AS (
                SELECT
                A.CUSTOMER_CODE_TY CUSTOMER_CODE
                ,B.VALID_CONTACT_INFO
                ,B.NSE
                ,B.TIPO_FAMILIA

                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini}' AND '{campana_fin}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_NATURAL_EVENT

                ,SUM(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini}' AND '{campana_fin}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN SALE_NET_VAL END) VENTA_NATURAL
                ,COUNT(DISTINCT CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini}' AND '{campana_fin}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN INVOICE_NO END) TX_NATURAL
                ,SUM(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini}' AND '{campana_fin}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN SALE_TOT_QTY END) UNIDADES_NATURAL

                ,SUM(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_12}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN SALE_NET_VAL END) VENTA
                ,COUNT(DISTINCT CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_12}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN INVOICE_NO END) TX
                ,SUM(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_12}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN SALE_TOT_QTY END) UNIDADES
                
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_12}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_CAP_12_MARCA
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_12}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_COM) THEN 1 ELSE 0 END) IND_CAP_12_COM
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_6}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_CAP_6_MARCA
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_6}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_COM) THEN 1 ELSE 0 END) IND_CAP_6_COM
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_3}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_CAP_3_MARCA
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_3}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_COM) THEN 1 ELSE 0 END) IND_CAP_3_COM

                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_12}' AND '{campana_ini_7}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_REC_12_6_P1
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_6}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_REC_12_6_P2
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_6}' AND '{campana_ini_4}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_REC_6_3_P1
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_3}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_REC_6_3_P2

                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_12}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_FID_12
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_6}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_FID_6
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_3}' AND '{campana_ini_1}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_FID_3

                FROM FCT_SALE_LINE A
                INNER JOIN CHEDRAUI.MON_ACT D USING(CUSTOMER_CODE_TY)
                {f"INNER JOIN (SELECT DISTINCT INVOICE_NO FROM FCT_SALE_HEADER WHERE CHANNEL_TYPE IN ('WEB','APP','CC HY') AND LEFT(INVOICE_DATE, 7) BETWEEN '{campana_ini_12}' AND '{campana_fin}') USING(INVOICE_NO)" if self.is_online else ''}
                {f'INNER JOIN CHEDRAUI.V_STORE C ON A.STORE_KEY = C.STORE_KEY AND A.STORE_CODE = C.STORE_CODE AND C.STORE_CODE IN ({self.tiendas})' if self.tiendas else ''}
                {f'INNER JOIN #TX_ELEGIBLES F ON A.CUSTOMER_CODE_TY = F.CUSTOMER_CODE_TY' if self.condicion else ''}
                LEFT JOIN CHEDRAUI.V_CUSTOMER_CONTACT B ON A.CUSTOMER_CODE_TY = B.CUSTOMER_CODE
                WHERE LEFT(A.INVOICE_DATE, 7) BETWEEN '{campana_ini_12}' AND '{campana_fin}'
                AND B.CONTACT_INFO IS NOT NULL
                AND (A.PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA)
                OR A.PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_COM))
                AND BUSINESS_TYPE = 'R'
                AND SALE_NET_VAL > 0
                --  AND C.STORE_FORMAT2 NOT IN ('08 SUPERCITO CD' ,'09 SUPERCITO SELECTO','07 SUPER CHE CD')
                GROUP BY 1,2,3,4--,5
            )
            
            --DEFINIR SEGMENTOS
            ,__SEGMENTOS_CLIENTE AS (
                SELECT
                CUSTOMER_CODE
                ,VALID_CONTACT_INFO
                ,NSE
                ,TIPO_FAMILIA

                ,IND_NATURAL_EVENT NATURAL_EVENT

                ,VENTA_NATURAL
                ,TX_NATURAL
                ,UNIDADES_NATURAL

                ,VENTA
                ,TX
                ,UNIDADES

                ,CASE WHEN IND_CAP_12_MARCA = 0 AND IND_CAP_12_COM = 1 THEN 1 ELSE 0 END CAP_12
                ,CASE WHEN IND_CAP_6_MARCA = 0 AND IND_CAP_6_COM = 1 THEN 1 ELSE 0 END CAP_6
                ,CASE WHEN IND_CAP_3_MARCA = 0 AND IND_CAP_3_COM = 1 THEN 1 ELSE 0 END CAP_3

                ,CASE WHEN IND_REC_12_6_P1 = 1 AND IND_REC_12_6_P2 = 0 THEN 1 ELSE 0 END REC_12_6
                ,CASE WHEN IND_REC_6_3_P1 = 1 AND IND_REC_6_3_P2 = 0 THEN 1 ELSE 0 END REC_6_3
                
                ,CASE WHEN IND_FID_12 = 1 THEN 1 ELSE 0 END FID_12
                ,CASE WHEN IND_FID_6 = 1 THEN 1 ELSE 0 END FID_6
                ,CASE WHEN IND_FID_3 = 1 THEN 1 ELSE 0 END FID_3

                FROM __IND_PO
            )
            ,__IND_PO_CLIENTE AS (
                SELECT
                CUSTOMER_CODE
                ,VALID_CONTACT_INFO
                ,NSE
                ,TIPO_FAMILIA

                ,NATURAL_EVENT

                ,VENTA_NATURAL
                ,TX_NATURAL
                ,UNIDADES_NATURAL

                ,VENTA
                ,TX
                ,UNIDADES

                --PO
                ,CAP_12
                ,CAP_6
                ,CAP_3

                ,REC_12_6
                ,REC_6_3
                ,CASE WHEN REC_12_6 = 1 OR REC_6_3 = 1 THEN 1 ELSE 0 END REC_12_REC_6
                
                ,FID_12
                ,FID_6
                ,FID_3
                
                ,CASE WHEN CAP_12 = 1 OR REC_12_6 = 1 OR FID_6 = 1 THEN 1 ELSE 0 END CAP_12_REC_12_FID_6
                ,CASE WHEN CAP_12 = 1 OR REC_12_6 = 1 OR REC_6_3 = 1 OR FID_3 = 1 THEN 1 ELSE 0 END CAP_12_REC_12_REC_6_FID_3
                ,CASE WHEN CAP_12 = 1 OR REC_12_6 = 1 OR REC_6_3 = 1 THEN 1 ELSE 0 END CAP_12_REC_12_REC_6
                ,CASE WHEN CAP_12 = 1 OR FID_12 = 1 THEN 1 ELSE 0 END CAP_12_FID_12
                ,CASE WHEN REC_12_6 = 1 OR FID_6 = 1 THEN 1 ELSE 0 END REC_12_FID_6
                ,CASE WHEN REC_12_6 = 1 OR REC_6_3 = 1 OR FID_3 = 1 THEN 1 ELSE 0 END REC_12_REC_6_FID_3
                
                --NATURAL_EVENT
                ,CASE WHEN NATURAL_EVENT = 1 AND CAP_12 = 1 THEN 1 ELSE 0 END CAP_12_NATURAL
                ,CASE WHEN NATURAL_EVENT = 1 AND CAP_6 = 1 THEN 1 ELSE 0 END CAP_6_NATURAL
                ,CASE WHEN NATURAL_EVENT = 1 AND CAP_3 = 1 THEN 1 ELSE 0 END CAP_3_NATURAL

                ,CASE WHEN NATURAL_EVENT = 1 AND REC_12_6 = 1 THEN 1 ELSE 0 END REC_12_6_NATURAL
                ,CASE WHEN NATURAL_EVENT = 1 AND REC_6_3 = 1 THEN 1 ELSE 0 END REC_6_3_NATURAL
                ,CASE WHEN NATURAL_EVENT = 1 AND (REC_12_6 = 1 OR REC_6_3 = 1) THEN 1 ELSE 0 END REC_12_REC_6_NATURAL
                
                ,CASE WHEN NATURAL_EVENT = 1 AND FID_12 = 1 THEN 1 ELSE 0 END FID_12_NATURAL
                ,CASE WHEN NATURAL_EVENT = 1 AND FID_6 = 1 THEN 1 ELSE 0 END FID_6_NATURAL
                ,CASE WHEN NATURAL_EVENT = 1 AND FID_3 = 1 THEN 1 ELSE 0 END FID_3_NATURAL
                
                ,CASE WHEN NATURAL_EVENT = 1 AND (CAP_12 = 1 OR REC_12_6 = 1 OR FID_6 = 1) THEN 1 ELSE 0 END CAP_12_REC_12_FID_6_NATURAL
                ,CASE WHEN NATURAL_EVENT = 1 AND (CAP_12 = 1 OR REC_12_6 = 1 OR REC_6_3 = 1 OR FID_3 = 1) THEN 1 ELSE 0 END CAP_12_REC_12_REC_6_FID_3_NATURAL
                ,CASE WHEN NATURAL_EVENT = 1 AND (CAP_12 = 1 OR REC_12_6 = 1 OR REC_6_3 = 1) THEN 1 ELSE 0 END CAP_12_REC_12_REC_6_NATURAL
                ,CASE WHEN NATURAL_EVENT = 1 AND (CAP_12 = 1 OR FID_12 = 1) THEN 1 ELSE 0 END CAP_12_FID_12_NATURAL
                ,CASE WHEN NATURAL_EVENT = 1 AND (REC_12_6 = 1 OR FID_6 = 1) THEN 1 ELSE 0 END REC_12_FID_6_NATURAL
                ,CASE WHEN NATURAL_EVENT = 1 AND (REC_12_6 = 1 OR REC_6_3 = 1 OR FID_3 = 1) THEN 1 ELSE 0 END REC_12_REC_6_FID_3_NATURAL

                FROM __SEGMENTOS_CLIENTE
            )
            
            SELECT
                *
            FROM __IND_PO_CLIENTE
        );
        '''
        return query_pos_temporal
    
    def get_query_create_pos_agg_temporal(self, table_name, from_table='#PO'):
        query_pos_agg_temporal = f'''
            -- Agrupado
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} AS (
                WITH __AGG_PO AS (
                --GLOBAL POR CANAL
                SELECT
                    '01 GLOBAL' TABLA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,VALID_CONTACT_INFO
                
                --     ,SUM(NATURAL_EVENT) NATURAL_EVENT

                    ,SUM(CAP_12) CAP_12
                    ,SUM(CAP_6) CAP_6
                    ,SUM(CAP_3) CAP_3

                    ,SUM(REC_12_6) REC_12_6
                    ,SUM(REC_6_3) REC_6_3
                    ,SUM(REC_12_REC_6) REC_12_REC_6

                    ,SUM(FID_12) FID_12
                    ,SUM(FID_6) FID_6
                    ,SUM(FID_3) FID_3

                    ,SUM(CAP_12_REC_12_FID_6) CAP_12_REC_12_FID_6
                    ,SUM(CAP_12_REC_12_REC_6_FID_3) CAP_12_REC_12_REC_6_FID_3
                    ,SUM(CAP_12_REC_12_REC_6) CAP_12_REC_12_REC_6
                    ,SUM(CAP_12_FID_12) CAP_12_FID_12
                    ,SUM(REC_12_FID_6) REC_12_FID_6
                    ,SUM(REC_12_REC_6_FID_3) REC_12_REC_6_FID_3
                    
                    ,SUM(CAP_12_NATURAL) CAP_12_NATURAL
                    ,SUM(CAP_6_NATURAL) CAP_6_NATURAL
                    ,SUM(CAP_3_NATURAL) CAP_3_NATURAL

                    ,SUM(REC_12_6_NATURAL) REC_12_6_NATURAL
                    ,SUM(REC_6_3_NATURAL) REC_6_3_NATURAL
                    ,SUM(REC_12_REC_6_NATURAL) REC_12_REC_6_NATURAL

                    ,SUM(FID_12_NATURAL) FID_12_NATURAL
                    ,SUM(FID_6_NATURAL) FID_6_NATURAL
                    ,SUM(FID_3_NATURAL) FID_3_NATURAL

                    ,SUM(CAP_12_REC_12_FID_6_NATURAL) CAP_12_REC_12_FID_6_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_FID_3_NATURAL) CAP_12_REC_12_REC_6_FID_3_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_NATURAL) CAP_12_REC_12_REC_6_NATURAL
                    ,SUM(CAP_12_FID_12_NATURAL) CAP_12_FID_12_NATURAL
                    ,SUM(REC_12_FID_6_NATURAL) REC_12_FID_6_NATURAL
                    ,SUM(REC_12_REC_6_FID_3_NATURAL) REC_12_REC_6_FID_3_NATURAL
                FROM {from_table}
                GROUP BY 1, 2, 3, 4

                UNION

                --GLOBAL
                SELECT
                    '01 GLOBAL' TABLA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'00 TOTAL' VALID_CONTACT_INFO
                
                --     ,SUM(NATURAL_EVENT) NATURAL_EVENT

                    ,SUM(CAP_12) CAP_12
                    ,SUM(CAP_6) CAP_6
                    ,SUM(CAP_3) CAP_3

                    ,SUM(REC_12_6) REC_12_6
                    ,SUM(REC_6_3) REC_6_3
                    ,SUM(REC_12_REC_6) REC_12_REC_6

                    ,SUM(FID_12) FID_12
                    ,SUM(FID_6) FID_6
                    ,SUM(FID_3) FID_3

                    ,SUM(CAP_12_REC_12_FID_6) CAP_12_REC_12_FID_6
                    ,SUM(CAP_12_REC_12_REC_6_FID_3) CAP_12_REC_12_REC_6_FID_3
                    ,SUM(CAP_12_REC_12_REC_6) CAP_12_REC_12_REC_6
                    ,SUM(CAP_12_FID_12) CAP_12_FID_12
                    ,SUM(REC_12_FID_6) REC_12_FID_6
                    ,SUM(REC_12_REC_6_FID_3) REC_12_REC_6_FID_3
                    
                    ,SUM(CAP_12_NATURAL) CAP_12_NATURAL
                    ,SUM(CAP_6_NATURAL) CAP_6_NATURAL
                    ,SUM(CAP_3_NATURAL) CAP_3_NATURAL

                    ,SUM(REC_12_6_NATURAL) REC_12_6_NATURAL
                    ,SUM(REC_6_3_NATURAL) REC_6_3_NATURAL
                    ,SUM(REC_12_REC_6_NATURAL) REC_12_REC_6_NATURAL

                    ,SUM(FID_12_NATURAL) FID_12_NATURAL
                    ,SUM(FID_6_NATURAL) FID_6_NATURAL
                    ,SUM(FID_3_NATURAL) FID_3_NATURAL

                    ,SUM(CAP_12_REC_12_FID_6_NATURAL) CAP_12_REC_12_FID_6_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_FID_3_NATURAL) CAP_12_REC_12_REC_6_FID_3_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_NATURAL) CAP_12_REC_12_REC_6_NATURAL
                    ,SUM(CAP_12_FID_12_NATURAL) CAP_12_FID_12_NATURAL
                    ,SUM(REC_12_FID_6_NATURAL) REC_12_FID_6_NATURAL
                    ,SUM(REC_12_REC_6_FID_3_NATURAL) REC_12_REC_6_FID_3_NATURAL
                FROM {from_table}
                GROUP BY 1, 2, 3, 4

                UNION

                --FAMILIA POR CANAL
                SELECT
                    '02 FAMILIA' TABLA
                    ,TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,VALID_CONTACT_INFO
                
                --     ,SUM(NATURAL_EVENT) NATURAL_EVENT

                    ,SUM(CAP_12) CAP_12
                    ,SUM(CAP_6) CAP_6
                    ,SUM(CAP_3) CAP_3

                    ,SUM(REC_12_6) REC_12_6
                    ,SUM(REC_6_3) REC_6_3
                    ,SUM(REC_12_REC_6) REC_12_REC_6

                    ,SUM(FID_12) FID_12
                    ,SUM(FID_6) FID_6
                    ,SUM(FID_3) FID_3

                    ,SUM(CAP_12_REC_12_FID_6) CAP_12_REC_12_FID_6
                    ,SUM(CAP_12_REC_12_REC_6_FID_3) CAP_12_REC_12_REC_6_FID_3
                    ,SUM(CAP_12_REC_12_REC_6) CAP_12_REC_12_REC_6
                    ,SUM(CAP_12_FID_12) CAP_12_FID_12
                    ,SUM(REC_12_FID_6) REC_12_FID_6
                    ,SUM(REC_12_REC_6_FID_3) REC_12_REC_6_FID_3
                    
                    ,SUM(CAP_12_NATURAL) CAP_12_NATURAL
                    ,SUM(CAP_6_NATURAL) CAP_6_NATURAL
                    ,SUM(CAP_3_NATURAL) CAP_3_NATURAL

                    ,SUM(REC_12_6_NATURAL) REC_12_6_NATURAL
                    ,SUM(REC_6_3_NATURAL) REC_6_3_NATURAL
                    ,SUM(REC_12_REC_6_NATURAL) REC_12_REC_6_NATURAL

                    ,SUM(FID_12_NATURAL) FID_12_NATURAL
                    ,SUM(FID_6_NATURAL) FID_6_NATURAL
                    ,SUM(FID_3_NATURAL) FID_3_NATURAL

                    ,SUM(CAP_12_REC_12_FID_6_NATURAL) CAP_12_REC_12_FID_6_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_FID_3_NATURAL) CAP_12_REC_12_REC_6_FID_3_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_NATURAL) CAP_12_REC_12_REC_6_NATURAL
                    ,SUM(CAP_12_FID_12_NATURAL) CAP_12_FID_12_NATURAL
                    ,SUM(REC_12_FID_6_NATURAL) REC_12_FID_6_NATURAL
                    ,SUM(REC_12_REC_6_FID_3_NATURAL) REC_12_REC_6_FID_3_NATURAL
                FROM {from_table}
                GROUP BY 1, 2, 3, 4

                UNION

                --FAMILIA GLOBAL
                SELECT
                    '02 FAMILIA' TABLA
                    ,TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'00 TOTAL' VALID_CONTACT_INFO
                
                --     ,SUM(NATURAL_EVENT) NATURAL_EVENT

                    ,SUM(CAP_12) CAP_12
                    ,SUM(CAP_6) CAP_6
                    ,SUM(CAP_3) CAP_3

                    ,SUM(REC_12_6) REC_12_6
                    ,SUM(REC_6_3) REC_6_3
                    ,SUM(REC_12_REC_6) REC_12_REC_6

                    ,SUM(FID_12) FID_12
                    ,SUM(FID_6) FID_6
                    ,SUM(FID_3) FID_3

                    ,SUM(CAP_12_REC_12_FID_6) CAP_12_REC_12_FID_6
                    ,SUM(CAP_12_REC_12_REC_6_FID_3) CAP_12_REC_12_REC_6_FID_3
                    ,SUM(CAP_12_REC_12_REC_6) CAP_12_REC_12_REC_6
                    ,SUM(CAP_12_FID_12) CAP_12_FID_12
                    ,SUM(REC_12_FID_6) REC_12_FID_6
                    ,SUM(REC_12_REC_6_FID_3) REC_12_REC_6_FID_3
                    
                    ,SUM(CAP_12_NATURAL) CAP_12_NATURAL
                    ,SUM(CAP_6_NATURAL) CAP_6_NATURAL
                    ,SUM(CAP_3_NATURAL) CAP_3_NATURAL

                    ,SUM(REC_12_6_NATURAL) REC_12_6_NATURAL
                    ,SUM(REC_6_3_NATURAL) REC_6_3_NATURAL
                    ,SUM(REC_12_REC_6_NATURAL) REC_12_REC_6_NATURAL

                    ,SUM(FID_12_NATURAL) FID_12_NATURAL
                    ,SUM(FID_6_NATURAL) FID_6_NATURAL
                    ,SUM(FID_3_NATURAL) FID_3_NATURAL

                    ,SUM(CAP_12_REC_12_FID_6_NATURAL) CAP_12_REC_12_FID_6_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_FID_3_NATURAL) CAP_12_REC_12_REC_6_FID_3_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_NATURAL) CAP_12_REC_12_REC_6_NATURAL
                    ,SUM(CAP_12_FID_12_NATURAL) CAP_12_FID_12_NATURAL
                    ,SUM(REC_12_FID_6_NATURAL) REC_12_FID_6_NATURAL
                    ,SUM(REC_12_REC_6_FID_3_NATURAL) REC_12_REC_6_FID_3_NATURAL
                FROM {from_table}
                GROUP BY 1, 2, 3, 4

                UNION

                --NSE POR CANAL
                SELECT
                    '03 NSE' TABLA
                    ,'TOTAL' TIPO_FAMILIA
                    ,NSE
                    ,VALID_CONTACT_INFO
                
                --     ,SUM(NATURAL_EVENT) NATURAL_EVENT

                    ,SUM(CAP_12) CAP_12
                    ,SUM(CAP_6) CAP_6
                    ,SUM(CAP_3) CAP_3

                    ,SUM(REC_12_6) REC_12_6
                    ,SUM(REC_6_3) REC_6_3
                    ,SUM(REC_12_REC_6) REC_12_REC_6

                    ,SUM(FID_12) FID_12
                    ,SUM(FID_6) FID_6
                    ,SUM(FID_3) FID_3

                    ,SUM(CAP_12_REC_12_FID_6) CAP_12_REC_12_FID_6
                    ,SUM(CAP_12_REC_12_REC_6_FID_3) CAP_12_REC_12_REC_6_FID_3
                    ,SUM(CAP_12_REC_12_REC_6) CAP_12_REC_12_REC_6
                    ,SUM(CAP_12_FID_12) CAP_12_FID_12
                    ,SUM(REC_12_FID_6) REC_12_FID_6
                    ,SUM(REC_12_REC_6_FID_3) REC_12_REC_6_FID_3
                    
                    ,SUM(CAP_12_NATURAL) CAP_12_NATURAL
                    ,SUM(CAP_6_NATURAL) CAP_6_NATURAL
                    ,SUM(CAP_3_NATURAL) CAP_3_NATURAL

                    ,SUM(REC_12_6_NATURAL) REC_12_6_NATURAL
                    ,SUM(REC_6_3_NATURAL) REC_6_3_NATURAL
                    ,SUM(REC_12_REC_6_NATURAL) REC_12_REC_6_NATURAL

                    ,SUM(FID_12_NATURAL) FID_12_NATURAL
                    ,SUM(FID_6_NATURAL) FID_6_NATURAL
                    ,SUM(FID_3_NATURAL) FID_3_NATURAL

                    ,SUM(CAP_12_REC_12_FID_6_NATURAL) CAP_12_REC_12_FID_6_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_FID_3_NATURAL) CAP_12_REC_12_REC_6_FID_3_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_NATURAL) CAP_12_REC_12_REC_6_NATURAL
                    ,SUM(CAP_12_FID_12_NATURAL) CAP_12_FID_12_NATURAL
                    ,SUM(REC_12_FID_6_NATURAL) REC_12_FID_6_NATURAL
                    ,SUM(REC_12_REC_6_FID_3_NATURAL) REC_12_REC_6_FID_3_NATURAL
                FROM {from_table}
                GROUP BY 1, 2, 3, 4

                UNION

                --NSE GLOBAL
                SELECT
                    '03 NSE' TABLA
                    ,'TOTAL' TIPO_FAMILIA
                    ,NSE
                    ,'00 TOTAL' VALID_CONTACT_INFO
                
                --     ,SUM(NATURAL_EVENT) NATURAL_EVENT

                    ,SUM(CAP_12) CAP_12
                    ,SUM(CAP_6) CAP_6
                    ,SUM(CAP_3) CAP_3

                    ,SUM(REC_12_6) REC_12_6
                    ,SUM(REC_6_3) REC_6_3
                    ,SUM(REC_12_REC_6) REC_12_REC_6

                    ,SUM(FID_12) FID_12
                    ,SUM(FID_6) FID_6
                    ,SUM(FID_3) FID_3

                    ,SUM(CAP_12_REC_12_FID_6) CAP_12_REC_12_FID_6
                    ,SUM(CAP_12_REC_12_REC_6_FID_3) CAP_12_REC_12_REC_6_FID_3
                    ,SUM(CAP_12_REC_12_REC_6) CAP_12_REC_12_REC_6
                    ,SUM(CAP_12_FID_12) CAP_12_FID_12
                    ,SUM(REC_12_FID_6) REC_12_FID_6
                    ,SUM(REC_12_REC_6_FID_3) REC_12_REC_6_FID_3
                    
                    ,SUM(CAP_12_NATURAL) CAP_12_NATURAL
                    ,SUM(CAP_6_NATURAL) CAP_6_NATURAL
                    ,SUM(CAP_3_NATURAL) CAP_3_NATURAL

                    ,SUM(REC_12_6_NATURAL) REC_12_6_NATURAL
                    ,SUM(REC_6_3_NATURAL) REC_6_3_NATURAL
                    ,SUM(REC_12_REC_6_NATURAL) REC_12_REC_6_NATURAL

                    ,SUM(FID_12_NATURAL) FID_12_NATURAL
                    ,SUM(FID_6_NATURAL) FID_6_NATURAL
                    ,SUM(FID_3_NATURAL) FID_3_NATURAL

                    ,SUM(CAP_12_REC_12_FID_6_NATURAL) CAP_12_REC_12_FID_6_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_FID_3_NATURAL) CAP_12_REC_12_REC_6_FID_3_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_NATURAL) CAP_12_REC_12_REC_6_NATURAL
                    ,SUM(CAP_12_FID_12_NATURAL) CAP_12_FID_12_NATURAL
                    ,SUM(REC_12_FID_6_NATURAL) REC_12_FID_6_NATURAL
                    ,SUM(REC_12_REC_6_FID_3_NATURAL) REC_12_REC_6_FID_3_NATURAL
                FROM {from_table}
                GROUP BY 1, 2, 3, 4

                UNION

                --FAMILIA Y NSE POR CANAL
                SELECT
                    '04 FAMILIA Y NSE' TABLA
                    ,TIPO_FAMILIA
                    ,NSE
                    ,VALID_CONTACT_INFO
                
                --     ,SUM(NATURAL_EVENT) NATURAL_EVENT

                    ,SUM(CAP_12) CAP_12
                    ,SUM(CAP_6) CAP_6
                    ,SUM(CAP_3) CAP_3

                    ,SUM(REC_12_6) REC_12_6
                    ,SUM(REC_6_3) REC_6_3
                    ,SUM(REC_12_REC_6) REC_12_REC_6

                    ,SUM(FID_12) FID_12
                    ,SUM(FID_6) FID_6
                    ,SUM(FID_3) FID_3

                    ,SUM(CAP_12_REC_12_FID_6) CAP_12_REC_12_FID_6
                    ,SUM(CAP_12_REC_12_REC_6_FID_3) CAP_12_REC_12_REC_6_FID_3
                    ,SUM(CAP_12_REC_12_REC_6) CAP_12_REC_12_REC_6
                    ,SUM(CAP_12_FID_12) CAP_12_FID_12
                    ,SUM(REC_12_FID_6) REC_12_FID_6
                    ,SUM(REC_12_REC_6_FID_3) REC_12_REC_6_FID_3
                    
                    ,SUM(CAP_12_NATURAL) CAP_12_NATURAL
                    ,SUM(CAP_6_NATURAL) CAP_6_NATURAL
                    ,SUM(CAP_3_NATURAL) CAP_3_NATURAL

                    ,SUM(REC_12_6_NATURAL) REC_12_6_NATURAL
                    ,SUM(REC_6_3_NATURAL) REC_6_3_NATURAL
                    ,SUM(REC_12_REC_6_NATURAL) REC_12_REC_6_NATURAL

                    ,SUM(FID_12_NATURAL) FID_12_NATURAL
                    ,SUM(FID_6_NATURAL) FID_6_NATURAL
                    ,SUM(FID_3_NATURAL) FID_3_NATURAL

                    ,SUM(CAP_12_REC_12_FID_6_NATURAL) CAP_12_REC_12_FID_6_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_FID_3_NATURAL) CAP_12_REC_12_REC_6_FID_3_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_NATURAL) CAP_12_REC_12_REC_6_NATURAL
                    ,SUM(CAP_12_FID_12_NATURAL) CAP_12_FID_12_NATURAL
                    ,SUM(REC_12_FID_6_NATURAL) REC_12_FID_6_NATURAL
                    ,SUM(REC_12_REC_6_FID_3_NATURAL) REC_12_REC_6_FID_3_NATURAL
                FROM {from_table}
                GROUP BY 1, 2, 3, 4

                UNION

                --NSE GLOBAL
                SELECT
                    '04 FAMILIA Y NSE' TABLA
                    ,TIPO_FAMILIA
                    ,NSE
                    ,'00 TOTAL' VALID_CONTACT_INFO
                
                --     ,SUM(NATURAL_EVENT) NATURAL_EVENT

                    ,SUM(CAP_12) CAP_12
                    ,SUM(CAP_6) CAP_6
                    ,SUM(CAP_3) CAP_3

                    ,SUM(REC_12_6) REC_12_6
                    ,SUM(REC_6_3) REC_6_3
                    ,SUM(REC_12_REC_6) REC_12_REC_6

                    ,SUM(FID_12) FID_12
                    ,SUM(FID_6) FID_6
                    ,SUM(FID_3) FID_3

                    ,SUM(CAP_12_REC_12_FID_6) CAP_12_REC_12_FID_6
                    ,SUM(CAP_12_REC_12_REC_6_FID_3) CAP_12_REC_12_REC_6_FID_3
                    ,SUM(CAP_12_REC_12_REC_6) CAP_12_REC_12_REC_6
                    ,SUM(CAP_12_FID_12) CAP_12_FID_12
                    ,SUM(REC_12_FID_6) REC_12_FID_6
                    ,SUM(REC_12_REC_6_FID_3) REC_12_REC_6_FID_3
                    
                    ,SUM(CAP_12_NATURAL) CAP_12_NATURAL
                    ,SUM(CAP_6_NATURAL) CAP_6_NATURAL
                    ,SUM(CAP_3_NATURAL) CAP_3_NATURAL

                    ,SUM(REC_12_6_NATURAL) REC_12_6_NATURAL
                    ,SUM(REC_6_3_NATURAL) REC_6_3_NATURAL
                    ,SUM(REC_12_REC_6_NATURAL) REC_12_REC_6_NATURAL

                    ,SUM(FID_12_NATURAL) FID_12_NATURAL
                    ,SUM(FID_6_NATURAL) FID_6_NATURAL
                    ,SUM(FID_3_NATURAL) FID_3_NATURAL

                    ,SUM(CAP_12_REC_12_FID_6_NATURAL) CAP_12_REC_12_FID_6_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_FID_3_NATURAL) CAP_12_REC_12_REC_6_FID_3_NATURAL
                    ,SUM(CAP_12_REC_12_REC_6_NATURAL) CAP_12_REC_12_REC_6_NATURAL
                    ,SUM(CAP_12_FID_12_NATURAL) CAP_12_FID_12_NATURAL
                    ,SUM(REC_12_FID_6_NATURAL) REC_12_FID_6_NATURAL
                    ,SUM(REC_12_REC_6_FID_3_NATURAL) REC_12_REC_6_FID_3_NATURAL
                FROM {from_table}
                GROUP BY 1, 2, 3, 4
                )
                SELECT
                *
                FROM __AGG_PO
                ORDER BY 1, 2, 3, 4
                )
            '''
        return query_pos_agg_temporal

    def create_table_pos_temporal(self, conn):
        # Validar si exite la tabla #PRODUCTOS
        if not conn.validate_if_table_exists('#PRODUCTOS'):
            print('No existe la tabla #PRODUCTOS')
            return False
        else:
            table_name_po = '#PO'
            if not conn.validate_if_table_exists(table_name_po):
                print(self.get_query_create_pos_temporal(table_name_po))
                conn.execute(query=self.get_query_create_pos_temporal(table_name_po))
            
            table_name_agg = '#PO_AGG'
            if not conn.validate_if_table_exists(table_name_agg):
                conn.execute(query=self.get_query_create_pos_agg_temporal(table_name=table_name_agg, from_table=table_name_po))
                print(self.get_query_create_pos_agg_temporal(table_name=table_name_agg, from_table=table_name_po))
                self.df_pos_agg = conn.select(query=f'SELECT * FROM {table_name_agg} ORDER BY 1,2,3,4')
            return True
            