from datetime import datetime
from pandas.tseries.offsets import MonthEnd
from credentials import dic_credentials
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from connection import Conn

# Create a Class to handle the Monetizacion data that inherits from the Conn class
class PublicosObjetivo(Conn):
    
    def __init__(self, name):
        self.name = name
        self.df_productos = pd.DataFrame()
        self.df_pos_agg = pd.DataFrame()
        
        self.initialize_variables_products()
        self.initialize_variables_pos()
        self.connect_db()
    
    def initialize_variables_pos(self):
        self.tiendas = ''
        self.is_online = False
        self.condicion = 0
        self.inicio = ''
        self.termino = ''

    def initialize_variables_products(self):
        self.skus = ''
        self.marcas = ''
        self.proveedores = ''
        self.clases = ''
        self.subclases = ''
        self.prod_type_desc = ''

    def drop_temporal_tables(self):
        print('Dropping temporal table: #PRODUCTOS...')
        self.execute(query='DROP TABLE IF EXISTS #PRODUCTOS')
        print('Dropping temporal table: #PO')
        self.execute(query='DROP TABLE IF EXISTS #PO')
        print('Dropping temporal table: #PO_AGG')
        self.execute(query='DROP TABLE IF EXISTS #PO_AGG')
        return
    
    def validate_table_exists(self, table_name):
        # Validate if table exists, if not catch exception
        try:
            self.select(query=f'SELECT 1 FROM {table_name} LIMIT 1')
            print('Table exists')
            return True
        except Exception as e:
            print('Table does not exist')
            return False
    
    # Override the close method to drop the temporal tables
    def close(self):
        self.drop_temporal_tables()
        super().close()
        return

    # Query para obtener los datos entre comillas simples y separados por coma
    @staticmethod
    def add_quotes(text):
        lis = []
        for item in text.split(','):
            item = f"'{item}'"
            lis.append(item)
        return (',').join(lis) if text else ''
    
    # Query para inicializar menu de usuario para obtener datos de marcas
    def menu_ingresa_productos(self):
        # Setear variables
        self.initialize_variables_products()

        # Ingresar variables por usuario
        while True:
            op = input('Cómo desea definir los productos?\n\n1. Ingresar por skus\n2. Ingresar por Marcas\n3. Ingresar por Proveedor\n\nIngrese una opción: ')

            if op == '1':
                skus = input('Ingrese los skus separados por coma: ').replace(', ', '')
                self.skus = skus

            elif op == '2':
                marcas = input('Ingrese las Marcas separadas por coma: ').replace(', ', '')
                self.marcas = self.add_quotes(marcas)

            elif op == '3':
                proveedores = input('Ingrese los Proveedores separados por coma: ').replace(', ', '')
                self.proveedores = self.add_quotes(proveedores)

            else:
                print('Opción no válida')
            
            if op == '1' or op == '2' or op == '3':
                break
        
        # Preguntar si desea filtrar por clase, subclase y tipo de producto
        if input('¿Desea filtrar por clase, subclase y tipo de producto? (y/n): ') == 'y':
            self.menu_clases()
        return

    # Menu para filtrar productos por clase, subclase y tipo de producto
    def menu_clases(self):
        # Seleccionar distintas class_desc, subclass_desc, prod_type_desc
        df_productos = self.select(query=self.get_query_productos())

        clases = df_productos['class_desc'].unique()
        subclases = df_productos['subclass_desc'].unique()
        prod_type_desc = df_productos['prod_type_desc'].unique()
        
        print('\n')
        print('Clases:', ', '.join(clases))
        print('Sub-Clases:', ', '.join(subclases))
        print('Tipo:', ', '.join(prod_type_desc))
        print('\n')

        # Inicializar menu de usuario para ingresar clases. Repetir hasta que el usuario ingrese valores dentro de las variables clases, subclases, prod_type_desc
        # Ingresar variables por usuario

        keep = True
        while keep:    
            op = input('¿Desea ingresar clases? (y/n): ')
            if op == 'y':
                clases = input('Ingrese clases: ').replace(', ', ',')
                self.clases = self.add_quotes(clases)
                keep = False
            elif op == 'n':
                keep = False
        
        keep = True
        while keep:
            op = input('¿Desea ingresar subclases? (y/n): ')
            if op == 'y':
                subclases = input('Ingrese subclases: ').replace(', ', ',')
                self.subclases = self.add_quotes(subclases)
                keep = False
            elif op == 'n':
                keep = False

        keep = True
        while keep:
            op = input('¿Desea ingresar tipo? (y/n): ')
            if op == 'y':
                prod_type_desc = input('Ingrese tipos: ').replace(', ', ',')
                self.prod_type_desc = self.add_quotes(prod_type_desc)
                keep = False
            elif op == 'n':
                keep = False

        print('Clases definidas')
        return

    # Construir el query de productos con los filtros ingresados. Cada parametro contiene las variabels separadas por coma.
    # En caso de que los parametros estén vacíos, se toman los valores de la clase
    def get_query_productos(self):
        query_productos = \
        f'''
            SELECT
                A.CLASS_CODE
                ,A.CLASS_DESC
                ,A.SUBCLASS_CODE
                ,A.SUBCLASS_DESC
                ,A.PROD_TYPE_DESC
                ,A.PRODUCT_CODE
                ,A.PRODUCT_DESCRIPTION
                ,B.MARCA
                ,B.PROVEEDOR
            FROM DIM_PRODUCT A
            INNER JOIN CHEDRAUI.MON_CRM_SKU_RAZONSOCIAL B ON A.PRODUCT_CODE = B.PRODUCT_CODE
            {f'AND A.PRODUCT_CODE::BIGINT IN ({self.skus})' if self.skus else ''}
            {f'AND B.MARCA IN ({self.marcas})' if self.marcas else ''}
            {f'AND B.PROVEEDOR = ({self.proveedores})' if self.proveedores else ''}
            {f'AND A.CLASS_DESC IN ({self.clases})' if self.clases else ''}
            {f'AND A.SUBCLASS_DESC IN ({self.subclases})' if self.subclases else ''}
            {f'AND A.PROD_TYPE_DESC IN ({self.prod_type_desc})' if self.prod_type_desc else ''}
        '''
        return query_productos

    # Query para obtener los datos de Públicos Objetivos
    def get_query_create_productos_temporal(self):
        # Resta un mes a la fecha actual y luego usa MonthEnd para obtener el último día de ese mes
        # Resta 12 meses a la fecha final para obtener la fecha inicial más 1 día
        val_end = (datetime.now() - pd.DateOffset(months=1) + MonthEnd(1)).strftime('%Y-%m-%d')
        val_ini = (pd.to_datetime(val_end) - pd.DateOffset(months=12) + pd.DateOffset(days=1)).strftime('%Y-%m-%d')
        val_dash = f"'{val_ini}' AND '{val_end}'"

        query_productos_temporal = \
        f'''
        CREATE TABLE #PRODUCTOS AS (
            WITH __PROD_MARCA AS (
            SELECT
            A.CLASS_CODE
            ,A.CLASS_DESC
            ,A.SUBCLASS_CODE
            ,A.SUBCLASS_DESC
            ,A.PROD_TYPE_DESC
            ,A.PRODUCT_CODE
            ,A.PRODUCT_DESCRIPTION
            ,B.MARCA
            ,B.PROVEEDOR
            FROM DIM_PRODUCT A
            INNER JOIN CHEDRAUI.MON_CRM_SKU_RAZONSOCIAL B ON A.PRODUCT_CODE = B.PRODUCT_CODE
            {f'AND A.PRODUCT_CODE::BIGINT IN ({self.skus})' if self.skus else ''}
            {f'AND B.MARCA IN ({self.marcas})' if self.marcas else ''}
            {f'AND B.PROVEEDOR = ({self.proveedores})' if self.proveedores else ''}
            {f'AND A.CLASS_DESC IN ({self.clases})' if self.clases else ''}
            {f'AND A.SUBCLASS_DESC IN ({self.subclases})' if self.subclases else ''}
            {f'AND A.PROD_TYPE_DESC IN ({self.prod_type_desc})' if self.prod_type_desc else ''}
            )
            ,__PRODUCTOS_CATEGORIA AS (
            SELECT
                A.CLASS_CODE
                ,A.CLASS_DESC
                ,A.SUBCLASS_CODE
                ,A.SUBCLASS_DESC
                ,A.PROD_TYPE_DESC
                ,A.PRODUCT_CODE
                ,A.PRODUCT_BRAND_VALUE
                ,A.PROD_ATTRIB15
                ,A.COMMERCIALB_DESC
                ,A.PRODUCT_DESCRIPTION
                
                ,CASE WHEN PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END IND_MARCA
                ,MARCA
                ,PROVEEDOR
        
            FROM DIM_PRODUCT A
            LEFT JOIN __PROD_MARCA USING(PRODUCT_CODE)
            WHERE A.SUBCLASS_DESC IN (SELECT DISTINCT SUBCLASS_DESC FROM __PROD_MARCA WHERE CLASS_CODE !~'ND')
            AND A.PROD_TYPE_DESC IN (SELECT DISTINCT PROD_TYPE_DESC FROM __PROD_MARCA WHERE CLASS_CODE !~'ND')
            AND A.PRODUCT_CODE IN (SELECT DISTINCT PRODUCT_CODE FROM FCT_SALE_LINE WHERE INVOICE_DATE BETWEEN {val_dash})
            )
            ,__PRODUCTOS AS (
            SELECT
            A.CLASS_CODE
            ,A.CLASS_DESC
            ,A.SUBCLASS_CODE
            ,A.SUBCLASS_DESC
            ,A.PROD_TYPE_DESC
            ,A.PRODUCT_CODE
            ,A.PRODUCT_BRAND_VALUE
            ,A.PROD_ATTRIB15
            ,A.COMMERCIALB_DESC
            ,A.PRODUCT_DESCRIPTION
            
            ,CASE WHEN UPPER(A.PRODUCT_DESCRIPTION) ~ '(\\d+(\\.\\d+)?\\s?(KG|G|ML|MILI|M L|M\\b|L|LT)|KG)' THEN 1 ELSE 0 END AS MEDIBLE
            ,CASE
                WHEN UPPER(A.PRODUCT_DESCRIPTION) ~ '\\d+(\\.\\d+)?\\s?(L|LT)' THEN 'L'
                WHEN UPPER(A.PRODUCT_DESCRIPTION) ~ '\\d+(\\.\\d+)?\\s?(ML|MILI|M L|M\\b)' THEN 'L'--'ML'
                WHEN UPPER(A.PRODUCT_DESCRIPTION) ~ '\\d+(\\.\\d+)?\\s?KG| KG' THEN 'KG'
                WHEN UPPER(A.PRODUCT_DESCRIPTION) ~ '\\d+(\\.\\d+)?\\s?G' THEN 'KG'--'G'
            END AS MEDIDA
            ,CASE
                WHEN UPPER(A.PRODUCT_DESCRIPTION) ~ '\\d+(\\.\\d+)?\\s?(L|LT)'        THEN REGEXP_SUBSTR(REGEXP_SUBSTR(UPPER(A.PRODUCT_DESCRIPTION), '\\d+(\\.\\d+)?\\s?(L|LT)'), '\\d+(\\.\\d+)?')::REAL
                WHEN UPPER(A.PRODUCT_DESCRIPTION) ~ '\\d+(\\.\\d+)?\\s?(ML|MILI|M L|M\\b)' THEN REGEXP_SUBSTR(REGEXP_SUBSTR(UPPER(A.PRODUCT_DESCRIPTION), '\\d+(\\.\\d+)?\\s?(ML|MILI|M L|M\\b)'), '\\d+(\\.\\d+)?')::REAL / 1000
                WHEN UPPER(A.PRODUCT_DESCRIPTION) ~ '\\d+(\\.\\d+)?\\s?KG'            THEN REGEXP_SUBSTR(REGEXP_SUBSTR(UPPER(A.PRODUCT_DESCRIPTION), '\\d+(\\.\\d+)?\\s?KG'), '\\d+(\\.\\d+)?')::REAL
                WHEN UPPER(A.PRODUCT_DESCRIPTION) ~ '\\d+(\\.\\d+)?\\s?G'             THEN REGEXP_SUBSTR(REGEXP_SUBSTR(UPPER(A.PRODUCT_DESCRIPTION), '\\d+(\\.\\d+)?\\s?G'), '\\d+(\\.\\d+)?')::REAL / 1000
                WHEN UPPER(A.PRODUCT_DESCRIPTION) ~ ' KG' THEN 1::REAL
            END AS VALOR
            ,CASE
                WHEN UPPER(A.PRODUCT_DESCRIPTION) ~ '\\d+\\s?([Pp][Zz])' THEN REGEXP_SUBSTR(REGEXP_SUBSTR(UPPER(A.PRODUCT_DESCRIPTION), '\\d+\\s?([Pp][Zz])'), '\\d+')::REAL
                ELSE 1
            END AS PZ

            ,A.IND_MARCA
            ,COALESCE(A.MARCA, B.MARCA) MARCA
            ,COALESCE(A.PROVEEDOR, B.PROVEEDOR) PROVEEDOR
            ,ROW_NUMBER() OVER(PARTITION BY PRODUCT_CODE ORDER BY PRODUCT_CODE) - 1 IND_DUPLICADO
            FROM __PRODUCTOS_CATEGORIA A
            LEFT JOIN (SELECT DISTINCT PROVEEDOR, MARCA, SUBCLASS_CODE, PROD_TYPE_DESC FROM __PRODUCTOS_CATEGORIA WHERE IND_MARCA = 1) B ON A.SUBCLASS_CODE = B.SUBCLASS_CODE AND A.PROD_TYPE_DESC = B.PROD_TYPE_DESC AND A.IND_MARCA = 0
            WHERE COALESCE(A.MARCA, B.MARCA) IS NOT NULL
            )
            SELECT * FROM __PRODUCTOS
        )
        '''
        return query_productos_temporal

    def create_table_productos_temporal(self):
        table_name = '#PRODUCTOS'
        if not self.validate_table_exists(table_name):
            self.execute(query=self.get_query_create_productos_temporal())
        return

    def get_query_select_productos_temporal(self):
        query_select_productos_temporal = f'''
            SELECT * FROM #PRODUCTOS
        '''
        return query_select_productos_temporal
    
    def get_query_create_pos_temporal(self):
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

        query_pos_temporal = f'''
        DROP TABLE IF EXISTS #PO;
        CREATE TABLE #PO AS (
            WITH
            __PROD_MARCA AS (
                SELECT
                PRODUCT_CODE
                FROM #PRODUCTOS
                WHERE IND_MARCA = 1
            )
            ,__PROD_COM AS (
                SELECT
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
                LEFT JOIN CHEDRAUI.V_CUSTOMER_CONTACT B ON A.CUSTOMER_CODE_TY = B.CUSTOMER_CODE
            --     INNER JOIN CHEDRAUI.V_STORE E ON B.STORE_KEY = E.STORE_KEY AND B.STORE_CODE = E.STORE_CODE --PARA TRAER TIENDA FAV DEL CLIENTE
            --     INNER JOIN CHEDRAUI.V_STORE C ON A.STORE_KEY = C.STORE_KEY AND A.STORE_CODE = C.STORE_CODE --PARA TRAER TIENDA DONDE SE REALIZÓ LA COMPRA
            --     INNER JOIN #TX_ELEGIBLES F ON A.CUSTOMER_CODE_TY = F.CUSTOMER_CODE_TY AND F.IND_ELEGIBLE = 1 --DESCOMENTAR PARA ELEGIR CLIENTES QUE CUMPLEN LA CONDICIÓN DE COMPRA AL MENOS UNA VEZ
                WHERE LEFT(A.INVOICE_DATE, 7) BETWEEN '{campana_ini_12}' AND '{campana_fin}'
                AND B.CONTACT_INFO IS NOT NULL
            --  AND C.STORE_FORMAT2 NOT IN ('08 SUPERCITO CD' ,'09 SUPERCITO SELECTO','07 SUPER CHE CD')
                AND (A.PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA)
                OR A.PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_COM))
                AND BUSINESS_TYPE = 'R'
                AND SALE_NET_VAL > 0
            --     AND C.STORE_CODE IN ('0010','0097','0061','0252','0135','0093','0043','0066','0016','0027','0602','0676','0240','0237','0111','0239','0100','0233','0117','0234')
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
        )
        '''
        return query_pos_temporal
    
    def get_query_create_pos_agg_temporal(self):
        query_pos_agg_temporal = f'''
            -- Agrupado
            DROP TABLE IF EXISTS #PO_AGG;
            CREATE TABLE #PO_AGG AS (
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
                FROM #PO
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
                FROM #PO
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
                FROM #PO
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
                FROM #PO
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
                FROM #PO
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
                FROM #PO
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
                FROM #PO
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
                FROM #PO
                GROUP BY 1, 2, 3, 4
                )
                SELECT
                *
                FROM __AGG_PO
                ORDER BY 1, 2, 3, 4
                )
            '''
        return query_pos_agg_temporal

    def create_table_pos_temporal(self):
        table_name = '#PO'
        if not self.validate_table_exists(table_name):
            self.execute(query=self.get_query_create_pos_temporal())
        
        table_name = '#PO_AGG'
        if not self.validate_table_exists(table_name):
            self.execute(query=self.get_query_create_pos_agg_temporal())
        return

    def get_query_select_pos_agg_temporal(self):
        query_select_pos_temporal = f'''
            SELECT * FROM #PO_AGG ORDER BY 1,2,3,4
        '''
        return query_select_pos_temporal

    # Query para obtener los datos de Públicos Objetivos
    # La tabla temporal de productos debe estar creada ya. FALTA VALIDAR QUE LA TABLA TEMPORAL YA ESTE CREADA
    def get_publicos_objetivos(self):
        # Ingresar periodo de campaña
        self.inicio = input('Ingrese la fecha de inicio de la campaña (yyyy-mm-dd): ')
        self.termino = input('Ingrese la fecha de fin de la campaña (yyyy-mm-dd): ')

        # Crear tabla temporal POs
        self.create_table_pos_temporal()

        # Obtain data
        df_pos_agg = self.select(query=self.get_query_select_pos_agg_temporal())
        print(df_pos_agg)

        return df_pos_agg

    def save_into_csv(self, df, file_name):
        try:
            df.to_csv(file_name, index=False)
            print('Archivo guardado con éxito')
        except:
            print('Error al guardar archivo. Intente cerrarlo primero.')

    def start_program(self):
        # Crear un menu para el usuario
        # 1. Ingresar productos
        # 2. Obtener POs
        # 3. Ver productos ingresados
        # 4. Salir

        ## Start Programm
        keep = True
        while keep:
            print('\n1. Ingresar Productos')
            print('2. Generar Públicos Objetivos')
            print('3. Ver/Guardar Productos ingresados')
            print('4. Ver/Guardar Públicos Objetivos')
            print('5. Salir')
            option = input('\nIngrese una opción: ')

            # Iniciar Menu para definir productos de interes. marca, proveedor, class, subclass, prod_type_desc
            if option == '1':
                print('\nIngresar productos\n')
                self.menu_ingresa_productos()
                
                # Create temporal table Productos, marca y competencia
                self.create_table_productos_temporal()

            # Obtener Públicos Objetivos
            elif option == '2':
                print('\nObtener POs\n')
                df_pos_agg = self.get_publicos_objetivos()

            elif option == '3':
                print('\nVer productos ingresados\n')
                if self.validate_table_exists('#PRODUCTOS'):
                    df_productos = self.select(query=self.get_query_select_productos_temporal())
                    print(df_productos)

                    # Save into csv
                    if input('\nQuiere guardar la información en un archivo csv? (y/n) ') == 'y':
                        self.save_into_csv(df_productos, '.\productos.csv')
                    
                else:
                    print('\nNo hay productos ingresados')

            elif option == '4':
                print('\nVer Públicos Objetivos\n')
                if self.validate_table_exists('#PO_AGG'):
                    df_pos_agg = self.select(query=self.get_query_select_pos_agg_temporal())
                    print(df_pos_agg)

                    # Save into csv
                    if input('\nQuiere guardar la información en un archivo csv? (y/n) ') == 'y':
                        self.save_into_csv(df_pos_agg, '.\pos_agg.csv')
                else:
                    print('\nNo ha generado POs aún. Por favor, ingrese productos y Genere POs.')

            elif option == '5':
                print('\nSalir\n')
                keep = False
            else:
                print('Opción no válida')

        # # Cerrar conexión
        self.close()
        return