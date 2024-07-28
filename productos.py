from datetime import datetime
from pandas.tseries.offsets import MonthEnd
import pandas as pd

# Create a Class to handle the Monetizacion data that inherits from the Conn class
class Productos():
    
    def __init__(self):
        self.df_productos = pd.DataFrame()
        self.set_products(skus='', marcas='', proveedores='', clases='', subclases='', prod_type_desc='')

    def set_products(self, skus, marcas, proveedores, clases, subclases, prod_type_desc):
        self.skus = skus
        self.marcas = marcas
        self.proveedores = proveedores
        self.clases = clases
        self.subclases = subclases
        self.prod_type_desc = prod_type_desc

    def create_tabla_productos(self, conn, override=None):
        
        #Tabla temporal para productos
        table_name = '#PRODUCTOS'
        query = self.get_query_create_productos_temporal(table_name)

        # Si override no se especifica, la tabla no existe, se crea
        if override is None:
            conn.execute(query=query) 
        
        # Si override es Si, se sobreescribe la tabla
        elif override:
            conn.override_table(table_name, query)
        # Si override es No, se espera que la tabla exista, no se hace nada. Salir de la función
        else:
            return
        
        self.df_productos = conn.select(query=f'SELECT * FROM {table_name}')

    def get_productos(self):
        return self.df_productos

    def get_productos_agg(self):
        columns = ['proveedor', 'marca', 'ind_marca', 'class_desc', 'subclass_desc', 'prod_type_desc']

        return self.df_productos.groupby(columns).agg({'product_code': 'count'}).reset_index().sort_values(by=['ind_marca', 'product_code'], ascending=[False, False])

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
    def get_query_create_productos_temporal(self, table_name):
        # Resta un mes a la fecha actual y luego usa MonthEnd para obtener el último día de ese mes
        # Resta 12 meses a la fecha final para obtener la fecha inicial más 1 día
        val_end = (datetime.now() - pd.DateOffset(months=1) + MonthEnd(1)).strftime('%Y-%m-%d')
        val_ini = (pd.to_datetime(val_end) - pd.DateOffset(months=12) + pd.DateOffset(days=1)).strftime('%Y-%m-%d')
        val_dash = f"'{val_ini}' AND '{val_end}'"

        query_productos_temporal = \
        f'''
        CREATE TABLE {table_name} AS (
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
    
    def get_df_categorias(self, skus, marcas, proveedores):
        query = f'''
            SELECT DISTINCT
                CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
            FROM DIM_PRODUCT A
            INNER JOIN CHEDRAUI.MON_CRM_SKU_RAZONSOCIAL B ON A.PRODUCT_CODE = B.PRODUCT_CODE
            {f'AND A.PRODUCT_CODE::BIGINT IN ({skus})' if skus else ''}
            {f'AND B.MARCA IN ({marcas})' if marcas else ''}
            {f'AND B.PROVEEDOR = ({proveedores})' if proveedores else ''}
        '''
        return self.select(query)