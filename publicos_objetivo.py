import pandas as pd

# Create a Class to handle the Monetizacion data that inherits from the Conn class
class PublicosObjetivo():
    
    def __init__(self):
        self.df_pos_agg = pd.DataFrame()
        self.df_bc_tx = pd.DataFrame()
        self.df_bc_unidades = pd.DataFrame()
        self.df_bc_tx_medio = pd.DataFrame()
        self.df_listas_envio = pd.DataFrame()
        self.set_pos_variables()
    
    def set_pos_variables(self, tiendas='', is_online=0, condicion=0, inicio='', termino=''):
        self.tiendas = tiendas
        self.is_online = is_online
        self.condicion = condicion
        self.inicio = inicio
        self.termino = termino
        self.__get_fechas_campana()

    def set_po_envios_variables(self, condicion, excluir):
        self.condicion = condicion
        self.excluir = excluir
    
    def set_po_filtros_variables(self, venta_antes, venta_camp, cond_antes, cond_camp):
        self.venta_antes = venta_antes
        self.venta_camp = venta_camp
        self.cond_antes = cond_antes
        self.cond_camp = cond_camp

    def set_listas_envio_variables(self, canales, grupo_control):
        self.canales = canales
        self.grupo_control = grupo_control
        # Configurar el ratio de control
        self.ratio_grupo_control = 0.1 if self.grupo_control else 0

    def __get_fechas_campana(self):
        # Crear diccionario de fechas
        self.dict_fechas = {}

        if self.inicio and self.termino:
            # Obtener las fechas de inicio y fin de la campaña quitando el día
            self.dict_fechas['ini'] = self.inicio[:7]
            self.dict_fechas['fin'] = self.termino[:7]

            # Restar 1, 2, 3, 4, 6, 7, 12 meses a la fecha de inicio de la campaña
            self.dict_fechas['ini_1'] = (pd.to_datetime(self.inicio) - pd.DateOffset(months=1)).strftime('%Y-%m-%d')[:7]
            self.dict_fechas['ini_2'] = (pd.to_datetime(self.inicio) - pd.DateOffset(months=2)).strftime('%Y-%m-%d')[:7]
            self.dict_fechas['ini_3'] = (pd.to_datetime(self.inicio) - pd.DateOffset(months=3)).strftime('%Y-%m-%d')[:7]
            self.dict_fechas['ini_4'] = (pd.to_datetime(self.inicio) - pd.DateOffset(months=4)).strftime('%Y-%m-%d')[:7]
            self.dict_fechas['ini_6'] = (pd.to_datetime(self.inicio) - pd.DateOffset(months=6)).strftime('%Y-%m-%d')[:7]
            self.dict_fechas['ini_7'] = (pd.to_datetime(self.inicio) - pd.DateOffset(months=7)).strftime('%Y-%m-%d')[:7]
            self.dict_fechas['ini_12'] = (pd.to_datetime(self.inicio) - pd.DateOffset(months=12)).strftime('%Y-%m-%d')[:7]

    def get_query_create_pos(self, table_name='#PO'):
        # Obtener las fechas de inicio y fin de la campaña quitando el día
        self.__get_fechas_campana()

        if self.condicion:
            query_elegibles = f'''
                DROP TABLE IF EXISTS #TX_ELEGIBLES;
                CREATE TABLE #TX_ELEGIBLES AS (
                    SELECT
                        CUSTOMER_CODE_TY
                        ,INVOICE_NO
                        ,SUM(SALE_NET_VAL) VENTA
                        ,CASE WHEN VENTA >= '{self.condicion}' THEN 1 ELSE 0 END IND_ELEGIBLE
                    FROM FCT_SALE_LINE
                    INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
                    WHERE IND_MARCA = 1
                    AND LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['fin']}'
                    AND BUSINESS_TYPE = 'R'
                    AND SALE_NET_VAL > 0
                    GROUP BY 1,2
                    HAVING IND_ELEGIBLE = 1
                );

                DROP TABLE IF EXISTS #TX_ELEGIBLES_NMC;
                CREATE TABLE #TX_ELEGIBLES_NMC AS (
                    SELECT
                        NULL CUSTOMER_CODE_TY
                        ,INVOICE_NO
                        ,SUM(SALE_NET_VAL) VENTA
                        ,CASE WHEN VENTA >= '{self.condicion}' THEN 1 ELSE 0 END IND_ELEGIBLE
                    FROM FCT_SALE_LINE_NM
                    INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
                    WHERE IND_MARCA = 1
                    AND LEFT(INVOICE_DATE, 7) BETWEEN ''{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['fin']}'
                    AND BUSINESS_TYPE = 'R'
                    AND SALE_NET_VAL > 0
                    GROUP BY 1,2
                    HAVING IND_ELEGIBLE = 1
                );
                
                DROP TABLE IF EXISTS #CLIENTES_ELEGIBLES;
                CREATE TABLE #CLIENTES_ELEGIBLES AS (
                SELECT DISTINCT
                    CUSTOMER_CODE_TY
                FROM #TX_ELEGIBLES
                );
            '''
        else:
            query_elegibles = ''

        query_pos_temporal = query_elegibles + \
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

                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini']}' AND '{self.dict_fechas['fin']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_NATURAL_EVENT

                ,SUM(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini']}' AND '{self.dict_fechas['fin']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN SALE_NET_VAL END) VENTA_NATURAL
                ,COUNT(DISTINCT CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini']}' AND '{self.dict_fechas['fin']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN INVOICE_NO END) TX_NATURAL
                ,SUM(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini']}' AND '{self.dict_fechas['fin']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN SALE_TOT_QTY END) UNIDADES_NATURAL

                ,SUM(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN SALE_NET_VAL END) VENTA
                ,COUNT(DISTINCT CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN INVOICE_NO END) TX
                ,SUM(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN SALE_TOT_QTY END) UNIDADES
                
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_CAP_12_MARCA
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_COM) THEN 1 ELSE 0 END) IND_CAP_12_COM
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_6']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_CAP_6_MARCA
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_6']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_COM) THEN 1 ELSE 0 END) IND_CAP_6_COM
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_3']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_CAP_3_MARCA
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_3']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_COM) THEN 1 ELSE 0 END) IND_CAP_3_COM

                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['ini_7']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_REC_12_6_P1
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_6']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_REC_12_6_P2
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_6']}' AND '{self.dict_fechas['ini_4']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_REC_6_3_P1
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_3']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_REC_6_3_P2

                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_FID_12
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_6']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_FID_6
                ,MAX(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_3']}' AND '{self.dict_fechas['ini_1']}' AND PRODUCT_CODE IN (SELECT PRODUCT_CODE FROM __PROD_MARCA) THEN 1 ELSE 0 END) IND_FID_3

                FROM FCT_SALE_LINE A
                INNER JOIN CHEDRAUI.MON_ACT D USING(CUSTOMER_CODE_TY)
                {f"INNER JOIN (SELECT DISTINCT INVOICE_NO FROM FCT_SALE_HEADER WHERE CHANNEL_TYPE IN ('WEB','APP','CC HY') AND LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['fin']}') USING(INVOICE_NO)" if self.is_online else ''}
                {f'INNER JOIN CHEDRAUI.V_STORE C ON A.STORE_KEY = C.STORE_KEY AND A.STORE_CODE = C.STORE_CODE AND C.STORE_CODE IN ({self.tiendas})' if self.tiendas else ''}
                {f'INNER JOIN #CLIENTES_ELEGIBLES F ON A.CUSTOMER_CODE_TY = F.CUSTOMER_CODE_TY' if self.condicion else ''}
                LEFT JOIN CHEDRAUI.V_CUSTOMER_CONTACT B ON A.CUSTOMER_CODE_TY = B.CUSTOMER_CODE
                WHERE LEFT(A.INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['fin']}'
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
    
    def get_query_create_pos_agg(self, table_name, from_table='#PO'):
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

    def get_query_create_bc_tables(self, from_table='#PO'):
        query_bc_tx = f'''
            --DEFINIR DESCRIPCION DE PO PARA EJECUTAR NUMERO DE TX
            DROP TABLE IF EXISTS #NUM_TX;
            CREATE TABLE #NUM_TX AS (
            SELECT
                'a) CAP_12' PO,
                CASE
                WHEN TX_NATURAL = 1 THEN 'a) 1'
                WHEN TX_NATURAL = 2 THEN 'b) 2'
                WHEN TX_NATURAL = 3 THEN 'c) 3'
                WHEN TX_NATURAL = 4 THEN 'd) 4'
                WHEN TX_NATURAL = 5 THEN 'e) 5'
                WHEN TX_NATURAL = 6 THEN 'f) 6'
                ELSE 'g) +7'
                END TX,
                COUNT(CUSTOMER_CODE) CLIENTES
            FROM {from_table}
            WHERE CAP_12_NATURAL = 1 
            GROUP BY 1,2
            
            UNION

            SELECT
                'b) REC_12_6' PO,
                CASE
                WHEN TX_NATURAL = 1 THEN 'a) 1'
                WHEN TX_NATURAL = 2 THEN 'b) 2'
                WHEN TX_NATURAL = 3 THEN 'c) 3'
                WHEN TX_NATURAL = 4 THEN 'd) 4'
                WHEN TX_NATURAL = 5 THEN 'e) 5'
                WHEN TX_NATURAL = 6 THEN 'f) 6'
                ELSE 'g) +7'
                END TX,
                COUNT(CUSTOMER_CODE) CLIENTES
            FROM {from_table}
            WHERE REC_12_6_NATURAL = 1 
            GROUP BY 1,2
            
            UNION

            SELECT
                'c) FID_6' PO,
                CASE
                WHEN TX_NATURAL = 1 THEN 'a) 1'
                WHEN TX_NATURAL = 2 THEN 'b) 2'
                WHEN TX_NATURAL = 3 THEN 'c) 3'
                WHEN TX_NATURAL = 4 THEN 'd) 4'
                WHEN TX_NATURAL = 5 THEN 'e) 5'
                WHEN TX_NATURAL = 6 THEN 'f) 6'
                ELSE 'g) +7'
                END TX,
                COUNT(CUSTOMER_CODE) CLIENTES
            FROM {from_table}
            WHERE FID_6_NATURAL = 1 
            GROUP BY 1,2
            );
            '''
        
        query_bc_unidades = f'''
            --DEFINIR DESCRIPCION DE PO PARA EJECUTAR NUMERO DE UNIDADES
            DROP TABLE IF EXISTS #NUM_UNIDADES;
            CREATE TABLE #NUM_UNIDADES AS (
            SELECT
                'a) CAP_12' PO,
                CASE
                WHEN UNIDADES_NATURAL = 1 THEN 'a) 1'
                WHEN UNIDADES_NATURAL = 2 THEN 'b) 2'
                WHEN UNIDADES_NATURAL = 3 THEN 'c) 3'
                WHEN UNIDADES_NATURAL = 4 THEN 'd) 4'
                WHEN UNIDADES_NATURAL = 5 THEN 'e) 5'
                WHEN UNIDADES_NATURAL = 6 THEN 'f) 6'
                ELSE 'g) +7'
                END UNIDADES,
                COUNT(CUSTOMER_CODE) CLIENTES
            FROM {from_table}
            WHERE CAP_12_NATURAL = 1 
            GROUP BY 1,2
            
            UNION

            SELECT
                'b) REC_12_6' PO,
                CASE
                WHEN UNIDADES_NATURAL = 1 THEN 'a) 1'
                WHEN UNIDADES_NATURAL = 2 THEN 'b) 2'
                WHEN UNIDADES_NATURAL = 3 THEN 'c) 3'
                WHEN UNIDADES_NATURAL = 4 THEN 'd) 4'
                WHEN UNIDADES_NATURAL = 5 THEN 'e) 5'
                WHEN UNIDADES_NATURAL = 6 THEN 'f) 6'
                ELSE 'g) +7'
                END UNIDADES,
                COUNT(CUSTOMER_CODE) CLIENTES
            FROM {from_table}
            WHERE REC_12_6_NATURAL = 1 
            GROUP BY 1,2
            
            UNION

            SELECT
                'c) FID_6' PO,
                CASE
                WHEN UNIDADES_NATURAL = 1 THEN 'a) 1'
                WHEN UNIDADES_NATURAL = 2 THEN 'b) 2'
                WHEN UNIDADES_NATURAL = 3 THEN 'c) 3'
                WHEN UNIDADES_NATURAL = 4 THEN 'd) 4'
                WHEN UNIDADES_NATURAL = 5 THEN 'e) 5'
                WHEN UNIDADES_NATURAL = 6 THEN 'f) 6'
                ELSE 'g) +7'
                END UNIDADES,
                COUNT(CUSTOMER_CODE) CLIENTES
            FROM {from_table}
            WHERE FID_6_NATURAL = 1 
            GROUP BY 1,2
            );
            '''
        query_bc_tx_medio = f'''
            --DEFINIR TX MEDIO
            DROP TABLE IF EXISTS #TX_MEDIO;
            CREATE TABLE #TX_MEDIO AS (
                WITH __VENTA_MC AS (
                    SELECT
                        IND_MARCA,
                        'MC' TIPO,
                        COUNT(DISTINCT A.CUSTOMER_CODE_TY) CLIENTES,
                        SUM(SALE_NET_VAL) VENTA,
                        COUNT(DISTINCT INVOICE_NO) TX,
                        VENTA / TX TX_MEDIO,
                        VENTA / CLIENTES CONSUMO_MEDIO
                    FROM FCT_SALE_LINE A
                    INNER JOIN #PRODUCTOS B ON A.PRODUCT_CODE = B.PRODUCT_CODE
                    {'INNER JOIN #TX_ELEGIBLES F ON A.INVOICE_NO = F.INVOICE_NO' if self.condicion else ''}
                    WHERE LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini']}' AND '{self.dict_fechas['fin']}'
                    AND SALE_NET_VAL > 0
                    AND BUSINESS_TYPE = 'R'
                    AND IND_DUPLICADO = 0
                    GROUP BY 1,2
                    )
                ,__VENTA_NMC AS (
                    SELECT
                        IND_MARCA,
                        'NMC' TIPO,
                        NULL CLIENTES,
                        SUM(SALE_NET_VAL) VENTA,
                        COUNT(DISTINCT INVOICE_NO) TX,
                        VENTA / TX TX_MEDIO,
                        VENTA / CLIENTES CONSUMO_MEDIO
                    FROM FCT_SALE_LINE_NM A
                    INNER JOIN #PRODUCTOS B ON A.PRODUCT_CODE = B.PRODUCT_CODE
                    {'INNER JOIN #TX_ELEGIBLES F ON A.INVOICE_NO = F.INVOICE_NO' if self.condicion else ''}
                    WHERE LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini']}' AND '{self.dict_fechas['fin']}'
                    AND SALE_NET_VAL > 0
                    AND BUSINESS_TYPE = 'R'
                    AND IND_DUPLICADO = 0
                    GROUP BY 1,2
                )
                SELECT * FROM __VENTA_MC
                UNION
                SELECT * FROM __VENTA_NMC
                ORDER BY 1,2
                );
            '''
        return query_bc_tx, query_bc_unidades, query_bc_tx_medio

    def create_table_pos_temporal(self, conn, override=False):
        table_name_po = '#PO'
        table_name_agg = '#PO_AGG'
        query_po = self.get_query_create_pos(table_name_po)
        query_po_agg = self.get_query_create_pos_agg(table_name_agg, from_table=table_name_po)
        
        # Si override no se especifica, la tabla no existe, se crea
        if override is None:
            conn.execute(query=query_po)
            conn.execute(query=query_po_agg)

        # Si override es True, se sobreescribe la tabla
        elif override:
            conn.override_table(table_name_po, query_po)
            conn.override_table(table_name_agg, query_po_agg)
        # Si override es False, se espera que la tabla exista, no se hace nada. Salir de la función
        else:
            return
        
        self.df_pos_agg = conn.select(query=f'SELECT * FROM {table_name_agg} ORDER BY 1,2,3,4')
        
    def get_query_create_pos_envios(self, table_name, from_table='#PO'):
        query_po_envios = f'''
            -- Tickets por Cliente
            DROP TABLE IF EXISTS #DATOS_CLIENTES;
            CREATE TABLE #DATOS_CLIENTES AS (
            WITH __TX_ELEGIBLES AS (
                SELECT
                    CUSTOMER_CODE_TY
                    ,INVOICE_NO
                    ,IND_MARCA
                    ,SUM(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['ini_1']}' THEN SALE_NET_VAL END) VENTA
                    ,SUM(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['ini_1']}' THEN SALE_TOT_QTY END) UNIDADES
                    ,SUM(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini']}' AND '{self.dict_fechas['fin']}' THEN SALE_NET_VAL END) VENTA_NATURAL
                    ,SUM(CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini']}' AND '{self.dict_fechas['fin']}' THEN SALE_TOT_QTY END) UNIDADES_NATURAL
                    ,CASE WHEN VENTA >= {self.condicion if self.condicion else 0} THEN 1 ELSE 0 END IND_ELEGIBLE
                    ,CASE WHEN VENTA_NATURAL >= {self.condicion if self.condicion else 0} THEN 1 ELSE 0 END IND_ELEGIBLE_NATURAL
                FROM FCT_SALE_LINE A
                INNER JOIN #PRODUCTOS B ON A.PRODUCT_CODE = B.PRODUCT_CODE
                WHERE LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['fin']}'
                AND BUSINESS_TYPE = 'R'
                AND SALE_NET_VAL > 0
                GROUP BY 1,2,3,INVOICE_DATE
                )
                ,__TX_FECHAS AS (
                SELECT
                    CUSTOMER_CODE_TY
                    ,INVOICE_NO
                    ,IND_MARCA
                    ,VENTA
                    ,VENTA_NATURAL
                    ,UNIDADES
                    ,UNIDADES_NATURAL
                    ,IND_ELEGIBLE
                    ,IND_ELEGIBLE_NATURAL
                    ,INVOICE_DATE
                    ,CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini']}' AND '{self.dict_fechas['fin']}' THEN 1 ELSE 0 END IND_NATURAL
                    ,LEAD(INVOICE_DATE) OVER(PARTITION BY CUSTOMER_CODE_TY ORDER BY INVOICE_DATE) NEXT_INVOICE_DATE
                    ,LEAD(INVOICE_DATE) OVER(PARTITION BY CUSTOMER_CODE_TY, IND_MARCA ORDER BY IND_MARCA, INVOICE_DATE) NEXT_INVOICE_DATE_MARCA
                    ,DATEDIFF(DAYS,INVOICE_DATE,NEXT_INVOICE_DATE)::REAL DIAS_PARA_RECOMPRA
                    ,DATEDIFF(DAYS,INVOICE_DATE,NEXT_INVOICE_DATE_MARCA)::REAL DIAS_PARA_RECOMPRA_MARCA
                FROM FCT_SALE_HEADER A
                INNER JOIN __TX_ELEGIBLES B USING(CUSTOMER_CODE_TY, INVOICE_NO)
                GROUP BY 1,2,3,4,5,6,7,8,9,10
                )
                ,__DATOS_CLIENTES AS (
                SELECT
                CUSTOMER_CODE_TY CUSTOMER_CODE
                ,MAX(CASE WHEN IND_NATURAL = 1 THEN 1 ELSE 0 END) IND_NATURAL

                --FECHAS
                ,MAX(CASE WHEN IND_NATURAL = 0 THEN INVOICE_DATE END) LAST_INVOICE_DATE
                ,MAX(CASE WHEN IND_NATURAL = 1 THEN INVOICE_DATE END) LAST_INVOICE_DATE_NATURAL
                
                ,MAX(CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 1 THEN INVOICE_DATE END) LAST_INVOICE_DATE_MARCA
                ,MAX(CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 0 THEN INVOICE_DATE END) LAST_INVOICE_DATE_COMP
                ,MAX(CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 1 THEN INVOICE_DATE END) LAST_INVOICE_DATE_MARCA_NATURAL
                ,MAX(CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 0 THEN INVOICE_DATE END) LAST_INVOICE_DATE_COMP_NATURAL

                --DIAS CON COMPRA
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 0 THEN INVOICE_DATE END) DIAS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 1 THEN INVOICE_DATE END) DIAS_CON_COMPRA_NATURAL
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 1 THEN INVOICE_DATE END) DIAS_CON_COMPRA_MARCA
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 0 THEN INVOICE_DATE END) DIAS_CON_COMPRA_COMP
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 1 THEN INVOICE_DATE END) DIAS_CON_COMPRA_MARCA_NATURAL
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 0 THEN INVOICE_DATE END) DIAS_CON_COMPRA_COMP_NATURAL
                
                --DIAS CON RECOMPRA
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 0 AND DIAS_PARA_RECOMPRA > 0 THEN INVOICE_DATE END) DIAS_CON_RECOMPRA
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 1 AND DIAS_PARA_RECOMPRA > 0 THEN INVOICE_DATE END) DIAS_CON_RECOMPRA_NATURAL
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 1 AND DIAS_PARA_RECOMPRA_MARCA > 0 THEN INVOICE_DATE END) DIAS_CON_RECOMPRA_MARCA
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 0 AND DIAS_PARA_RECOMPRA_MARCA > 0 THEN INVOICE_DATE END) DIAS_CON_RECOMPRA_COMP
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 1 AND DIAS_PARA_RECOMPRA_MARCA > 0 THEN INVOICE_DATE END) DIAS_CON_RECOMPRA_MARCA_NATURAL
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 0 AND DIAS_PARA_RECOMPRA_MARCA > 0 THEN INVOICE_DATE END) DIAS_CON_RECOMPRA_COMP_NATURAL

                --PROMEDIO DE DIAS PARA RECOMPRA
                ,AVG(CASE WHEN IND_NATURAL = 0 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA
                ,AVG(CASE WHEN IND_NATURAL = 1 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA_NATURAL
                ,AVG(CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 1 THEN DIAS_PARA_RECOMPRA_MARCA END) PROMEDIO_DIAS_RECOMPRA_MARCA
                ,AVG(CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 0 THEN DIAS_PARA_RECOMPRA_MARCA END) PROMEDIO_DIAS_RECOMPRA_COMP
                ,AVG(CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 1 THEN DIAS_PARA_RECOMPRA_MARCA END) PROMEDIO_DIAS_RECOMPRA_MARCA_NATURAL
                ,AVG(CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 0 THEN DIAS_PARA_RECOMPRA_MARCA END) PROMEDIO_DIAS_RECOMPRA_COMP_NATURAL

                --STD DE D�AS PARA RECOMPRA
                ,STDDEV_SAMP(CASE WHEN IND_NATURAL = 0 THEN DIAS_PARA_RECOMPRA END) STD_DIAS_RECOMPRA
                ,STDDEV_SAMP(CASE WHEN IND_NATURAL = 1 THEN DIAS_PARA_RECOMPRA END) STD_DIAS_RECOMPRA_NATURAL
                ,STDDEV_SAMP(CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 1 THEN DIAS_PARA_RECOMPRA_MARCA END) STD_DIAS_RECOMPRA_MARCA
                ,STDDEV_SAMP(CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 0 THEN DIAS_PARA_RECOMPRA_MARCA END) STD_DIAS_RECOMPRA_COMP
                ,STDDEV_SAMP(CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 1 THEN DIAS_PARA_RECOMPRA_MARCA END) STD_DIAS_RECOMPRA_MARCA_NATURAL
                ,STDDEV_SAMP(CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 0 THEN DIAS_PARA_RECOMPRA_MARCA END) STD_DIAS_RECOMPRA_COMP_NATURAL

                --VENTA, TX Y UNIDADES
                ,SUM(CASE WHEN IND_NATURAL = 0 THEN VENTA END) VENTA_CAT
                ,SUM(CASE WHEN IND_NATURAL = 1 THEN VENTA END) VENTA_CAT_NATURAL
                ,SUM(CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 1 THEN VENTA END) VENTA_MARCA
                ,SUM(CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 0 THEN VENTA END) VENTA_COMP
                ,SUM(CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 1 THEN VENTA_NATURAL END) VENTA_MARCA_NATURAL
                ,SUM(CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 0 THEN VENTA_NATURAL END) VENTA_COMP_NATURAL

                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_MARCA
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 0 THEN INVOICE_NO END) TX_COMP
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_MARCA_NATURAL
                ,COUNT(DISTINCT CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 0 THEN INVOICE_NO END) TX_COMP_NATURAL

                ,SUM(CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 1 THEN UNIDADES END) UNIDADES_MARCA
                ,SUM(CASE WHEN IND_NATURAL = 0 AND IND_MARCA = 0 THEN UNIDADES END) UNIDADES_COMP
                ,SUM(CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 1 THEN UNIDADES_NATURAL END) UNIDADES_MARCA_NATURAL
                ,SUM(CASE WHEN IND_NATURAL = 1 AND IND_MARCA = 0 THEN UNIDADES_NATURAL END) UNIDADES_COMP_NATURAL

                --TX ELEGIBLES
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) TX_ELEGIBLES_MARCA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 0 AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) TX_ELEGIBLES_COMP
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ELEGIBLE_NATURAL = 1 THEN INVOICE_NO END) TX_ELEGIBLES_NATURAL_MARCA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 0 AND IND_ELEGIBLE_NATURAL = 1 THEN INVOICE_NO END) TX_ELEGIBLES_NATURAL_COMP
                
                --UNIDADES POR TX Y VENTA POR TX
                ,UNIDADES_MARCA / TX_MARCA UNIDADES_TX_MARCA
                ,UNIDADES_COMP / TX_COMP UNIDADES_TX_COMP
                ,UNIDADES_MARCA_NATURAL / TX_MARCA_NATURAL UNIDADES_TX_MARCA_NATURAL
                ,UNIDADES_COMP_NATURAL / TX_COMP_NATURAL UNIDADES_TX_COMP_NATURAL

                ,VENTA_MARCA / TX_MARCA TX_PROMEDIO_MARCA
                ,VENTA_COMP / TX_COMP TX_PROMEDIO_COMP
                ,VENTA_MARCA_NATURAL / TX_MARCA_NATURAL TX_PROMEDIO_MARCA_NATURAL
                ,VENTA_COMP_NATURAL / TX_COMP_NATURAL TX_PROMEDIO_COMP_NATURAL
                
                FROM __TX_FECHAS
                GROUP BY 1
            )
            
            SELECT * FROM __DATOS_CLIENTES
            );

            --Lista de envios
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} AS (
            WITH __EX_CLIENTES AS (
                SELECT DISTINCT
                CUSTOMER_CODE
                FROM MAP_CUST_LIST
                WHERE LIST_ID IN ({self.excluir if self.excluir else '0'})
                --AND OPEN_CUSTOMER_CODE IS NOT NULL
            )
            ,__CUSTOMER_LIST AS (
                SELECT
            --       INDICADORES DE FILTRADO POR SEGMENTO
                CASE WHEN FID_6 = 1 THEN 1 ELSE 0 END IND_FID
                ,CASE WHEN REC_12_6 = 1 THEN 1 ELSE 0 END IND_REC
                ,CASE WHEN CAP_12 = 1 THEN 1 ELSE 0 END IND_CAP

                ,CASE
                    WHEN IND_FID = 1 THEN '1 FID'
                    WHEN IND_REC = 1 THEN '2 REC'
                    WHEN IND_CAP = 1 THEN '3 CAP'
                    END ORDEN_SEGMENTO

            --       DATOS CLIENTE
                ,CUSTOMER_CODE
                ,VALID_CONTACT_INFO
                ,NSE
                ,TIPO_FAMILIA

                ,IND_NATURAL
                ,VENTA_NATURAL
                ,TX_NATURAL
                ,UNIDADES_NATURAL  
                ,VENTA
                ,TX
                ,UNIDADES
                
                --FECHAS
                ,LAST_INVOICE_DATE
                ,LAST_INVOICE_DATE_NATURAL
                
                ,LAST_INVOICE_DATE_MARCA
                ,LAST_INVOICE_DATE_COMP
                ,LAST_INVOICE_DATE_MARCA_NATURAL
                ,LAST_INVOICE_DATE_COMP_NATURAL

                --DIAS CON COMPRA
                ,DIAS_CON_COMPRA
                ,DIAS_CON_COMPRA_NATURAL
                ,DIAS_CON_COMPRA_MARCA
                ,DIAS_CON_COMPRA_COMP
                ,DIAS_CON_COMPRA_MARCA_NATURAL
                ,DIAS_CON_COMPRA_COMP_NATURAL
                
                --DIAS CON RECOMPRA
                ,DIAS_CON_RECOMPRA
                ,DIAS_CON_RECOMPRA_NATURAL
                ,DIAS_CON_RECOMPRA_MARCA
                ,DIAS_CON_RECOMPRA_COMP
                ,DIAS_CON_RECOMPRA_MARCA_NATURAL
                ,DIAS_CON_RECOMPRA_COMP_NATURAL

                --PROMEDIO DE DIAS PARA RECOMPRA
                ,PROMEDIO_DIAS_RECOMPRA
                ,PROMEDIO_DIAS_RECOMPRA_NATURAL
                ,PROMEDIO_DIAS_RECOMPRA_MARCA
                ,PROMEDIO_DIAS_RECOMPRA_COMP
                ,PROMEDIO_DIAS_RECOMPRA_MARCA_NATURAL
                ,PROMEDIO_DIAS_RECOMPRA_COMP_NATURAL

                --STD DE DIAS PARA RECOMPRA
                ,STD_DIAS_RECOMPRA
                ,STD_DIAS_RECOMPRA_NATURAL
                ,STD_DIAS_RECOMPRA_MARCA
                ,STD_DIAS_RECOMPRA_COMP
                ,STD_DIAS_RECOMPRA_MARCA_NATURAL
                ,STD_DIAS_RECOMPRA_COMP_NATURAL

                --VENTA, TX Y UNIDADES
                ,VENTA_MARCA
                ,VENTA_COMP
                ,VENTA_MARCA_NATURAL
                ,VENTA_COMP_NATURAL
                ,VENTA_CAT

                ,TX_MARCA
                ,TX_COMP
                ,TX_MARCA_NATURAL
                ,TX_COMP_NATURAL

                ,UNIDADES_MARCA
                ,UNIDADES_COMP
                ,UNIDADES_MARCA_NATURAL
                ,UNIDADES_COMP_NATURAL

                --TX ELEGIBLES
                ,TX_ELEGIBLES_MARCA
                ,TX_ELEGIBLES_COMP
                ,TX_ELEGIBLES_NATURAL_MARCA
                ,TX_ELEGIBLES_NATURAL_COMP
                
                --UNIDADES POR TX Y VENTA POR TX
                ,UNIDADES_TX_MARCA
                ,UNIDADES_TX_COMP
                ,UNIDADES_TX_MARCA_NATURAL
                ,UNIDADES_TX_COMP_NATURAL

                ,TX_PROMEDIO_MARCA
                ,TX_PROMEDIO_COMP
                ,TX_PROMEDIO_MARCA_NATURAL
                ,TX_PROMEDIO_COMP_NATURAL

                FROM {from_table}
                LEFT JOIN #DATOS_CLIENTES USING(CUSTOMER_CODE)
                WHERE CUSTOMER_CODE NOT IN (SELECT CUSTOMER_CODE FROM __EX_CLIENTES)
                AND CAP_12_REC_12_FID_6 = 1
            )
            SELECT * FROM __CUSTOMER_LIST
            );
        '''
        return query_po_envios

    def create_tables_bc(self, conn, override=False):
        from_table = '#PO'
        query_bc_tx, query_bc_unidades, query_bc_tx_medio = self.get_query_create_bc_tables(from_table=from_table)
        table_name_tx = '#NUM_TX'
        table_name_unidades = '#NUM_UNIDADES'
        table_name_tx_medio = '#TX_MEDIO'
        
        # Si override no se especifica, la tabla no existe, se crea
        if override is None:
            # Crear tablas BC
            conn.execute(query=query_bc_tx)
            conn.execute(query=query_bc_unidades)
            conn.execute(query=query_bc_tx_medio)

        # Si override es True, se sobreescribe la tabla
        elif override:
            conn.override_table(table_name_tx, query_bc_tx)
            conn.override_table(table_name_unidades, query_bc_unidades)
            conn.override_table(table_name_tx_medio, query_bc_tx_medio)
        # Si override es False, se espera que la tabla exista, no se hace nada. Salir de la función
        else:
            return

        self.df_bc_tx = conn.select(query=f'SELECT * FROM {table_name_tx} ORDER BY 1,2')
        self.df_bc_unidades = conn.select(query=f'SELECT * FROM {table_name_unidades} ORDER BY 1,2')
        self.df_bc_tx_medio = conn.select(query=f'SELECT * FROM {table_name_tx_medio} ORDER BY 1,2')
        
    def create_table_po_envios(self, conn, override=False):
        table_name = '#PO_ENVIOS'
        query = self.get_query_create_pos_envios(table_name, from_table='#PO')
        
        # Si override no se especifica, la tabla no existe, se crea
        if override is None:
            conn.execute(query=query)
        # Si override es True, se sobreescribe la tabla
        elif override:
            conn.override_table(table_name, query)
        # Si override es False, se espera que la tabla exista, no se hace nada. Salir de la función
        else:
            return

    # Funcion para obtener los filtros del query de listas
    def get_filtros_listas(self):
        # Filtro de venta antes
        if self.venta_antes.lower() == 'si':
            venta_antes = 'AND VENTA_MARCA > 0'
        elif self.venta_antes.lower() == 'no':
            venta_antes = 'AND VENTA_MARCA = 0 OR VENTA_MARCA IS NULL'
        else:
            venta_antes = ''

        # Filtro de venta campaña
        if self.venta_camp.lower() == 'si':
            venta_camp = 'AND VENTA_MARCA_NATURAL > 0'
        elif self.venta_camp.lower() == 'no':
            venta_camp = 'AND VENTA_MARCA_NATURAL = 0 OR VENTA_MARCA_NATURAL IS NULL'
        else:
            venta_camp = ''

        # Filtro de condición antes
        if self.cond_antes.lower() == 'si':
            cond_antes = 'AND TX_ELEGIBLES_MARCA > 0'
        elif self.cond_antes.lower() == 'no':
            cond_antes = 'AND TX_ELEGIBLES_MARCA = 0 OR TX_ELEGIBLES_MARCA IS NULL'
        else:
            cond_antes = ''

        # Filtro de condición campaña
        if self.cond_camp.lower() == 'si':
            cond_camp = 'AND TX_ELEGIBLES_NATURAL_MARCA > 0'
        elif self.cond_camp.lower() == 'no':
            cond_camp = 'AND TX_ELEGIBLES_NATURAL_MARCA = 0 OR TX_ELEGIBLES_NATURAL_MARCA IS NULL'
        else:
            cond_camp = ''
        
        return venta_antes, venta_camp, cond_antes, cond_camp

    def get_query_select_po_envios_conteo(self, from_table='#PO_ENVIOS'):
        venta_antes, venta_camp, cond_antes, cond_camp = self.get_filtros_listas()

        query_po_envios = f'''
            SELECT
                *
            FROM (
                SELECT
                    VALID_CONTACT_INFO,
                    CUSTOMER_CODE,
                    ORDEN_SEGMENTO
                FROM {from_table}
                WHERE 1= 1
                {venta_antes}
                {venta_camp}
                {cond_antes}
                {cond_camp}

            ) AS PO_ENVIOS
            PIVOT (
            COUNT(DISTINCT CUSTOMER_CODE)
            FOR ORDEN_SEGMENTO IN ('1 FID', '2 REC', '3 CAP')
            ) AS PIVOT_TABLE
            ORDER BY 1;
        '''
        return query_po_envios
        
    def get_query_create_listas_envio(self, table_name, from_table):
        venta_antes, venta_camp, cond_antes, cond_camp = self.get_filtros_listas()
        
        # Extraer canales y numero de envios

        print('canales:')
        print(self.canales)

        fid_sms = self.canales['entry_fid_sms']
        fid_email = self.canales['entry_fid_mail']
        fid_sms_mail = self.canales['entry_fid_sms&mail']
        
        rec_sms = self.canales['entry_rec_sms']
        rec_email = self.canales['entry_rec_mail']
        rec_sms_mail = self.canales['entry_rec_sms&mail']

        cap_sms = self.canales['entry_cap_sms']
        cap_email = self.canales['entry_cap_mail']
        cap_sms_mail = self.canales['entry_cap_sms&mail']
        
        orden_venta = 'VENTA_MARCA' if any([self.venta_antes, self.venta_camp, self.cond_antes, self.cond_camp]) else 'VENTA_CAT'
        porcentaje_gt = 1 - self.ratio_grupo_control

        query = f'''
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} AS (
                WITH __PO_ENVIOS AS (
                    SELECT
                        ROW_NUMBER() OVER(PARTITION BY ORDEN_SEGMENTO, VALID_CONTACT_INFO ORDER BY ORDEN_SEGMENTO, VALID_CONTACT_INFO, {orden_venta} DESC, CUSTOMER_CODE) ROW_N
                        ,*
                    FROM {from_table}
                )
                SELECT * FROM __PO_ENVIOS

                WHERE VALID_CONTACT_INFO IN ('01 SMS', '02 MAIL', '03 MAIL & SMS', '04 INVALID CONTACT') --('01 SMS', '02 MAIL', '03 MAIL & SMS', '04 INVALID CONTACT')
                AND (IND_FID = 1 OR IND_REC = 1 OR IND_CAP = 1) --(IND_FID = 1 OR IND_REC = 1 OR IND_CAP = 1)

                AND (
                    --FID
                    (IND_FID = 1 AND VALID_CONTACT_INFO = '01 SMS'                AND ROW_N <= {fid_sms}/{porcentaje_gt})
                    OR (IND_FID = 1 AND VALID_CONTACT_INFO = '02 MAIL'            AND ROW_N <= {fid_email}/{porcentaje_gt})
                    OR (IND_FID = 1 AND VALID_CONTACT_INFO = '03 MAIL & SMS'      AND ROW_N <= {fid_sms_mail}/{porcentaje_gt})
                    -- OR (IND_FID = 1 AND VALID_CONTACT_INFO = '04 INVALID CONTACT' AND ROW_N <= 0/{porcentaje_gt})

                    --REC
                    OR (IND_REC = 1 AND VALID_CONTACT_INFO = '01 SMS'             AND ROW_N <= {rec_sms}/{porcentaje_gt})
                    OR (IND_REC = 1 AND VALID_CONTACT_INFO = '02 MAIL'            AND ROW_N <= {rec_email}/{porcentaje_gt})
                    OR (IND_REC = 1 AND VALID_CONTACT_INFO = '03 MAIL & SMS'      AND ROW_N <= {rec_sms_mail}/{porcentaje_gt})
                    -- OR (IND_REC = 1 AND VALID_CONTACT_INFO = '04 INVALID CONTACT' AND ROW_N <= 0/{porcentaje_gt})

                    --CAP
                    OR (IND_CAP = 1 AND VALID_CONTACT_INFO = '01 SMS'             AND ROW_N <= {cap_sms}/{porcentaje_gt})
                    OR (IND_CAP = 1 AND VALID_CONTACT_INFO = '02 MAIL'            AND ROW_N <= {cap_email}/{porcentaje_gt})
                    OR (IND_CAP = 1 AND VALID_CONTACT_INFO = '03 MAIL & SMS'      AND ROW_N <= {cap_sms_mail}/{porcentaje_gt})
                    -- OR (IND_CAP = 1 AND VALID_CONTACT_INFO = '04 INVALID CONTACT' AND ROW_N <= 0/{porcentaje_gt})
                )
                
                {venta_antes}
                {venta_camp}
                {cond_antes}
                {cond_camp}
                
                ORDER BY ORDEN_SEGMENTO, VALID_CONTACT_INFO, {orden_venta} DESC, CUSTOMER_CODE
            );
        '''
        return query

    def create_table_po_envios_conteo(self, conn):
        from_table = '#PO_ENVIOS'
        query = self.get_query_select_po_envios_conteo(from_table=from_table)
        self.df_po_conteo = conn.select(query=query)
    
    def create_table_listas_envio(self, conn):
        table_name = '#LISTAS_ENVIO'
        from_table = '#PO_ENVIOS'
        query = self.get_query_create_listas_envio(table_name, from_table=from_table)
        print(query)

        conn.execute(query=query)

        orden_venta = 'VENTA_MARCA' if any([self.venta_antes, self.venta_camp, self.cond_antes, self.cond_camp]) else 'VENTA_CAT'

        query = f'SELECT * FROM {table_name} ORDER BY ORDEN_SEGMENTO, VALID_CONTACT_INFO, {orden_venta} DESC, CUSTOMER_CODE'
        self.df_listas_envio = conn.select(query=query)
