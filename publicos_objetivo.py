import pandas as pd
from tqdm import tqdm

# Create a Class to handle the Monetizacion data that inherits from the Conn class
class PublicosObjetivo():
    
    def __init__(self):
        self.df_pos_agg = pd.DataFrame()
        self.df_bc = pd.DataFrame()
        self.df_listas_envio = pd.DataFrame()
        self.dict_listas_envios = {}
        self.set_pos_variables()
    
    def set_pos_variables(self, tiendas='', excluir='', is_online=0, condicion=0, inicio='', termino=''):
        self.tiendas = tiendas
        self.is_online = is_online
        self.excluir = excluir
        self.condicion = condicion
        self.inicio = inicio
        self.termino = termino
        self.__get_fechas_campana()
    
    def set_po_filtros_variables(self, venta_antes, venta_camp, cond_antes, cond_camp, online):
        self.venta_antes = venta_antes
        self.venta_camp = venta_camp
        self.cond_antes = cond_antes
        self.cond_camp = cond_camp
        self.online = online

    def set_bc_variables(self, fec_ini_campana, fec_fin_campana, fec_fin_analisis, condicion):
        # Seleccionar las fechas y offsets

        ini_campana = fec_ini_campana[:7]
        fin_campana = fec_fin_campana[:7]
        fin_analisis = fec_fin_analisis[:7]
        ini_analisis = (pd.to_datetime(fec_fin_analisis) - pd.DateOffset(months=11)).strftime('%Y-%m-%d')[:7]
        fin_analisis_aa = (pd.to_datetime(fec_fin_analisis) - pd.DateOffset(months=12)).strftime('%Y-%m-%d')[:7]
        ini_analisis_aa = (pd.to_datetime(fec_fin_analisis) - pd.DateOffset(months=23)).strftime('%Y-%m-%d')[:7]

        self.dict_bc_var = {
            'mes_ini_campana': ini_campana
            ,'mes_fin_campana': fin_campana
            ,'mes_fin_analisis': fin_analisis
            ,'mes_ini_analisis': ini_analisis
            ,'mes_fin_analisis_aa': fin_analisis_aa
            ,'mes_ini_analisis_aa': ini_analisis_aa
            ,'date_dash_campana': f"'{ini_campana}' AND '{fin_campana}'"
            ,'date_dash': f"'{ini_analisis}' AND '{fin_analisis}'"
            ,'date_dash_aa': f"'{ini_analisis_aa}' AND '{fin_analisis_aa}'"
            ,'condicion': condicion
            ,'condicion_50': 50
            ,'condicion_75': 75
            ,'condicion_100': 100
            ,'condicion_150': 150
            ,'condicion_200': 200
        }

    def set_listas_envio_variables(self, canales, grupo_control, prioridad_online):
        self.canales = canales
        self.grupo_control = grupo_control
        self.prioridad_online = prioridad_online
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

        query_pos_temporal = \
        f'''
        --CREAR TABLA DE VENTA
        DROP TABLE IF EXISTS #VENTA;
        CREATE TABLE #VENTA AS (
        SELECT
            CUSTOMER_CODE_TY
            ,VALID_CONTACT_INFO
            ,NSE
            ,TIPO_FAMILIA
            ,INVOICE_DATE
            ,LEFT(INVOICE_DATE, 7) MES
        --     ,A.STORE_CODE
            ,INVOICE_NO
            ,IND_MARCA
            ,IND_ONLINE
            ,CASE WHEN SUM(SALE_NET_VAL) >= {self.condicion if self.condicion else 0} THEN 1 ELSE 0 END AS IND_ELEGIBLE
            ,SUM(SALE_NET_VAL) VENTA
            ,SUM(SALE_TOT_QTY) UNIDADES

        FROM FCT_SALE_LINE A
        {f'INNER JOIN DIM_STORE B ON A.STORE_KEY = B.STORE_KEY AND A.STORE_CODE = B.STORE_CODE AND STORE_CODE IN ({self.tiendas})' if self.tiendas else ''}
        INNER JOIN CHEDRAUI.MON_ACT C USING(CUSTOMER_CODE_TY)
        INNER JOIN #PRODUCTOS D USING(PRODUCT_CODE)
        LEFT JOIN (SELECT DISTINCT INVOICE_NO, CASE WHEN CHANNEL_TYPE IN ('WEB','APP','CC HY') THEN 1 ELSE 0 END IND_ONLINE FROM FCT_SALE_HEADER) E USING(INVOICE_NO)
        LEFT JOIN CHEDRAUI.V_CUSTOMER_CONTACT F ON A.CUSTOMER_CODE_TY = F.CUSTOMER_CODE
        WHERE LEFT(A.INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['fin']}'
        AND CUSTOMER_CODE_TY NOT IN (SELECT DISTINCT CUSTOMER_CODE FROM MAP_CUST_LIST WHERE LIST_ID IN (SELECT LIST_ID FROM CHEDRAUI.MON_LISTAS_NEGRAS WHERE STATUS = 'ACTIVO'))
        {f'AND CUSTOMER_CODE_TY NOT IN (SELECT DISTINCT CUSTOMER_CODE FROM MAP_CUST_LIST WHERE LIST_ID IN ({self.excluir}))' if self.excluir else ''}
        AND CONTACT_INFO IS NOT NULL
        AND VALID_CONTACT_INFO <> '04 INVALID CONTACT'
        AND BUSINESS_TYPE = 'R'
        AND SALE_NET_VAL > 0
        AND IND_DUPLICADO = 0
        GROUP BY 1,2,3,4,5,6,7,8,9
        {'HAVING IND_ONLINE = 1' if self.is_online else ''}
        );

        --CALCULAR DIAS DE RECOMPRA POR CLIENTE
        DROP TABLE IF EXISTS #TX_RECOMPRA;
        CREATE TABLE #TX_RECOMPRA AS (
        WITH __TX_DIA AS (
            SELECT
            CUSTOMER_CODE_TY
            ,INVOICE_NO
            ,IND_MARCA
            ,INVOICE_DATE
            ,LEAD(INVOICE_DATE) OVER(PARTITION BY CUSTOMER_CODE_TY ORDER BY INVOICE_DATE) NEXT_INVOICE_DATE
            ,LEAD(INVOICE_DATE) OVER(PARTITION BY CUSTOMER_CODE_TY, IND_MARCA ORDER BY IND_MARCA, INVOICE_DATE) NEXT_INVOICE_DATE_MARCA
            ,DATEDIFF(DAYS,INVOICE_DATE,NEXT_INVOICE_DATE)::REAL DIAS_PARA_RECOMPRA
            ,DATEDIFF(DAYS,INVOICE_DATE,NEXT_INVOICE_DATE_MARCA)::REAL DIAS_PARA_RECOMPRA_MARCA
            FROM FCT_SALE_HEADER
            INNER JOIN (SELECT DISTINCT INVOICE_NO, IND_MARCA FROM #VENTA) USING(INVOICE_NO)
            GROUP BY 1,2,3,4
        )
        SELECT
            CUSTOMER_CODE_TY
            ,MAX(INVOICE_DATE) LAST_INVOICE_DATE
            ,MAX(CASE WHEN IND_MARCA = 1 THEN INVOICE_DATE END) LAST_INVOICE_DATE_MARCA
            ,COUNT(DISTINCT INVOICE_DATE) DIAS_CON_COMPRA
            ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 THEN INVOICE_DATE END) DIAS_CON_COMPRA_MARCA
            ,AVG(DIAS_PARA_RECOMPRA) PROMEDIO_DIAS_RECOMPRA
            ,AVG(CASE WHEN IND_MARCA = 1 THEN DIAS_PARA_RECOMPRA_MARCA END) PROMEDIO_DIAS_RECOMPRA_MARCA
        FROM __TX_DIA
        GROUP BY 1
        );

        --CREAR FECHAS DE MES
        DROP TABLE IF EXISTS #MESES;
        CREATE TABLE #MESES AS (
        WITH __MESES AS (
        SELECT DISTINCT
            LEFT(INVOICE_DATE, 7) MES
            ,EXTRACT(MONTH FROM INVOICE_DATE) MES_NUMERO
            ,CASE WHEN MES BETWEEN '{self.dict_fechas['ini']}' AND '{self.dict_fechas['fin']}' THEN 1 ELSE 0 END IND_MES_CAMPANA
        FROM FCT_SALE_HEADER
        WHERE LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_fechas['ini_12']}' AND '{self.dict_fechas['fin']}'
        )
        SELECT
        *
        ,ROW_NUMBER() OVER(ORDER BY MES) IND_MES
        FROM __MESES
        );

        -- SELECT * FROM #MESES;

        --SEGMENTOS
        --INDICADORES
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} AS (
        WITH __INDICADORES AS (
            SELECT
            --DATOS CLIENTES
            CUSTOMER_CODE_TY
            ,VALID_CONTACT_INFO
            ,NSE
            ,TIPO_FAMILIA
            
            --INDICADORES DE COMPRA
            --MARCA
                --COMPRA EN MES
            ,MAX(CASE WHEN IND_MES = 1 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_1
            ,MAX(CASE WHEN IND_MES = 2 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_2
            ,MAX(CASE WHEN IND_MES = 3 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_3
            ,MAX(CASE WHEN IND_MES = 4 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_4
            ,MAX(CASE WHEN IND_MES = 5 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_5
            ,MAX(CASE WHEN IND_MES = 6 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_6
            ,MAX(CASE WHEN IND_MES = 7 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_7
            ,MAX(CASE WHEN IND_MES = 8 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_8
            ,MAX(CASE WHEN IND_MES = 9 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_9
            ,MAX(CASE WHEN IND_MES = 10 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_10
            ,MAX(CASE WHEN IND_MES = 11 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_11
            ,MAX(CASE WHEN IND_MES = 12 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_12
            ,MAX(CASE WHEN IND_MES_CAMPANA = 0 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_ANTERIOR
            ,MAX(CASE WHEN IND_MES_CAMPANA = 1 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_CAMPANA
            
                --VENTA
            ,SUM(CASE WHEN IND_MES_CAMPANA = 0 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_ANTERIOR
            ,SUM(CASE WHEN IND_MES_CAMPANA = 1 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_CAMPANA

                --TX
            ,COUNT(DISTINCT CASE WHEN IND_MES_CAMPANA = 0 AND IND_MARCA = 1 THEN INVOICE_NO END) TX
            ,COUNT(DISTINCT CASE WHEN IND_MES_CAMPANA = 1 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_CAMPANA

            --UNIDADES
            ,SUM(CASE WHEN IND_MES_CAMPANA = 1 AND IND_MARCA = 1 THEN UNIDADES ELSE 0 END) UNIDADES_CAMPANA

                --VENTA_ELEGIBLE
            ,SUM(CASE WHEN IND_MES_CAMPANA = 0 AND IND_MARCA = 1 AND IND_ELEGIBLE = 1 THEN VENTA ELSE 0 END) VENTA_ELEGIBLE
            ,SUM(CASE WHEN IND_MES_CAMPANA = 1 AND IND_MARCA = 1 AND IND_ELEGIBLE = 1 THEN VENTA ELSE 0 END) VENTA_ELEGIBLE_CAMPANA
            
                --TX_ELEGIBLES
            ,COUNT(DISTINCT CASE WHEN IND_MES_CAMPANA = 0 AND IND_MARCA = 1 AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) TX_ELEGIBLE
            ,COUNT(DISTINCT CASE WHEN IND_MES_CAMPANA = 1 AND IND_MARCA = 1 AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) TX_ELEGIBLE_CAMPANA

            --CAT
                --CLIENTES
            ,MAX(CASE WHEN IND_MES = 1 THEN 1 ELSE 0 END) CAT_IND_1
            ,MAX(CASE WHEN IND_MES = 2 THEN 1 ELSE 0 END) CAT_IND_2
            ,MAX(CASE WHEN IND_MES = 3 THEN 1 ELSE 0 END) CAT_IND_3
            ,MAX(CASE WHEN IND_MES = 4 THEN 1 ELSE 0 END) CAT_IND_4
            ,MAX(CASE WHEN IND_MES = 5 THEN 1 ELSE 0 END) CAT_IND_5
            ,MAX(CASE WHEN IND_MES = 6 THEN 1 ELSE 0 END) CAT_IND_6
            ,MAX(CASE WHEN IND_MES = 7 THEN 1 ELSE 0 END) CAT_IND_7
            ,MAX(CASE WHEN IND_MES = 8 THEN 1 ELSE 0 END) CAT_IND_8
            ,MAX(CASE WHEN IND_MES = 9 THEN 1 ELSE 0 END) CAT_IND_9
            ,MAX(CASE WHEN IND_MES = 10 THEN 1 ELSE 0 END) CAT_IND_10
            ,MAX(CASE WHEN IND_MES = 11 THEN 1 ELSE 0 END) CAT_IND_11
            ,MAX(CASE WHEN IND_MES = 12 THEN 1 ELSE 0 END) CAT_IND_12
            ,MAX(CASE WHEN IND_MES_CAMPANA = 0 THEN 1 ELSE 0 END) CAT_IND_ANTERIOR
            ,MAX(CASE WHEN IND_MES_CAMPANA = 1 THEN 1 ELSE 0 END) CAT_IND_CAMPANA
            
                --VENTA
            ,SUM(CASE WHEN IND_MES_CAMPANA = 0 THEN VENTA ELSE 0 END) CAT_VENTA
            ,SUM(CASE WHEN IND_MES_CAMPANA = 1 THEN VENTA ELSE 0 END) CAT_VENTA_CAMPANA

                --TX
            ,COUNT(DISTINCT CASE WHEN IND_MES_CAMPANA = 0 THEN INVOICE_NO END) CAT_TX
            ,COUNT(DISTINCT CASE WHEN IND_MES_CAMPANA = 1 THEN INVOICE_NO END) CAT_TX_CAMPANA
            
                --VENTA_ELEGIBLE
            ,SUM(CASE WHEN IND_MES_CAMPANA = 0 AND IND_ELEGIBLE = 1 THEN VENTA ELSE 0 END) CAT_VENTA_ELEGIBLE
            ,SUM(CASE WHEN IND_MES_CAMPANA = 1 AND IND_ELEGIBLE = 1 THEN VENTA ELSE 0 END) CAT_VENTA_ELEGIBLE_CAMPANA

                --TX_ELEGIBLES
            ,COUNT(DISTINCT CASE WHEN IND_MES_CAMPANA = 0 AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) CAT_TX_ELEGIBLE
            ,COUNT(DISTINCT CASE WHEN IND_MES_CAMPANA = 1 AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) CAT_TX_ELEGIBLE_CAMPANA

            --ONLINE
            ,MAX(CASE WHEN IND_MES_CAMPANA = 0 AND IND_ONLINE = 1 THEN 1 ELSE 0 END) CAT_IND_ONLINE

            --RECOMPRA ULTIMOS 6 MESES
                --MARCA
            ,CASE WHEN IND_12 = 1 AND IND_CAMPANA = 1 THEN 1 ELSE 0 END RECOMPRA_MARCA
            ,CASE WHEN IND_11 = 1 AND IND_12 = 1 THEN 1 ELSE 0 END RECOMPRA_12
            ,CASE WHEN IND_10 = 1 AND IND_11 = 1 THEN 1 ELSE 0 END RECOMPRA_11
            ,CASE WHEN IND_9 = 1 AND IND_10 = 1 THEN 1 ELSE 0 END RECOMPRA_10
            ,CASE WHEN IND_8 = 1 AND IND_9 = 1 THEN 1 ELSE 0 END RECOMPRA_9
            ,CASE WHEN IND_7 = 1 AND IND_8 = 1 THEN 1 ELSE 0 END RECOMPRA_8
            ,CASE WHEN IND_6 = 1 AND IND_7 = 1 THEN 1 ELSE 0 END RECOMPRA_7
                --CAT
            ,CASE WHEN CAT_IND_12 = 1 AND CAT_IND_CAMPANA = 1 THEN 1 ELSE 0 END RECOMPRA_CAT
            ,CASE WHEN CAT_IND_11 = 1 AND CAT_IND_12 = 1 THEN 1 ELSE 0 END CAT_RECOMPRA_12
            ,CASE WHEN CAT_IND_10 = 1 AND CAT_IND_11 = 1 THEN 1 ELSE 0 END CAT_RECOMPRA_11
            ,CASE WHEN CAT_IND_9 = 1 AND CAT_IND_10 = 1 THEN 1 ELSE 0 END CAT_RECOMPRA_10
            ,CASE WHEN CAT_IND_8 = 1 AND CAT_IND_9 = 1 THEN 1 ELSE 0 END CAT_RECOMPRA_9
            ,CASE WHEN CAT_IND_7 = 1 AND CAT_IND_8 = 1 THEN 1 ELSE 0 END CAT_RECOMPRA_8
            ,CASE WHEN CAT_IND_6 = 1 AND CAT_IND_7 = 1 THEN 1 ELSE 0 END CAT_RECOMPRA_7
            
            FROM #VENTA
            LEFT JOIN #MESES USING(MES)
            GROUP BY 1,2,3,4
        )
        SELECT
            --DATOS CLIENTES
            CUSTOMER_CODE_TY
            ,VALID_CONTACT_INFO
            ,NSE
            ,TIPO_FAMILIA
                
            --PO
            --CAP
            ,CASE WHEN IND_1 = 0 AND IND_2 = 0 AND IND_3 = 0 AND IND_4 = 0 AND IND_5 = 0 AND IND_6 = 0 AND IND_7 = 0 AND IND_8 = 0 AND IND_9 = 0 AND IND_10 = 0 AND IND_11 = 0 AND IND_12 = 0 AND (CAT_IND_1 = 1 OR CAT_IND_2 = 1 OR CAT_IND_3 = 1 OR CAT_IND_4 = 1 OR CAT_IND_5 = 1 OR CAT_IND_6 = 1 OR CAT_IND_7 = 1 OR CAT_IND_8 = 1 OR CAT_IND_9 = 1 OR CAT_IND_10 = 1 OR CAT_IND_11 = 1 OR CAT_IND_12 = 1) THEN 1 ELSE 0 END AS PO_CAP_12
            ,CASE WHEN IND_7 = 0 AND IND_8 = 0 AND IND_9 = 0 AND IND_10 = 0 AND IND_11 = 0 AND IND_12 = 0 AND (CAT_IND_7 = 1 OR CAT_IND_8 = 1 OR CAT_IND_9 = 1 OR CAT_IND_10 = 1 OR CAT_IND_11 = 1 OR CAT_IND_12 = 1) THEN 1 ELSE 0 END AS PO_CAP_6
            ,CASE WHEN IND_10 = 0 AND IND_11 = 0 AND IND_12 = 0 AND (CAT_IND_10 = 1 OR CAT_IND_11 = 1 OR CAT_IND_12 = 1) THEN 1 ELSE 0 END AS PO_CAP_3

            --REC
            ,CASE WHEN (IND_1 = 1 OR IND_2 = 1 OR IND_3 = 1 OR IND_4 = 1 OR IND_5 = 1 OR IND_6 = 1) AND IND_7 = 0 AND IND_8 = 0 AND IND_9 = 0 AND IND_10 = 0 AND IND_11 = 0 AND IND_12 = 0 THEN 1 ELSE 0 END AS PO_REC_12_6
            ,CASE WHEN (IND_7 = 1 OR IND_8 = 1 OR IND_9 = 1) AND IND_10 = 0 AND IND_11 = 0 AND IND_12 = 0 THEN 1 ELSE 0 END AS PO_REC_6_3
            ,CASE WHEN PO_REC_12_6 = 1 OR PO_REC_6_3 = 1 THEN 1 ELSE 0 END PO_REC_12_REC_6
            
            --FID
            ,CASE WHEN IND_1 = 1 OR IND_2 = 1 OR IND_3 = 1 OR IND_4 = 1 OR IND_5 = 1 OR IND_6 = 1 OR IND_7 = 1 OR IND_8 = 1 OR IND_9 = 1 OR IND_10 = 1 OR IND_11 = 1 OR IND_12 = 1 THEN 1 ELSE 0 END AS PO_FID_12
            ,CASE WHEN IND_7 = 1 OR IND_8 = 1 OR IND_9 = 1 OR IND_10 = 1 OR IND_11 = 1 OR IND_12 = 1 THEN 1 ELSE 0 END AS PO_FID_6
            ,CASE WHEN IND_10 = 1 OR IND_11 = 1 OR IND_12 = 1 THEN 1 ELSE 0 END AS PO_FID_3
            
            --COMBINADOS
            ,CASE WHEN PO_CAP_12 = 1 OR PO_REC_12_6 = 1 OR PO_FID_6 = 1 THEN 1 ELSE 0 END PO_CAP_12_REC_12_FID_6
            ,CASE WHEN PO_CAP_12 = 1 OR PO_REC_12_6 = 1 OR PO_REC_6_3 = 1 OR PO_FID_3 = 1 THEN 1 ELSE 0 END PO_CAP_12_REC_12_REC_6_FID_3
            ,CASE WHEN PO_CAP_12 = 1 OR PO_REC_12_6 = 1 OR PO_REC_6_3 = 1 THEN 1 ELSE 0 END PO_CAP_12_REC_12_REC_6
            ,CASE WHEN PO_CAP_12 = 1 OR PO_FID_12 = 1 THEN 1 ELSE 0 END PO_CAP_12_FID_12
            ,CASE WHEN PO_REC_12_6 = 1 OR PO_FID_6 = 1 THEN 1 ELSE 0 END PO_REC_12_FID_6
            ,CASE WHEN PO_REC_12_6 = 1 OR PO_REC_6_3 = 1 OR PO_FID_3 = 1 THEN 1 ELSE 0 END PO_REC_12_REC_6_FID_3
            
            --COMPRA EN PERIODO DE INTERES
            --CAP
            ,CASE WHEN IND_CAMPANA = 1 AND PO_CAP_12 = 1 THEN 1 ELSE 0 END CAP_12
            ,CASE WHEN IND_CAMPANA = 1 AND PO_CAP_6 = 1 THEN 1 ELSE 0 END CAP_6
            ,CASE WHEN IND_CAMPANA = 1 AND PO_CAP_3 = 1 THEN 1 ELSE 0 END CAP_3

            --REC
            ,CASE WHEN IND_CAMPANA = 1 AND PO_REC_12_6 = 1 THEN 1 ELSE 0 END REC_12_6
            ,CASE WHEN IND_CAMPANA = 1 AND PO_REC_6_3 = 1 THEN 1 ELSE 0 END REC_6_3
            ,CASE WHEN IND_CAMPANA = 1 AND PO_REC_12_REC_6 = 1 THEN 1 ELSE 0 END REC_12_REC_6
            
            --FID
            ,CASE WHEN IND_CAMPANA = 1 AND PO_FID_12 = 1 THEN 1 ELSE 0 END FID_12
            ,CASE WHEN IND_CAMPANA = 1 AND PO_FID_6 = 1 THEN 1 ELSE 0 END FID_6
            ,CASE WHEN IND_CAMPANA = 1 AND PO_FID_3 = 1 THEN 1 ELSE 0 END FID_3
            
            --COMBINADOS
            ,CASE WHEN IND_CAMPANA = 1 AND PO_CAP_12_REC_12_FID_6 = 1 THEN 1 ELSE 0 END CAP_12_REC_12_FID_6
            ,CASE WHEN IND_CAMPANA = 1 AND PO_CAP_12_REC_12_REC_6_FID_3 = 1 OR PO_FID_3 = 1 THEN 1 ELSE 0 END CAP_12_REC_12_REC_6_FID_3
            ,CASE WHEN IND_CAMPANA = 1 AND PO_CAP_12_REC_12_REC_6 = 1 THEN 1 ELSE 0 END CAP_12_REC_12_REC_6
            ,CASE WHEN IND_CAMPANA = 1 AND PO_CAP_12_FID_12 = 1 THEN 1 ELSE 0 END CAP_12_FID_12
            ,CASE WHEN IND_CAMPANA = 1 AND PO_REC_12_FID_6 = 1 THEN 1 ELSE 0 END REC_12_FID_6
            ,CASE WHEN IND_CAMPANA = 1 AND PO_REC_12_REC_6_FID_3 = 1 THEN 1 ELSE 0 END REC_12_REC_6_FID_3
        
            --INDICADOR DE RECOMPRA EN LOS ÚLTIMOS 6 MESES - 0 A 1
            ,IND_12 AS PO_RECOMPRA_MARCA
            ,RECOMPRA_MARCA
            ,(RECOMPRA_12 + RECOMPRA_11 + RECOMPRA_10 + RECOMPRA_9 + RECOMPRA_8 + RECOMPRA_7)::NUMERIC / 6 AS RECOMPRA_6M_MARCA
            ,(CAT_RECOMPRA_12 + CAT_RECOMPRA_11 + CAT_RECOMPRA_10 + CAT_RECOMPRA_9 + CAT_RECOMPRA_8 + CAT_RECOMPRA_7)::NUMERIC / 6 AS RECOMPRA_6M_CAT

            --INDICADORES DE FILTRADO
            ,CASE WHEN PO_FID_6 = 1 THEN 1 ELSE 0 END IND_FID
            ,CASE WHEN PO_REC_12_6 = 1 THEN 1 ELSE 0 END IND_REC
            ,CASE WHEN PO_CAP_12 = 1 THEN 1 ELSE 0 END IND_CAP
            
            ,CASE
            WHEN IND_FID = 1 THEN '1 FID'
            WHEN IND_REC = 1 THEN '2 REC'
            WHEN IND_CAP = 1 THEN '3 CAP'
            END ORDEN_SEGMENTO

            --INDICADORES DE COMPRA
            ,CAT_IND_ONLINE AS IND_ONLINE
            ,IND_CAMPANA
            ,IND_ANTERIOR
            ,CASE WHEN VENTA_ELEGIBLE_CAMPANA > 0 THEN 1 ELSE 0 END AS IND_CAMPANA_ELEGIBLE
            ,CASE WHEN VENTA_ELEGIBLE > 0 THEN 1 ELSE 0 END AS IND_ANTERIOR_ELEGIBLE
            
            ,UNIDADES_CAMPANA
            ,TX_CAMPANA

            --DIAS PARA RECOMPRA
            ,LAST_INVOICE_DATE
            ,LAST_INVOICE_DATE_MARCA
            ,DIAS_CON_COMPRA
            ,DIAS_CON_COMPRA_MARCA
            ,PROMEDIO_DIAS_RECOMPRA
            ,PROMEDIO_DIAS_RECOMPRA_MARCA
            
            ,CAT_VENTA AS VENTA_CAT
            ,VENTA_ANTERIOR AS VENTA_MARCA

            ,CAT_TX AS TX_CAT
            ,TX AS TX_MARCA

            ,CASE WHEN TX_CAT = 0 THEN 0 ELSE VENTA_CAT::NUMERIC / TX_CAT::NUMERIC END AS TICKET_MEDIO_CAT
            ,CASE WHEN TX_MARCA = 0 THEN 0 ELSE VENTA_MARCA::NUMERIC / TX_MARCA::NUMERIC END AS TICKET_MEDIO_MARCA

            ,CAT_VENTA_ELEGIBLE AS VENTA_ELEGIBLE_CAT
            ,VENTA_ELEGIBLE AS VENTA_ELEGIBLE_MARCA

            ,CAT_TX_ELEGIBLE AS TX_ELEGIBLE_CAT
            ,TX_ELEGIBLE AS TX_ELEGIBLE_MARCA

            ,CASE WHEN TX_ELEGIBLE_CAT = 0 THEN 0 ELSE VENTA_ELEGIBLE_CAT::NUMERIC / TX_ELEGIBLE_CAT::NUMERIC END AS TICKET_MEDIO_ELEGIBLE_CAT
            ,CASE WHEN TX_ELEGIBLE_MARCA = 0 THEN 0 ELSE VENTA_ELEGIBLE_MARCA::NUMERIC / TX_ELEGIBLE_MARCA::NUMERIC END AS TICKET_MEDIO_ELEGIBLE_MARCA
            
        FROM __INDICADORES
        LEFT JOIN #TX_RECOMPRA B USING(CUSTOMER_CODE_TY)
        WHERE PO_CAP_12_REC_12_FID_6 = 1
        );
        '''

        return query_pos_temporal
    
    def get_query_create_pos_agg(self, table_name, from_table='#PO'):
        query_agg = '''
            --PO
            ,SUM(PO_CAP_12) PO_CAP_12
            ,SUM(PO_CAP_6) PO_CAP_6
            ,SUM(PO_CAP_3) PO_CAP_3

            ,SUM(PO_REC_12_6) PO_REC_12_6
            ,SUM(PO_REC_6_3) PO_REC_6_3
            ,SUM(PO_REC_12_REC_6) PO_REC_12_REC_6

            ,SUM(PO_FID_12) PO_FID_12
            ,SUM(PO_FID_6) PO_FID_6
            ,SUM(PO_FID_3) PO_FID_3

            ,SUM(PO_CAP_12_REC_12_FID_6) PO_CAP_12_REC_12_FID_6
            ,SUM(PO_CAP_12_REC_12_REC_6_FID_3) PO_CAP_12_REC_12_REC_6_FID_3
            ,SUM(PO_CAP_12_REC_12_REC_6) PO_CAP_12_REC_12_REC_6
            ,SUM(PO_CAP_12_FID_12) PO_CAP_12_FID_12
            ,SUM(PO_REC_12_FID_6) PO_REC_12_FID_6
            ,SUM(PO_REC_12_REC_6_FID_3) PO_REC_12_REC_6_FID_3
            
            --PO CON COMPRA
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
        '''
        
        query_pos_agg_temporal = f'''
        -- AGRUPADOS
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} AS (
        WITH __AGG_PO AS (
        --GLOBAL POR CANAL
        SELECT
            '01 GLOBAL' TABLA
            ,'TOTAL' TIPO_FAMILIA
            ,'TOTAL' NSE
            ,VALID_CONTACT_INFO
            {query_agg}
        FROM {from_table}
        GROUP BY 1,2,3,4

        UNION

        --GLOBAL
        SELECT
            '01 GLOBAL' TABLA
            ,'TOTAL' TIPO_FAMILIA
            ,'TOTAL' NSE
            ,'00 TOTAL' VALID_CONTACT_INFO
            {query_agg}
        FROM {from_table}
        GROUP BY 1,2,3,4

        UNION

        --FAMILIA POR CANAL
        SELECT
            '02 FAMILIA' TABLA
            ,TIPO_FAMILIA
            ,'TOTAL' NSE
            ,VALID_CONTACT_INFO
            {query_agg}
        FROM {from_table}
        GROUP BY 1,2,3,4

        UNION

        --FAMILIA GLOBAL
        SELECT
            '02 FAMILIA' TABLA
            ,TIPO_FAMILIA
            ,'TOTAL' NSE
            ,'00 TOTAL' VALID_CONTACT_INFO
            {query_agg}
        FROM {from_table}
        GROUP BY 1,2,3,4

        UNION

        --NSE POR CANAL
        SELECT
            '03 NSE' TABLA
            ,'TOTAL' TIPO_FAMILIA
            ,NSE
            ,VALID_CONTACT_INFO
            {query_agg}
        FROM {from_table}
        GROUP BY 1,2,3,4

        UNION

        --NSE GLOBAL
        SELECT
            '03 NSE' TABLA
            ,'TOTAL' TIPO_FAMILIA
            ,NSE
            ,'00 TOTAL' VALID_CONTACT_INFO
            {query_agg}
        FROM {from_table}
        GROUP BY 1,2,3,4

        UNION

        --FAMILIA Y NSE POR CANAL
        SELECT
            '04 FAMILIA Y NSE' TABLA
            ,TIPO_FAMILIA
            ,NSE
            ,VALID_CONTACT_INFO
            {query_agg}
        FROM {from_table}
        GROUP BY 1,2,3,4

        UNION

        --NSE GLOBAL
        SELECT
            '04 FAMILIA Y NSE' TABLA
            ,TIPO_FAMILIA
            ,NSE
            ,'00 TOTAL' VALID_CONTACT_INFO
            {query_agg}
        FROM {from_table}
        GROUP BY 1,2,3,4
        )
        SELECT
        *
        FROM __AGG_PO
        ORDER BY 1, 2, 3, 4
        );
            '''
        return query_pos_agg_temporal

    def get_query_create_bc_tables(self):
        query_meses = f'''
            --CREAR FECHAS DE MES
            DROP TABLE IF EXISTS #MESES;
            CREATE TABLE #MESES AS (
            WITH __MESES AS (
            SELECT DISTINCT
                LEFT(INVOICE_DATE, 7) MES
                ,EXTRACT(MONTH FROM INVOICE_DATE) MES_NUMERO
                ,CASE
                WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_bc_var['date_dash']} THEN 'ACTUAL'
                WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_bc_var['date_dash_aa']} THEN 'ANO_ANTERIOR'
                END AS PERIODO
                ,CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_bc_var['date_dash_campana']} THEN 1 ELSE 0 END AS IND_CAMPANA
                
            FROM FCT_SALE_HEADER
            WHERE LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_bc_var['mes_ini_analisis_aa']}' AND '{self.dict_bc_var['mes_fin_analisis']}'
            )
            SELECT
            *
            ,ROW_NUMBER() OVER(ORDER BY MES) IND_MES
            FROM __MESES
            );

            -- SELECT * FROM #MESES ORDER BY 1;
        '''

        query_tx = f'''
            -- CREAR TABLA DE TX
            DROP TABLE IF EXISTS #TX;
            CREATE TABLE #TX AS (
            SELECT
                INVOICE_NO
                ,IND_MARCA
                ,MARCA
                ,IND_ONLINE
                ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_var['condicion']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE
                ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_var['condicion_50']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE_50
                ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_var['condicion_75']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE_75
                ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_var['condicion_100']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE_100
                ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_var['condicion_150']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE_150
                ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_var['condicion_200']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE_200
            FROM FCT_SALE_LINE
            INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
            LEFT JOIN (SELECT DISTINCT INVOICE_NO, CASE WHEN CHANNEL_TYPE IN ('WEB','APP','CC HY') THEN 1 ELSE 0 END IND_ONLINE FROM FCT_SALE_HEADER) F USING(INVOICE_NO)
            WHERE LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_bc_var['mes_ini_analisis_aa']}' AND '{self.dict_bc_var['mes_fin_analisis']}'
            AND BUSINESS_TYPE = 'R'
            AND SALE_NET_VAL > 0
            GROUP BY 1,2,3,4

            --   UNION
            --   
            --   SELECT
            --     INVOICE_NO
            --     ,IND_MARCA
            --     ,MARCA
            --     ,IND_ONLINE
            --     ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_var['condicion']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE
            --     ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_var['condicion_50']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE_50
            --     ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_var['condicion_75']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE_75
            --     ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_var['condicion_100']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE_100
            --     ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_var['condicion_150']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE_150
            --     ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_var['condicion_200']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE_200
            --   FROM FCT_SALE_LINE_NM A
            --   INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
            --   LEFT JOIN (SELECT DISTINCT INVOICE_NO, CASE WHEN CHANNEL_TYPE IN ('WEB','APP','CC HY') THEN 1 ELSE 0 END IND_ONLINE FROM FCT_SALE_HEADER_NM) F USING(INVOICE_NO)
            --   WHERE LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_bc_var['mes_ini_analisis_aa']}' AND '{self.dict_bc_var['mes_fin_analisis']}'
            --   AND BUSINESS_TYPE = 'R'
            --   AND SALE_NET_VAL > 0
            --   GROUP BY 1,2,3,4
            );
            '''
        
        query_venta = f'''
            --TABLA VENTA
            DROP TABLE IF EXISTS #VENTA;
            CREATE TABLE #VENTA AS (
                SELECT
                1::INT IND_MC
                ,CUSTOMER_CODE_TY
                ,COALESCE(NSE, 'NO SEGMENTADO') NSE
                ,COALESCE(TIPO_FAMILIA, 'NO SEGMENTADO') TIPO_FAMILIA
                ,EXTRACT(MONTH FROM INVOICE_DATE) MES_NUMERO
                ,CASE
                    WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_bc_var['date_dash']} THEN 'ACTUAL'
                    WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_bc_var['date_dash_aa']} THEN 'ANO_ANTERIOR'
                END AS PERIODO
                ,A.INVOICE_NO
                ,IND_ELEGIBLE
                ,IND_ELEGIBLE_50
                ,IND_ELEGIBLE_75
                ,IND_ELEGIBLE_100
                ,IND_ELEGIBLE_150
                ,IND_ELEGIBLE_200
                ,PROVEEDOR
                ,B.MARCA
                ,B.IND_MARCA
                ,PRODUCT_CODE
                ,PRODUCT_DESCRIPTION
                ,STATE
                ,REGION
                ,FORMATO_TIENDA
                ,STORE_DESCRIPTION
                ,SUM(SALE_TOT_QTY) UNIDADES
                ,SUM(SALE_NET_VAL) VENTA
                FROM FCT_SALE_LINE A
                INNER JOIN #PRODUCTOS B USING(PRODUCT_CODE)
                INNER JOIN #TX C USING(INVOICE_NO, IND_MARCA, MARCA)
                INNER JOIN CHEDRAUI.V_STORE USING(STORE_CODE, STORE_KEY)
                LEFT JOIN CHEDRAUI.V_CUSTOMER_CONTACT D ON A.CUSTOMER_CODE_TY = D.CUSTOMER_CODE
                WHERE BUSINESS_TYPE = 'R'
                AND SALE_NET_VAL > 0
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22

            --   UNION
            --   
            --     SELECT
            --       0::INT IND_MC
            --       ,NULL CUSTOMER_CODE_TY
            --       ,NULL NSE
            --       ,NULL TIPO_FAMILIA
            --       ,EXTRACT(MONTH FROM INVOICE_DATE) MES_NUMERO
            --       ,CASE
            --         WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_bc_var['date_dash']} THEN 'ACTUAL'
            --         WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_bc_var['date_dash_aa']} THEN 'ANO_ANTERIOR'
            --       END AS PERIODO
            --       ,A.INVOICE_NO
            --       ,IND_ELEGIBLE
            --       ,IND_ELEGIBLE_50
            --       ,IND_ELEGIBLE_75
            --       ,IND_ELEGIBLE_100
            --       ,IND_ELEGIBLE_150
            --       ,IND_ELEGIBLE_200
            --       ,PROVEEDOR
            --       ,B.MARCA
            --       ,B.IND_MARCA
            --       ,PRODUCT_CODE
            --       ,PRODUCT_DESCRIPTION
            --       ,STATE
            --       ,REGION
            --       ,FORMATO_TIENDA
            --       ,STORE_DESCRIPTION
            --       ,SUM(SALE_TOT_QTY) UNIDADES
            --       ,SUM(SALE_NET_VAL) VENTA
            --     FROM FCT_SALE_LINE_NM A
            --     INNER JOIN #PRODUCTOS B USING(PRODUCT_CODE)
            --     INNER JOIN #TX C USING(INVOICE_NO, IND_MARCA, MARCA)
            --     INNER JOIN CHEDRAUI.V_STORE USING(STORE_CODE, STORE_KEY)
            --     WHERE BUSINESS_TYPE = 'R'
            --     AND SALE_NET_VAL > 0
            --     GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
            );
            '''

        query_recompra = f'''
            DROP TABLE IF EXISTS #RECOMPRA;
            CREATE TABLE #RECOMPRA AS (
            WITH __INDICADORES AS (
            SELECT
                CUSTOMER_CODE_TY
                ,PROVEEDOR
                ,MARCA
                
                --INDICADORES DE COMPRA
                --MARCA
                ,MAX(CASE WHEN IND_MES = 1 THEN 1 ELSE 0 END) IND_1
                ,MAX(CASE WHEN IND_MES = 2 THEN 1 ELSE 0 END) IND_2
                ,MAX(CASE WHEN IND_MES = 3 THEN 1 ELSE 0 END) IND_3
                ,MAX(CASE WHEN IND_MES = 4 THEN 1 ELSE 0 END) IND_4
                ,MAX(CASE WHEN IND_MES = 5 THEN 1 ELSE 0 END) IND_5
                ,MAX(CASE WHEN IND_MES = 6 THEN 1 ELSE 0 END) IND_6
                ,MAX(CASE WHEN IND_MES = 7 THEN 1 ELSE 0 END) IND_7
                ,MAX(CASE WHEN IND_MES = 8 THEN 1 ELSE 0 END) IND_8
                ,MAX(CASE WHEN IND_MES = 9 THEN 1 ELSE 0 END) IND_9
                ,MAX(CASE WHEN IND_MES = 10 THEN 1 ELSE 0 END) IND_10
                ,MAX(CASE WHEN IND_MES = 11 THEN 1 ELSE 0 END) IND_11
                ,MAX(CASE WHEN IND_MES = 12 THEN 1 ELSE 0 END) IND_12
                ,MAX(CASE WHEN IND_MES = 13 THEN 1 ELSE 0 END) IND_13
                ,MAX(CASE WHEN IND_MES = 14 THEN 1 ELSE 0 END) IND_14
                ,MAX(CASE WHEN IND_MES = 15 THEN 1 ELSE 0 END) IND_15
                ,MAX(CASE WHEN IND_MES = 16 THEN 1 ELSE 0 END) IND_16
                ,MAX(CASE WHEN IND_MES = 17 THEN 1 ELSE 0 END) IND_17
                ,MAX(CASE WHEN IND_MES = 18 THEN 1 ELSE 0 END) IND_18
                ,MAX(CASE WHEN IND_MES = 19 THEN 1 ELSE 0 END) IND_19
                ,MAX(CASE WHEN IND_MES = 20 THEN 1 ELSE 0 END) IND_20
                ,MAX(CASE WHEN IND_MES = 21 THEN 1 ELSE 0 END) IND_21
                ,MAX(CASE WHEN IND_MES = 22 THEN 1 ELSE 0 END) IND_22
                ,MAX(CASE WHEN IND_MES = 23 THEN 1 ELSE 0 END) IND_23
                ,MAX(CASE WHEN IND_MES = 24 THEN 1 ELSE 0 END) IND_24

                --RECOMPRA
                ,CASE WHEN IND_12 = 1 AND IND_13 = 1 THEN 1 ELSE 0 END AS RECOMPRA_13
                ,CASE WHEN IND_13 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END AS RECOMPRA_14
                ,CASE WHEN IND_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS RECOMPRA_15
                ,CASE WHEN IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS RECOMPRA_16
                ,CASE WHEN IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS RECOMPRA_17
                ,CASE WHEN IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS RECOMPRA_18
                ,CASE WHEN IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS RECOMPRA_19
                ,CASE WHEN IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS RECOMPRA_20
                ,CASE WHEN IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS RECOMPRA_21
                ,CASE WHEN IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS RECOMPRA_22
                ,CASE WHEN IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS RECOMPRA_23
                ,CASE WHEN IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS RECOMPRA_24

            FROM #VENTA
            LEFT JOIN #MESES USING(MES_NUMERO, PERIODO)
            WHERE IND_MC = 1
            AND IND_MARCA = 1
            GROUP BY 1,2,3
            )
            ,__CLIENTES AS (
                SELECT
                PROVEEDOR
                ,MARCA
                --CLIENTES
                ,SUM(IND_13) CLIENTES_13
                ,SUM(IND_14) CLIENTES_14
                ,SUM(IND_15) CLIENTES_15
                ,SUM(IND_16) CLIENTES_16
                ,SUM(IND_17) CLIENTES_17
                ,SUM(IND_18) CLIENTES_18
                ,SUM(IND_19) CLIENTES_19
                ,SUM(IND_20) CLIENTES_20
                ,SUM(IND_21) CLIENTES_21
                ,SUM(IND_22) CLIENTES_22
                ,SUM(IND_23) CLIENTES_23
                ,SUM(IND_24) CLIENTES_24

                --COMPRA RECOMPRA
                ,SUM(RECOMPRA_13) RECOMPRA_13
                ,SUM(RECOMPRA_14) RECOMPRA_14
                ,SUM(RECOMPRA_15) RECOMPRA_15
                ,SUM(RECOMPRA_16) RECOMPRA_16
                ,SUM(RECOMPRA_17) RECOMPRA_17
                ,SUM(RECOMPRA_18) RECOMPRA_18
                ,SUM(RECOMPRA_19) RECOMPRA_19
                ,SUM(RECOMPRA_20) RECOMPRA_20
                ,SUM(RECOMPRA_21) RECOMPRA_21
                ,SUM(RECOMPRA_22) RECOMPRA_22
                ,SUM(RECOMPRA_23) RECOMPRA_23
                ,SUM(RECOMPRA_24) RECOMPRA_24

            FROM __INDICADORES
            GROUP BY 1,2
            )
            ,__RESUMEN_SEGMENTOS AS (
                SELECT PROVEEDOR, MARCA, 13 IND_MES, CLIENTES_13 AS CLIENTES, RECOMPRA_13 AS RECOMPRA FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 14 IND_MES, CLIENTES_14 AS CLIENTES, RECOMPRA_14 AS RECOMPRA FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 15 IND_MES, CLIENTES_15 AS CLIENTES, RECOMPRA_15 AS RECOMPRA FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 16 IND_MES, CLIENTES_16 AS CLIENTES, RECOMPRA_16 AS RECOMPRA FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 17 IND_MES, CLIENTES_17 AS CLIENTES, RECOMPRA_17 AS RECOMPRA FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 18 IND_MES, CLIENTES_18 AS CLIENTES, RECOMPRA_18 AS RECOMPRA FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 19 IND_MES, CLIENTES_19 AS CLIENTES, RECOMPRA_19 AS RECOMPRA FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 20 IND_MES, CLIENTES_20 AS CLIENTES, RECOMPRA_20 AS RECOMPRA FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 21 IND_MES, CLIENTES_21 AS CLIENTES, RECOMPRA_21 AS RECOMPRA FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 22 IND_MES, CLIENTES_22 AS CLIENTES, RECOMPRA_22 AS RECOMPRA FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 23 IND_MES, CLIENTES_23 AS CLIENTES, RECOMPRA_23 AS RECOMPRA FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 24 IND_MES, CLIENTES_24 AS CLIENTES, RECOMPRA_24 AS RECOMPRA FROM __CLIENTES
            )
            SELECT
                PROVEEDOR
                ,MARCA
                ,MES
                ,CLIENTES
                ,RECOMPRA
                ,CASE WHEN RECOMPRA > 0 THEN RECOMPRA::NUMERIC / CLIENTES END AS "%RECOMPRA"
            FROM __RESUMEN_SEGMENTOS A
            LEFT JOIN #MESES B USING(IND_MES)
            );

            -- SELECT * FROM #RECOMPRA;
            '''
        
        query_agg_func = f'''
                --MARCA
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' THEN VENTA END) VENTA_ACTUAL
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' THEN UNIDADES END) UNIDADES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' THEN CUSTOMER_CODE_TY END) CLIENTES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' THEN INVOICE_NO END) TX_ACTUAL

                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ANO_ANTERIOR' THEN VENTA END) VENTA_AA
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ANO_ANTERIOR' THEN UNIDADES END) UNIDADES_AA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ANO_ANTERIOR' THEN CUSTOMER_CODE_TY END) CLIENTES_AA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ANO_ANTERIOR' THEN INVOICE_NO END) TX_AA
                
                --CAT
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' THEN VENTA END) CAT_VENTA_ACTUAL
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' THEN UNIDADES END) CAT_UNIDADES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACTUAL' THEN CUSTOMER_CODE_TY END) CAT_CLIENTES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACTUAL' THEN INVOICE_NO END) CAT_TX_ACTUAL

                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN VENTA END) CAT_VENTA_AA
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN UNIDADES END) CAT_UNIDADES_AA
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN CUSTOMER_CODE_TY END) CAT_CLIENTES_AA
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN INVOICE_NO END) CAT_TX_AA
                
                --CONDICION COMPRA
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_50 = 1 THEN VENTA END) VENTA_50
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_75 = 1 THEN VENTA END) VENTA_75
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_100 = 1 THEN VENTA END) VENTA_100
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_150 = 1 THEN VENTA END) VENTA_150
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_200 = 1 THEN VENTA END) VENTA_200

                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_50 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_50
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_75 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_75
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_100 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_100
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_150 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_150
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_200 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_200

                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_50 = 1 THEN INVOICE_NO END) TX_50
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_75 = 1 THEN INVOICE_NO END) TX_75
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_100 = 1 THEN INVOICE_NO END) TX_100
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_150 = 1 THEN INVOICE_NO END) TX_150
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE_200 = 1 THEN INVOICE_NO END) TX_200
            
                ,VENTA_50::NUMERIC / TX_50 AS TX_MEDIO_50
                ,VENTA_75::NUMERIC / TX_75 AS TX_MEDIO_75
                ,VENTA_100::NUMERIC / TX_100 AS TX_MEDIO_100
                ,VENTA_150::NUMERIC / TX_150 AS TX_MEDIO_150
                ,VENTA_200::NUMERIC / TX_200 AS TX_MEDIO_200
                
                ,AVG("%RECOMPRA") "%RECOMPRA"
        '''

        query_venta_marca = f'''
            --VENTA AGRUPADA POR MES
            DROP TABLE IF EXISTS #VENTA_MARCA;
            CREATE TABLE #VENTA_MARCA AS (
            SELECT
                'MARCA' TABLA
                ,A.PROVEEDOR
                ,A.MARCA
                ,B.MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,'TOTAL' STATE
                ,'TOTAL' REGION
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,'TOTAL' PRODUCT_DESCRIPTION

                {query_agg_func}
                
            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #RECOMPRA C ON A.PROVEEDOR = C.PROVEEDOR AND A.MARCA = C.MARCA AND B.MES = C.MES
            WHERE IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11
            );

            -- SELECT * FROM #VENTA_MENSUAL;
        '''
        
        query_venta_tienda = f'''
            --VENTA AGRUPADA POR MES
            DROP TABLE IF EXISTS #VENTA_TIENDA;
            CREATE TABLE #VENTA_TIENDA AS (
            SELECT
                'TIENDA' TABLA
                ,A.PROVEEDOR
                ,A.MARCA
                ,B.MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,STATE
                ,REGION
                ,FORMATO_TIENDA
                ,STORE_DESCRIPTION
                
                ,'TOTAL' PRODUCT_DESCRIPTION

                {query_agg_func}
                
            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #RECOMPRA C ON A.PROVEEDOR = C.PROVEEDOR AND A.MARCA = C.MARCA AND B.MES = C.MES
            WHERE IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11
            );
        '''

        query_producto = f'''
            --VENTA AGRUPADA POR MES
            DROP TABLE IF EXISTS #VENTA_PRODUCTO;
            CREATE TABLE #VENTA_PRODUCTO AS (
            SELECT
                'PRODUCTO' TABLA
                ,A.PROVEEDOR
                ,A.MARCA
                ,B.MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,'TOTAL' STATE
                ,'TOTAL' REGION
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,PRODUCT_DESCRIPTION

                {query_agg_func}
                
            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #RECOMPRA C ON A.PROVEEDOR = C.PROVEEDOR AND A.MARCA = C.MARCA AND B.MES = C.MES
            WHERE IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11
            );
        '''

        query_nse_familia = f'''
            --VENTA AGRUPADA POR MES
            DROP TABLE IF EXISTS #VENTA_NSE_FAMILIA;
            CREATE TABLE #VENTA_NSE_FAMILIA AS (
            SELECT
                'NSE_FMILIA' TABLA
                ,A.PROVEEDOR
                ,A.MARCA
                ,B.MES
                
                ,NSE
                ,TIPO_FAMILIA

                ,'TOTAL' STATE
                ,'TOTAL' REGION
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,'TOTAL' PRODUCT_DESCRIPTION

                {query_agg_func}
                
            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #RECOMPRA C ON A.PROVEEDOR = C.PROVEEDOR AND A.MARCA = C.MARCA AND B.MES = C.MES
            WHERE IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11
            );
        '''

        query_bc = f'''
            --VENTA AGRUPADA POR MES
            DROP TABLE IF EXISTS #VENTA_BC;
            CREATE TABLE #VENTA_BC AS (
            SELECT
                'BC' TABLA
                ,A.PROVEEDOR
                ,A.MARCA
                ,'CAMPANA' MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,'TOTAL' STATE
                ,'TOTAL' REGION
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,'TOTAL' PRODUCT_DESCRIPTION

                {query_agg_func}

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO, IND_CAMPANA FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #RECOMPRA C ON A.PROVEEDOR = C.PROVEEDOR AND A.MARCA = C.MARCA AND B.MES = C.MES
            WHERE IND_MC = 1
            AND B.IND_CAMPANA = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11
            );
        '''

        query_agg = f'''
            DROP TABLE IF EXISTS #VENTA_AGG;
            CREATE TABLE #VENTA_AGG AS (
            WITH __AGG AS (
                SELECT * FROM #VENTA_MARCA
                UNION
                SELECT * FROM #VENTA_TIENDA
                UNION
                SELECT * FROM #VENTA_PRODUCTO
                UNION
                SELECT * FROM #VENTA_NSE_FAMILIA
                UNION
                SELECT * FROM #VENTA_BC
            )
            SELECT
                TABLA
                ,A.PROVEEDOR
                ,A.MARCA
                ,A.MES
            
                ,NSE
                ,TIPO_FAMILIA
            
                ,STATE
                ,REGION
                ,FORMATO_TIENDA
                ,STORE_DESCRIPTION
            
                ,PRODUCT_DESCRIPTION

                ,VENTA_ACTUAL
                ,UNIDADES_ACTUAL
                ,CLIENTES_ACTUAL
                ,TX_ACTUAL

                ,VENTA_AA
                ,UNIDADES_AA
                ,CLIENTES_AA
                ,TX_AA
            
                ,CAT_VENTA_ACTUAL
                ,CAT_UNIDADES_ACTUAL
                ,CAT_CLIENTES_ACTUAL
                ,CAT_TX_ACTUAL
            
                ,"%RECOMPRA"

                ,CASE WHEN TX_ACTUAL > 0 THEN VENTA_ACTUAL::NUMERIC / TX_ACTUAL ELSE 0 END AS TX_MEDIO
                ,CASE WHEN TX_AA > 0 THEN VENTA_AA::NUMERIC / TX_AA ELSE 0 END AS TX_MEDIO_AA
                ,CASE WHEN CAT_TX_ACTUAL > 0 THEN CAT_VENTA_ACTUAL::NUMERIC / CAT_TX_ACTUAL ELSE 0 END AS CAT_TX_MEDIO
                ,CASE WHEN CAT_TX_AA > 0 THEN CAT_VENTA_AA::NUMERIC / CAT_TX_AA ELSE 0 END AS CAT_TX_MEDIO_AA
                
                ,CASE WHEN UNIDADES_ACTUAL > 0 THEN VENTA_ACTUAL::NUMERIC / UNIDADES_ACTUAL ELSE 0 END AS PRECIO_MEDIO
                ,CASE WHEN UNIDADES_AA > 0 THEN VENTA_AA::NUMERIC / UNIDADES_AA ELSE 0 END AS PRECIO_MEDIO_AA
                ,CASE WHEN CAT_UNIDADES_ACTUAL > 0 THEN CAT_VENTA_ACTUAL::NUMERIC / CAT_UNIDADES_ACTUAL ELSE 0 END AS CAT_PRECIO_MEDIO
                ,CASE WHEN CAT_UNIDADES_AA > 0 THEN CAT_VENTA_AA::NUMERIC / CAT_UNIDADES_AA ELSE 0 END AS CAT_PRECIO_MEDIO_AA

                ,CASE WHEN CAT_VENTA_ACTUAL > 0 THEN VENTA_ACTUAL::NUMERIC / CAT_VENTA_ACTUAL ELSE 0 END AS SHARE
                ,CASE WHEN CAT_VENTA_AA > 0 THEN VENTA_AA::NUMERIC / CAT_VENTA_AA ELSE 0 END AS SHARE_AA

                ,SHARE - SHARE_AA AS CRECIMIENTO_SHARE

                ,CASE WHEN TX_MEDIO_AA > 0 THEN TX_MEDIO::NUMERIC / TX_MEDIO_AA - 1 ELSE 0 END AS CRECIMIENTO_TX_MEDIO
                ,CASE WHEN CAT_TX_MEDIO_AA > 0 THEN CAT_TX_MEDIO::NUMERIC / CAT_TX_MEDIO_AA - 1 ELSE 0 END AS CAT_CRECIMIENTO_TX_MEDIO

                ,CASE WHEN PRECIO_MEDIO_AA > 0 THEN PRECIO_MEDIO::NUMERIC / PRECIO_MEDIO_AA - 1 ELSE 0 END AS CRECIMIENTO_PRECIO_MEDIO
                ,CASE WHEN CAT_PRECIO_MEDIO_AA > 0 THEN CAT_PRECIO_MEDIO::NUMERIC / CAT_PRECIO_MEDIO_AA - 1 ELSE 0 END AS CAT_CRECIMIENTO_PRECIO_MEDIO

                ,CASE WHEN VENTA_AA > 0 THEN VENTA_ACTUAL::NUMERIC / VENTA_AA - 1 ELSE 0 END AS CRECIMIENTO_VENTA
                ,CASE WHEN CAT_VENTA_AA > 0 THEN CAT_VENTA_ACTUAL::NUMERIC / CAT_VENTA_AA - 1 ELSE 0 END AS CAT_CRECIMIENTO_VENTA
            
                ,CASE WHEN CLIENTES_ACTUAL > 0 THEN CLIENTES_50::NUMERIC / CLIENTES_ACTUAL END AS "%CLIENTES_50"
                ,CASE WHEN CLIENTES_ACTUAL > 0 THEN CLIENTES_75::NUMERIC / CLIENTES_ACTUAL END AS "%CLIENTES_75"
                ,CASE WHEN CLIENTES_ACTUAL > 0 THEN CLIENTES_100::NUMERIC / CLIENTES_ACTUAL END AS "%CLIENTES_100"
                ,CASE WHEN CLIENTES_ACTUAL > 0 THEN CLIENTES_150::NUMERIC / CLIENTES_ACTUAL END AS "%CLIENTES_150"
                ,CASE WHEN CLIENTES_ACTUAL > 0 THEN CLIENTES_200::NUMERIC / CLIENTES_ACTUAL END AS "%CLIENTES_200"
            
                ,TX_MEDIO_50
                ,TX_MEDIO_75
                ,TX_MEDIO_100
                ,TX_MEDIO_150
                ,TX_MEDIO_200

            FROM __AGG A
            WHERE VENTA_ACTUAL > 0
            ORDER BY 1,2,3,4,5,6,7,8,9,10,11
            );

            -- @WBRESULT VENTA_AGG
            -- SELECT * FROM #VENTA_AGG ORDER BY 1,2,3,4,5,6,7,8,9,10,11;
        '''

        query_bc_resumen = f'''
            --DROP TABLE IF EXISTS #BC;
            CREATE TABLE #BC AS (
                SELECT
                    'VALOR' VARIABLE
                    ,TABLA
                    ,MARCA
                    ,MES
                    ,VENTA_ACTUAL
                    ,CLIENTES_ACTUAL
                    ,CAT_CLIENTES_ACTUAL
                    ,TX_MEDIO
                    ,CRECIMIENTO_VENTA
                    ,CAT_CRECIMIENTO_VENTA
                    ,"%RECOMPRA"
                    ,"%CLIENTES_50"
                    ,"%CLIENTES_75"
                    ,"%CLIENTES_100"
                    ,"%CLIENTES_150"
                    ,"%CLIENTES_200"
                FROM #VENTA_AGG
                WHERE TABLA IN ('BC', 'MARCA')
                ORDER BY 1,2,3,4
            );
            '''

        return [query_meses, query_tx, query_venta, query_recompra, query_venta_marca, query_venta_tienda, query_producto, query_nse_familia, query_bc, query_agg, query_bc_resumen]

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

    def create_tables_bc(self, conn, override=False):
        lis_queries = self.get_query_create_bc_tables()
        table_name = '#BC'

        # Si override no se especifica, la tabla no existe, se crea
        if override is None:
            # Crear tablas BC
            for query in tqdm(lis_queries, desc='Creando tablas BC'):
                conn.execute(query=query)

        # Si override es True, se sobreescribe la tabla
        elif override:
            conn.drop_temporal_tables(table_name)

            for query in tqdm(lis_queries, desc='Creando tablas BC'):
                conn.execute(query)
        # Si override es False, se espera que la tabla exista, no se hace nada. Salir de la función
        else:
            return

        self.df_bc = conn.select(query=f'SELECT * FROM {table_name}')

    def get_bc_data(self):
        return self.df_bc

    # Funcion para obtener los filtros del query de listas
    def __get_filtros_listas(self):
        # Filtro de venta antes
        if self.venta_antes.lower() == 'si':
            venta_antes = 'AND IND_ANTERIOR = 1'
        elif self.venta_antes.lower() == 'no':
            venta_antes = 'AND IND_ANTERIOR = 0'
        else:
            venta_antes = ''

        # Filtro de venta campaña
        if self.venta_camp.lower() == 'si':
            venta_camp = 'AND IND_CAMPANA = 1'
        elif self.venta_camp.lower() == 'no':
            venta_camp = 'AND IND_CAMPANA = 0'
        else:
            venta_camp = ''

        # Filtro de condición antes
        if self.cond_antes.lower() == 'si':
            cond_antes = 'AND IND_ANTERIOR_ELEGIBLE = 1'
        elif self.cond_antes.lower() == 'no':
            cond_antes = 'AND IND_ANTERIOR_ELEGIBLE = 0'
        else:
            cond_antes = ''

        # Filtro de condición campaña
        if self.cond_camp.lower() == 'si':
            cond_camp = 'AND IND_CAMPANA_ELEGIBLE = 1'
        elif self.cond_camp.lower() == 'no':
            cond_camp = 'AND IND_CAMPANA_ELEGIBLE = 0'
        else:
            cond_camp = ''

        # Filtro de condición campaña
        if self.online:
            online = 'AND IND_ONLINE = 1'
        else:
            online = ''
        
        return venta_antes, venta_camp, cond_antes, cond_camp, online

    def get_query_select_po_envios_conteo(self, from_table):
        venta_antes, venta_camp, cond_antes, cond_camp, online = self.__get_filtros_listas()

        query_po_envios = f'''
            SELECT
                *
            FROM (
                SELECT
                    VALID_CONTACT_INFO,
                    CUSTOMER_CODE_TY,
                    ORDEN_SEGMENTO
                FROM {from_table}
                WHERE PO_CAP_12_REC_12_FID_6 = 1
                {venta_antes}
                {venta_camp}
                {cond_antes}
                {cond_camp}
                {online}

            ) AS PO_ENVIOS
            PIVOT (
            COUNT(DISTINCT CUSTOMER_CODE_TY)
            FOR ORDEN_SEGMENTO IN ('1 FID', '2 REC', '3 CAP')
            ) AS PIVOT_TABLE
            ORDER BY 1;
        '''
        return query_po_envios
        
    def get_query_create_listas_envio(self, table_name, from_table):
        venta_antes, venta_camp, cond_antes, cond_camp, online = self.__get_filtros_listas()
        
        # Extraer canales y numero de envios

        print('canales:')
        print(self.canales)

        fid_sms = self.canales['entry_fid_01 sms']
        fid_email = self.canales['entry_fid_02 mail']
        fid_sms_mail = self.canales['entry_fid_03 mail & sms']
        
        rec_sms = self.canales['entry_rec_01 sms']
        rec_email = self.canales['entry_rec_02 mail']
        rec_sms_mail = self.canales['entry_rec_03 mail & sms']

        cap_sms = self.canales['entry_cap_01 sms']
        cap_email = self.canales['entry_cap_02 mail']
        cap_sms_mail = self.canales['entry_cap_03 mail & sms']
        
        porcentaje_gt = 1 - self.ratio_grupo_control

        query = f'''

            --FILTRAR Y DAR ORDEN AL PO
            DROP TABLE IF EXISTS #PO_ORDENADO;
            CREATE TABLE #PO_ORDENADO AS (
            SELECT
                ROW_NUMBER() OVER(PARTITION BY ORDEN_SEGMENTO, VALID_CONTACT_INFO ORDER BY ORDEN_SEGMENTO, VALID_CONTACT_INFO, {f'IND_ONLINE DESC, ' if self.prioridad_online else ''}VENTA_MARCA DESC, VENTA_CAT DESC, CUSTOMER_CODE_TY) N_PREVIO
                ,ROW_NUMBER() OVER(PARTITION BY ORDEN_SEGMENTO, VALID_CONTACT_INFO ORDER BY ORDEN_SEGMENTO, VALID_CONTACT_INFO, {f'IND_ONLINE DESC, ' if self.prioridad_online else ''}VENTA_CAT DESC, CUSTOMER_CODE_TY) N_NUEVO
                ,CUSTOMER_CODE_TY,VALID_CONTACT_INFO,NSE,TIPO_FAMILIA,PO_RECOMPRA_MARCA,RECOMPRA_MARCA,RECOMPRA_6M_CAT,RECOMPRA_6M_MARCA,IND_FID,IND_REC,IND_CAP,ORDEN_SEGMENTO,IND_ONLINE,IND_CAMPANA,IND_CAMPANA_ELEGIBLE,IND_ANTERIOR,IND_ANTERIOR_ELEGIBLE,LAST_INVOICE_DATE,LAST_INVOICE_DATE_MARCA,DIAS_CON_COMPRA,DIAS_CON_COMPRA_MARCA,PROMEDIO_DIAS_RECOMPRA,PROMEDIO_DIAS_RECOMPRA_MARCA,VENTA_CAT,VENTA_MARCA,TX_CAT,TX_MARCA,TICKET_MEDIO_CAT,TICKET_MEDIO_MARCA,VENTA_ELEGIBLE_CAT,VENTA_ELEGIBLE_MARCA,TX_ELEGIBLE_CAT,TX_ELEGIBLE_MARCA,TICKET_MEDIO_ELEGIBLE_CAT,TICKET_MEDIO_ELEGIBLE_MARCA
            FROM {from_table}
            WHERE PO_CAP_12_REC_12_FID_6 = 1 = 1
            {venta_antes}
            {venta_camp}
            {cond_antes}
            {cond_camp}
            {online}
            );

            --CREAR LISTA DE CLIENTES
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} AS (
                SELECT
                *
                FROM #PO_ORDENADO
                WHERE (
                --FID
                   (IND_FID = 1 AND VALID_CONTACT_INFO = '01 SMS'             AND N_PREVIO <= {fid_sms}/{porcentaje_gt})
                OR (IND_FID = 1 AND VALID_CONTACT_INFO = '02 MAIL'            AND N_PREVIO <= {fid_email}/{porcentaje_gt})
                OR (IND_FID = 1 AND VALID_CONTACT_INFO = '03 MAIL & SMS'      AND N_PREVIO <= {fid_sms_mail}/{porcentaje_gt})
                --REC
                OR (IND_REC = 1 AND VALID_CONTACT_INFO = '01 SMS'             AND N_PREVIO <= {rec_sms}/{porcentaje_gt})
                OR (IND_REC = 1 AND VALID_CONTACT_INFO = '02 MAIL'            AND N_PREVIO <= {rec_email}/{porcentaje_gt})
                OR (IND_REC = 1 AND VALID_CONTACT_INFO = '03 MAIL & SMS'      AND N_PREVIO <= {rec_sms_mail}/{porcentaje_gt})
                --CAP
                OR (IND_CAP = 1 AND VALID_CONTACT_INFO = '01 SMS'             AND N_NUEVO <= {cap_sms}/{porcentaje_gt})
                OR (IND_CAP = 1 AND VALID_CONTACT_INFO = '02 MAIL'            AND N_NUEVO <= {cap_email}/{porcentaje_gt})
                OR (IND_CAP = 1 AND VALID_CONTACT_INFO = '03 MAIL & SMS'      AND N_NUEVO <= {cap_sms_mail}/{porcentaje_gt})
                )
            );
        '''
        return query

    def create_table_po_envios_conteo(self, conn):
        from_table = '#PO'
        query = self.get_query_select_po_envios_conteo(from_table=from_table)
        self.df_po_conteo = conn.select(query=query)
    
    def create_table_listas_envio(self, conn):
        table_name = '#LISTAS_ENVIO'
        from_table = '#PO'
        query = self.get_query_create_listas_envio(table_name, from_table=from_table)

        conn.execute(query=query)

        query = f'SELECT * FROM {table_name} ORDER BY ORDEN_SEGMENTO, VALID_CONTACT_INFO, N_PREVIO, N_NUEVO'
        
        self.df_listas_total = conn.select(query=query)
        
        # Separar las listas de envio por canal y agregar a un diccionario
        df_sms = self.df_listas_total[self.df_listas_total['valid_contact_info'] == '01 SMS'][['customer_code_ty']]
        df_email = self.df_listas_total[self.df_listas_total['valid_contact_info'] == '02 MAIL'][['customer_code_ty']]
        df_sms_email = self.df_listas_total[self.df_listas_total['valid_contact_info'] == '03 MAIL & SMS'][['customer_code_ty']]

        lis_df = [df_sms, df_email, df_sms_email]
        lis_names = ['list_sms', 'list_mail', 'list_sms_mail']

        # Guardar listas de envio en un diccionario. Guardar solo los df no vacios
        self.dict_listas_envios = {}
        for df, name in zip(lis_df, lis_names):
            if not df.empty:
                self.dict_listas_envios[name] = df

        # Agregar lista de envíos total
        self.dict_listas_envios['list_total'] = self.df_listas_total

    def separar_listas_envio(self):
        df_sms = self.df_listas_total[self.df_listas_total['valid_contact_info'] == '01 SMS']
        df_email = self.df_listas_total[self.df_listas_total['valid_contact_info'] == '02 MAIL']
        df_sms_email = self.df_listas_total[self.df_listas_total['valid_contact_info'] == '03 MAIL & SMS']

        # Separar listas de envio por canal y segmento y agregar a un diccionario
        df_sms_fid = df_sms[df_sms['orden_segmento'] == '1 FID'][['customer_code_ty']]
        df_sms_rec = df_sms[df_sms['orden_segmento'] == '2 REC'][['customer_code_ty']]
        df_sms_cap = df_sms[df_sms['orden_segmento'] == '3 CAP'][['customer_code_ty']]

        df_email_fid = df_email[df_email['orden_segmento'] == '1 FID'][['customer_code_ty']]
        df_email_rec = df_email[df_email['orden_segmento'] == '2 REC'][['customer_code_ty']]
        df_email_cap = df_email[df_email['orden_segmento'] == '3 CAP'][['customer_code_ty']]

        df_sms_email_fid = df_sms_email[df_sms_email['orden_segmento'] == '1 FID'][['customer_code_ty']]
        df_sms_email_rec = df_sms_email[df_sms_email['orden_segmento'] == '2 REC'][['customer_code_ty']]
        df_sms_email_cap = df_sms_email[df_sms_email['orden_segmento'] == '3 CAP'][['customer_code_ty']]

        lis_df = [df_sms_fid, df_sms_rec, df_sms_cap, df_email_fid, df_email_rec, df_email_cap, df_sms_email_fid, df_sms_email_rec, df_sms_email_cap]
        lis_names = ['list_sms_fid', 'list_sms_rec', 'list_sms_cap', 'list_email_fid', 'list_email_rec', 'list_email_cap', 'list_sms_email_fid', 'list_sms_email_rec', 'list_sms_email_cap']

        # Guardar listas de envio en un diccionario. Guardar solo los df no vacios
        self.dict_listas_envios = {}
        for df, name in zip(lis_df, lis_names):
            if not df.empty:
                self.dict_listas_envios[name] = df

        # Agregar lista de envíos total
        self.dict_listas_envios['list_total'] = self.df_listas_total
        