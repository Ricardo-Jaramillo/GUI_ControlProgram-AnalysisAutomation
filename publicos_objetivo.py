import pandas as pd
from tqdm import tqdm

# Create a Class to handle the Monetizacion data that inherits from the Conn class
class PublicosObjetivo():
    
    def __init__(self):
        self.df_pos_agg = pd.DataFrame()
        self.df_bc = pd.DataFrame()
        self.df_analisis_bc = pd.DataFrame()
        self.dict_df_analisis_bc = {}
        self.df_listas_envio = pd.DataFrame()
        self.dict_listas_envios = {}
        self.set_pos_variables()
        self.dict_bc_analisis_var = {}

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
        {f'INNER JOIN DIM_STORE B ON A.STORE_KEY = B.STORE_KEY AND A.STORE_CODE = B.STORE_CODE AND A.STORE_CODE IN ({self.tiendas})' if self.tiendas else ''}
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
        
    def get_query_analisis_bc_agg(self):
        query_tx = f'''
            -- CREAR TABLA DE TX
            DROP TABLE IF EXISTS #TX_CONDICION;
            CREATE TABLE #TX_CONDICION AS (
            -- /*
            SELECT
                INVOICE_NO
                ,PROVEEDOR
                ,MARCA
                ,IND_MARCA
                ,IND_ONLINE

                ,CASE WHEN INVOICE_DATE BETWEEN {self.dict_bc_analisis_var['date_dash']} THEN INVOICE_DATE ELSE DATE_ADD('MONTH', 12, INVOICE_DATE) END AS INVOICE_DATE_ADJ
                ,CASE WHEN INVOICE_DATE BETWEEN {self.dict_bc_analisis_var['date_dash']} THEN 1 ELSE 0 END AS IND_ACTUAL
                ,CASE WHEN INVOICE_DATE BETWEEN {self.dict_bc_analisis_var['date_dash_campana']} THEN 1 ELSE 0 END AS IND_CAMPANA
                
                ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_analisis_var['condicion_compra']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE
                ,CASE WHEN SUM(SALE_NET_VAL) BETWEEN 0 AND '{self.dict_bc_analisis_var['condicion_1']}' THEN 1 ELSE 0 END AS IND_CONDICION_1
                ,CASE WHEN SUM(SALE_NET_VAL) BETWEEN '{self.dict_bc_analisis_var['condicion_1']}' AND '{self.dict_bc_analisis_var['condicion_2']}' THEN 1 ELSE 0 END AS IND_CONDICION_2
                ,CASE WHEN SUM(SALE_NET_VAL) BETWEEN '{self.dict_bc_analisis_var['condicion_2']}' AND '{self.dict_bc_analisis_var['condicion_3']}' THEN 1 ELSE 0 END AS IND_CONDICION_3
                ,CASE WHEN SUM(SALE_NET_VAL) BETWEEN '{self.dict_bc_analisis_var['condicion_3']}' AND '{self.dict_bc_analisis_var['condicion_4']}' THEN 1 ELSE 0 END AS IND_CONDICION_4
                ,CASE WHEN SUM(SALE_NET_VAL) BETWEEN '{self.dict_bc_analisis_var['condicion_4']}' AND '{self.dict_bc_analisis_var['condicion_5']}' THEN 1 ELSE 0 END AS IND_CONDICION_5
                ,CASE WHEN SUM(SALE_NET_VAL) > '{self.dict_bc_analisis_var['condicion_5']}' THEN 1 ELSE 0 END AS IND_CONDICION_6
                
            FROM FCT_SALE_LINE
            INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
            LEFT JOIN (SELECT DISTINCT INVOICE_NO, CASE WHEN CHANNEL_TYPE IN ('WEB','APP','CC HY') THEN 1 ELSE 0 END IND_ONLINE FROM FCT_SALE_HEADER) F USING(INVOICE_NO)
            WHERE INVOICE_DATE BETWEEN {self.dict_bc_analisis_var['date_dash']} OR INVOICE_DATE BETWEEN {self.dict_bc_analisis_var['date_dash_aa']}
            AND BUSINESS_TYPE = 'R'
            AND SALE_NET_VAL > 0
            --   AND UI_FLG = 0
            GROUP BY 1,2,3,4,5,6,7,8--,9,10,11,12
            {f'HAVING IND_ELEGIBLE = 1' if self.dict_bc_analisis_var['elegible'] else ''}

            UNION ALL
            -- */
            -- /*  
            SELECT
                INVOICE_NO
                ,PROVEEDOR
                ,MARCA
                ,IND_MARCA
                ,IND_ONLINE

                ,CASE WHEN INVOICE_DATE BETWEEN {self.dict_bc_analisis_var['date_dash']} THEN INVOICE_DATE ELSE DATE_ADD('MONTH', 12, INVOICE_DATE) END AS INVOICE_DATE_ADJ
                ,CASE WHEN INVOICE_DATE BETWEEN {self.dict_bc_analisis_var['date_dash']} THEN 1 ELSE 0 END AS IND_ACTUAL
                ,CASE WHEN INVOICE_DATE BETWEEN {self.dict_bc_analisis_var['date_dash_campana']} THEN 1 ELSE 0 END AS IND_CAMPANA
                
                ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.dict_bc_analisis_var['condicion_compra']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE
                ,CASE WHEN SUM(SALE_NET_VAL) BETWEEN 0 AND '{self.dict_bc_analisis_var['condicion_1']}' THEN 1 ELSE 0 END AS IND_CONDICION_1
                ,CASE WHEN SUM(SALE_NET_VAL) BETWEEN '{self.dict_bc_analisis_var['condicion_1']}' AND '{self.dict_bc_analisis_var['condicion_2']}' THEN 1 ELSE 0 END AS IND_CONDICION_2
                ,CASE WHEN SUM(SALE_NET_VAL) BETWEEN '{self.dict_bc_analisis_var['condicion_2']}' AND '{self.dict_bc_analisis_var['condicion_3']}' THEN 1 ELSE 0 END AS IND_CONDICION_3
                ,CASE WHEN SUM(SALE_NET_VAL) BETWEEN '{self.dict_bc_analisis_var['condicion_3']}' AND '{self.dict_bc_analisis_var['condicion_4']}' THEN 1 ELSE 0 END AS IND_CONDICION_4
                ,CASE WHEN SUM(SALE_NET_VAL) BETWEEN '{self.dict_bc_analisis_var['condicion_4']}' AND '{self.dict_bc_analisis_var['condicion_5']}' THEN 1 ELSE 0 END AS IND_CONDICION_5
                ,CASE WHEN SUM(SALE_NET_VAL) > '{self.dict_bc_analisis_var['condicion_5']}' THEN 1 ELSE 0 END AS IND_CONDICION_6
                
            FROM FCT_SALE_LINE_NM
            INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
            LEFT JOIN (SELECT DISTINCT INVOICE_NO, CASE WHEN CHANNEL_TYPE IN ('WEB','APP','CC HY') THEN 1 ELSE 0 END IND_ONLINE FROM FCT_SALE_HEADER_NM) F USING(INVOICE_NO)
            WHERE INVOICE_DATE BETWEEN {self.dict_bc_analisis_var['date_dash']} OR INVOICE_DATE BETWEEN {self.dict_bc_analisis_var['date_dash_aa']}
            AND BUSINESS_TYPE = 'R'
            AND SALE_NET_VAL > 0
            --   AND UI_FLG = 0
            GROUP BY 1,2,3,4,5,6,7,8--,9,10,11,12
            {f'HAVING IND_ELEGIBLE = 1' if self.dict_bc_analisis_var['elegible'] else ''}
            --   */
            );
        '''

        query_venta = '''
            DROP TABLE IF EXISTS #VENTA;
            CREATE TABLE #VENTA AS (
            WITH __TX AS (
                SELECT DISTINCT
                INVOICE_NO
                FROM #TX_CONDICION
            )
                SELECT
                1::INT AS IND_MC, CUSTOMER_CODE_TY, INVOICE_NO, INVOICE_DATE, PRODUCT_CODE, STORE_CODE, STORE_KEY, SUM(SALE_NET_VAL) VENTA, SUM(SALE_TOT_QTY) UNIDADES
                FROM FCT_SALE_LINE
            --     AND UI_FLG = 0
                INNER JOIN __TX USING(INVOICE_NO)
                WHERE BUSINESS_TYPE = 'R'
                AND SALE_NET_VAL > 0
                GROUP BY 1,2,3,4,5,6,7
            
                UNION ALL
            
                SELECT
                0::INT IND_MC, NULL, INVOICE_NO, INVOICE_DATE, PRODUCT_CODE, STORE_CODE, STORE_KEY, SUM(SALE_NET_VAL) VENTA, SUM(SALE_TOT_QTY) UNIDADES
                FROM FCT_SALE_LINE_NM
            --     AND UI_FLG = 0
                INNER JOIN __TX USING(INVOICE_NO)
                WHERE BUSINESS_TYPE = 'R'
                AND SALE_NET_VAL > 0
                GROUP BY 1,2,3,4,5,6,7
            );
        '''

        query_agg_total = '''
            DROP TABLE IF EXISTS #AGG_TOTAL;
            CREATE TABLE #AGG_TOTAL AS (
            SELECT
            
                CASE
                WHEN DATE_TRUNC('MONTH', INVOICE_DATE_ADJ)::DATE::VARCHAR <> '' THEN 'MES'
                WHEN REGION <> '' AND STATE <> '' AND FORMATO_TIENDA <> '' AND STORE_DESCRIPTION <> '' THEN 'TIENDA'
                WHEN FORMATO_TIENDA <> '' THEN 'FORMATO'
                WHEN REGION <> '' AND STATE <> '' THEN 'ESTADO'
                WHEN NSE <> '' THEN 'NSE'
                WHEN TIPO_FAMILIA <> '' THEN 'FAMILIA'
                WHEN CLASS_DESC <> '' AND SUBCLASS_DESC <> '' AND PROD_TYPE_DESC <> '' AND PRODUCT_DESCRIPTION <> '' THEN 'PRODUCTO'
                WHEN CLASS_DESC <> '' AND SUBCLASS_DESC <> '' AND PROD_TYPE_DESC <> '' THEN 'PROD_TYPE'
                WHEN CLASS_DESC <> '' AND SUBCLASS_DESC <> '' THEN 'SUBCLASS'
                WHEN CLASS_DESC <> '' THEN 'CLASS'
                ELSE 'TOTAL'
                END AS TABLA
                
                ,1::INT IND_MC
                ,'TOTAL' PROVEEDOR
                ,'TOTAL' MARCA

                ,DATE_TRUNC('MONTH', INVOICE_DATE_ADJ)::DATE::VARCHAR MES

                ,REGION
                ,STATE
                ,FORMATO_TIENDA
                ,STORE_DESCRIPTION
                
                ,NSE --COALESCE(NSE, 'NO SEGMENTADO') AS NSE
                ,TIPO_FAMILIA --COALESCE(TIPO_FAMILIA, 'NO SEGMENTADO') AS TIPO_FAMILIA
                
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
                ,PRODUCT_DESCRIPTION
                
                --MARCA
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 THEN VENTA END) VENTA_ACTUAL
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 THEN UNIDADES END) UNIDADES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 THEN INVOICE_NO END) TX_ACTUAL

                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 0 THEN VENTA END) VENTA_AA
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 0 THEN UNIDADES END) UNIDADES_AA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 0 THEN CUSTOMER_CODE_TY END) CLIENTES_AA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 0 THEN INVOICE_NO END) TX_AA
                
                --CAT
                ,SUM(CASE WHEN IND_ACTUAL = 1 THEN VENTA END) CAT_VENTA_ACTUAL
                ,SUM(CASE WHEN IND_ACTUAL = 1 THEN UNIDADES END) CAT_UNIDADES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 1 THEN CUSTOMER_CODE_TY END) CAT_CLIENTES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 1 THEN INVOICE_NO END) CAT_TX_ACTUAL

                ,SUM(CASE WHEN IND_ACTUAL = 0 THEN VENTA END) CAT_VENTA_AA
                ,SUM(CASE WHEN IND_ACTUAL = 0 THEN UNIDADES END) CAT_UNIDADES_AA
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 0 THEN CUSTOMER_CODE_TY END) CAT_CLIENTES_AA
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 0 THEN INVOICE_NO END) CAT_TX_AA
            
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN IND_ACTUAL = 1 AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE
                
                ,SUM(VENTA_ACTUAL) OVER(
                    PARTITION BY TABLA 
                    ORDER BY VENTA_ACTUAL DESC, CAT_VENTA_ACTUAL DESC 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS VENTA_ACTUAL_ANUAL

                ,SUM(VENTA_AA) OVER(
                    PARTITION BY TABLA 
                    ORDER BY VENTA_ACTUAL DESC, CAT_VENTA_ACTUAL DESC 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(
                    PARTITION BY TABLA 
                    ORDER BY VENTA_ACTUAL DESC, CAT_VENTA_ACTUAL DESC 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS CAT_VENTA_ACTUAL_ANUAL

                ,SUM(CAT_VENTA_AA) OVER(
                    PARTITION BY TABLA 
                    ORDER BY VENTA_ACTUAL DESC, CAT_VENTA_ACTUAL DESC 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS CAT_VENTA_AA_ANUAL
                
                --CONDICION COMPRA
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_1 = 1 THEN VENTA END) VENTA_CONDICION_1
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_2 = 1 THEN VENTA END) VENTA_CONDICION_2
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_3 = 1 THEN VENTA END) VENTA_CONDICION_3
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_4 = 1 THEN VENTA END) VENTA_CONDICION_4
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_5 = 1 THEN VENTA END) VENTA_CONDICION_5
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_6 = 1 THEN VENTA END) VENTA_CONDICION_6

                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_1 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_CONDICION_1
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_2 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_CONDICION_2
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_3 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_CONDICION_3
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_4 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_CONDICION_4
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_5 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_CONDICION_5
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_6 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_CONDICION_6

                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_1 = 1 THEN INVOICE_NO END) TX_CONDICION_1
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_2 = 1 THEN INVOICE_NO END) TX_CONDICION_2
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_3 = 1 THEN INVOICE_NO END) TX_CONDICION_3
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_4 = 1 THEN INVOICE_NO END) TX_CONDICION_4
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_5 = 1 THEN INVOICE_NO END) TX_CONDICION_5
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_6 = 1 THEN INVOICE_NO END) TX_CONDICION_6

            FROM #VENTA
            INNER JOIN #TX_CONDICION USING(INVOICE_NO)
            INNER JOIN #PRODUCTOS USING(PRODUCT_CODE, PROVEEDOR, MARCA, IND_MARCA)
            LEFT JOIN CHEDRAUI.V_STORE USING(STORE_CODE, STORE_KEY)
            LEFT JOIN CHEDRAUI.V_CUSTOMER_CONTACT ON CUSTOMER_CODE_TY = CUSTOMER_CODE
            --   LEFT JOIN DIM_PRODUCT USING(PRODUCT_CODE)
            --   GROUP BY ROLLUP(1,2,3,4,5,6,7,8)
            WHERE IND_MC = 1
            AND IND_DUPLICADO = 0
            --   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            GROUP BY GROUPING SETS (
                ()
                ,(MES)
                ,(REGION, STATE)
                ,(FORMATO_TIENDA)
                ,(REGION, STATE, FORMATO_TIENDA, STORE_DESCRIPTION)
                ,(NSE)
                ,(TIPO_FAMILIA)
                ,(CLASS_DESC)
                ,(CLASS_DESC, SUBCLASS_DESC)
                ,(CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                ,(CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC, PRODUCT_DESCRIPTION)
            --     ,(REGION, STATE, PRODUCT_DESCRIPTION)
                )
            );
        '''

        query_agg_campana = '''
            DROP TABLE IF EXISTS #AGG_CAMPANA;
            CREATE TABLE #AGG_CAMPANA AS (
            SELECT
            
                CASE
            --       WHEN DATE_TRUNC('MONTH', INVOICE_DATE_ADJ)::DATE::VARCHAR <> '' THEN 'MES'
                WHEN REGION <> '' AND STATE <> '' AND FORMATO_TIENDA <> '' AND STORE_DESCRIPTION <> '' THEN 'TIENDA'
                WHEN FORMATO_TIENDA <> '' THEN 'FORMATO'
                WHEN REGION <> '' AND STATE <> '' THEN 'ESTADO'
                WHEN NSE <> '' THEN 'NSE'
                WHEN TIPO_FAMILIA <> '' THEN 'FAMILIA'
                WHEN CLASS_DESC <> '' AND SUBCLASS_DESC <> '' AND PROD_TYPE_DESC <> '' AND PRODUCT_DESCRIPTION <> '' THEN 'PRODUCTO'
                WHEN CLASS_DESC <> '' AND SUBCLASS_DESC <> '' AND PROD_TYPE_DESC <> '' THEN 'PROD_TYPE'
                WHEN CLASS_DESC <> '' AND SUBCLASS_DESC <> '' THEN 'SUBCLASS'
                WHEN CLASS_DESC <> '' THEN 'CLASS'
                ELSE 'CAMPANA'
                END AS TABLA
                
                ,1::INT IND_MC
                ,'TOTAL' PROVEEDOR
                ,'TOTAL' MARCA

                ,'CAMPANA' MES -- DATE_TRUNC('MONTH', INVOICE_DATE_ADJ)::DATE::VARCHAR MES

                ,REGION
                ,STATE
                ,FORMATO_TIENDA
                ,STORE_DESCRIPTION
                
                ,NSE --COALESCE(NSE, 'NO SEGMENTADO') AS NSE
                ,TIPO_FAMILIA --COALESCE(TIPO_FAMILIA, 'NO SEGMENTADO') AS TIPO_FAMILIA
                
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
                ,PRODUCT_DESCRIPTION
                
                --MARCA
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 THEN VENTA END) VENTA_ACTUAL
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 THEN UNIDADES END) UNIDADES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 THEN INVOICE_NO END) TX_ACTUAL

                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 0 THEN VENTA END) VENTA_AA
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 0 THEN UNIDADES END) UNIDADES_AA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 0 THEN CUSTOMER_CODE_TY END) CLIENTES_AA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 0 THEN INVOICE_NO END) TX_AA
                
                --CAT
                ,SUM(CASE WHEN IND_ACTUAL = 1 THEN VENTA END) CAT_VENTA_ACTUAL
                ,SUM(CASE WHEN IND_ACTUAL = 1 THEN UNIDADES END) CAT_UNIDADES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 1 THEN CUSTOMER_CODE_TY END) CAT_CLIENTES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 1 THEN INVOICE_NO END) CAT_TX_ACTUAL

                ,SUM(CASE WHEN IND_ACTUAL = 0 THEN VENTA END) CAT_VENTA_AA
                ,SUM(CASE WHEN IND_ACTUAL = 0 THEN UNIDADES END) CAT_UNIDADES_AA
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 0 THEN CUSTOMER_CODE_TY END) CAT_CLIENTES_AA
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 0 THEN INVOICE_NO END) CAT_TX_AA
            
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN IND_ACTUAL = 1 AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE
                
                ,SUM(VENTA_ACTUAL) OVER(
                    PARTITION BY TABLA 
                    ORDER BY VENTA_ACTUAL DESC, CAT_VENTA_ACTUAL DESC 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS VENTA_ACTUAL_ANUAL

                ,SUM(VENTA_AA) OVER(
                    PARTITION BY TABLA 
                    ORDER BY VENTA_ACTUAL DESC, CAT_VENTA_ACTUAL DESC 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(
                    PARTITION BY TABLA 
                    ORDER BY VENTA_ACTUAL DESC, CAT_VENTA_ACTUAL DESC 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS CAT_VENTA_ACTUAL_ANUAL

                ,SUM(CAT_VENTA_AA) OVER(
                    PARTITION BY TABLA 
                    ORDER BY VENTA_ACTUAL DESC, CAT_VENTA_ACTUAL DESC 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS CAT_VENTA_AA_ANUAL
                
                --CONDICIONES COMPRA
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_1 = 1 THEN VENTA END) VENTA_CONDICION_1
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_2 = 1 THEN VENTA END) VENTA_CONDICION_2
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_3 = 1 THEN VENTA END) VENTA_CONDICION_3
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_4 = 1 THEN VENTA END) VENTA_CONDICION_4
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_5 = 1 THEN VENTA END) VENTA_CONDICION_5
                ,SUM(CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_6 = 1 THEN VENTA END) VENTA_CONDICION_6

                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_1 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_CONDICION_1
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_2 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_CONDICION_2
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_3 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_CONDICION_3
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_4 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_CONDICION_4
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_5 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_CONDICION_5
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_6 = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_CONDICION_6

                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_1 = 1 THEN INVOICE_NO END) TX_CONDICION_1
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_2 = 1 THEN INVOICE_NO END) TX_CONDICION_2
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_3 = 1 THEN INVOICE_NO END) TX_CONDICION_3
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_4 = 1 THEN INVOICE_NO END) TX_CONDICION_4
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_5 = 1 THEN INVOICE_NO END) TX_CONDICION_5
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND IND_ACTUAL = 1 AND IND_CONDICION_6 = 1 THEN INVOICE_NO END) TX_CONDICION_6

            FROM #VENTA
            INNER JOIN #TX_CONDICION USING(INVOICE_NO)
            INNER JOIN #PRODUCTOS USING(PRODUCT_CODE, PROVEEDOR, MARCA, IND_MARCA)
            LEFT JOIN CHEDRAUI.V_STORE USING(STORE_CODE, STORE_KEY)
            LEFT JOIN CHEDRAUI.V_CUSTOMER_CONTACT ON CUSTOMER_CODE_TY = CUSTOMER_CODE
            --   LEFT JOIN DIM_PRODUCT USING(PRODUCT_CODE)
            --   GROUP BY ROLLUP(1,2,3,4,5,6,7,8)
            WHERE IND_MC = 1
            AND IND_DUPLICADO = 0
            AND IND_CAMPANA = 1
            --   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            GROUP BY GROUPING SETS (
                ()
                ,(REGION, STATE)
                ,(FORMATO_TIENDA)
                ,(REGION, STATE, FORMATO_TIENDA, STORE_DESCRIPTION)
                ,(NSE)
                ,(TIPO_FAMILIA)
                ,(CLASS_DESC)
                ,(CLASS_DESC, SUBCLASS_DESC)
                ,(CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                ,(CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC, PRODUCT_DESCRIPTION)
                )
            );
        '''

        query_agg = '''
            --TABLA AGG
            DROP TABLE IF EXISTS #AGG;
            CREATE TABLE #AGG AS (
            WITH __AGG AS (
                SELECT * FROM #AGG_TOTAL
                UNION ALL
                SELECT * FROM #AGG_CAMPANA
            )
            ,__VENTA AS (
                SELECT
                *
                ,CASE WHEN CLIENTES_ACTUAL > 0 THEN TX_ACTUAL::NUMERIC / CLIENTES_ACTUAL ELSE 0 END AS FRECUENCIA_TX
                ,CASE WHEN CLIENTES_AA > 0 THEN TX_AA::NUMERIC / CLIENTES_AA ELSE 0 END AS FRECUENCIA_TX_AA
                ,CASE WHEN CAT_CLIENTES_ACTUAL > 0 THEN CAT_TX_ACTUAL::NUMERIC / CAT_CLIENTES_ACTUAL ELSE 0 END AS CAT_FRECUENCIA_TX
                ,CASE WHEN CAT_CLIENTES_AA > 0 THEN CAT_TX_AA::NUMERIC / CAT_CLIENTES_AA ELSE 0 END AS CAT_FRECUENCIA_TX_AA

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

                ,CASE WHEN FRECUENCIA_TX_AA > 0 THEN FRECUENCIA_TX::NUMERIC / FRECUENCIA_TX_AA - 1 ELSE 0 END AS CRECIMIENTO_FRECUENCIA_TX
                ,CASE WHEN CAT_FRECUENCIA_TX_AA > 0 THEN FRECUENCIA_TX::NUMERIC / CAT_FRECUENCIA_TX_AA - 1 ELSE 0 END AS CAT_CRECIMIENTO_FRECUENCIA_TX
                
                ,CASE WHEN TX_MEDIO_AA > 0 THEN TX_MEDIO::NUMERIC / TX_MEDIO_AA - 1 ELSE 0 END AS CRECIMIENTO_TX_MEDIO
                ,CASE WHEN CAT_TX_MEDIO_AA > 0 THEN CAT_TX_MEDIO::NUMERIC / CAT_TX_MEDIO_AA - 1 ELSE 0 END AS CAT_CRECIMIENTO_TX_MEDIO

                ,CASE WHEN PRECIO_MEDIO_AA > 0 THEN PRECIO_MEDIO::NUMERIC / PRECIO_MEDIO_AA - 1 ELSE 0 END AS CRECIMIENTO_PRECIO_MEDIO
                ,CASE WHEN CAT_PRECIO_MEDIO_AA > 0 THEN CAT_PRECIO_MEDIO::NUMERIC / CAT_PRECIO_MEDIO_AA - 1 ELSE 0 END AS CAT_CRECIMIENTO_PRECIO_MEDIO

                ,CASE WHEN VENTA_AA > 0 THEN VENTA_ACTUAL::NUMERIC / VENTA_AA - 1 ELSE 0 END AS CRECIMIENTO_VENTA
                ,CASE WHEN CAT_VENTA_AA > 0 THEN CAT_VENTA_ACTUAL::NUMERIC / CAT_VENTA_AA - 1 ELSE 0 END AS CAT_CRECIMIENTO_VENTA
                
                ,CASE WHEN VENTA_AA_ANUAL > 0 THEN VENTA_ACTUAL_ANUAL::NUMERIC / VENTA_AA_ANUAL - 1 ELSE 0 END AS CRECIMIENTO_VENTA_ANUAL
                ,CASE WHEN CAT_VENTA_AA_ANUAL > 0 THEN CAT_VENTA_ACTUAL_ANUAL::NUMERIC / CAT_VENTA_AA_ANUAL - 1 ELSE 0 END AS CAT_CRECIMIENTO_VENTA_ANUAL
            
                ,CASE WHEN VENTA_ACTUAL > 0 THEN VENTA_ACTUAL_ONLINE::NUMERIC / VENTA_ACTUAL ELSE 0 END AS "%VENTA_ONLINE"
                ,CASE WHEN CAT_VENTA_ACTUAL > 0 THEN CAT_VENTA_ACTUAL_ONLINE::NUMERIC / CAT_VENTA_ACTUAL ELSE 0 END AS "%CAT_VENTA_ONLINE"

                ,CASE WHEN CLIENTES_ACTUAL > 0 THEN CLIENTES_CONDICION_1::NUMERIC / CLIENTES_ACTUAL END AS "%CLIENTES_CONDICION_1"
                ,CASE WHEN CLIENTES_ACTUAL > 0 THEN CLIENTES_CONDICION_2::NUMERIC / CLIENTES_ACTUAL END AS "%CLIENTES_CONDICION_2"
                ,CASE WHEN CLIENTES_ACTUAL > 0 THEN CLIENTES_CONDICION_3::NUMERIC / CLIENTES_ACTUAL END AS "%CLIENTES_CONDICION_3"
                ,CASE WHEN CLIENTES_ACTUAL > 0 THEN CLIENTES_CONDICION_4::NUMERIC / CLIENTES_ACTUAL END AS "%CLIENTES_CONDICION_4"
                ,CASE WHEN CLIENTES_ACTUAL > 0 THEN CLIENTES_CONDICION_5::NUMERIC / CLIENTES_ACTUAL END AS "%CLIENTES_CONDICION_5"
                ,CASE WHEN CLIENTES_ACTUAL > 0 THEN CLIENTES_CONDICION_6::NUMERIC / CLIENTES_ACTUAL END AS "%CLIENTES_CONDICION_6"
            
                ,CASE WHEN TX_CONDICION_1 > 0 THEN VENTA_CONDICION_1::NUMERIC / TX_CONDICION_1 ELSE 0 END AS TX_MEDIO_CONDICION_1
                ,CASE WHEN TX_CONDICION_2 > 0 THEN VENTA_CONDICION_2::NUMERIC / TX_CONDICION_2 ELSE 0 END AS TX_MEDIO_CONDICION_2
                ,CASE WHEN TX_CONDICION_3 > 0 THEN VENTA_CONDICION_3::NUMERIC / TX_CONDICION_3 ELSE 0 END AS TX_MEDIO_CONDICION_3
                ,CASE WHEN TX_CONDICION_4 > 0 THEN VENTA_CONDICION_4::NUMERIC / TX_CONDICION_4 ELSE 0 END AS TX_MEDIO_CONDICION_4
                ,CASE WHEN TX_CONDICION_5 > 0 THEN VENTA_CONDICION_5::NUMERIC / TX_CONDICION_5 ELSE 0 END AS TX_MEDIO_CONDICION_5
                ,CASE WHEN TX_CONDICION_6 > 0 THEN VENTA_CONDICION_6::NUMERIC / TX_CONDICION_6 ELSE 0 END AS TX_MEDIO_CONDICION_6

                FROM __AGG
                )
            SELECT
                *
            FROM __VENTA
            --   LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            );
        '''

        return [query_tx, query_venta, query_agg_total, query_agg_campana, query_agg]

    def get_query_analisis_bc_segmentos(self):
        query_meses = f'''
            --CREAR FECHAS DE MES
            DROP TABLE IF EXISTS #MESES;
            CREATE TABLE #MESES AS (
            WITH __MESES AS (
            SELECT DISTINCT
                DATE_TRUNC('MONTH', INVOICE_DATE)::DATE::VARCHAR MES
            --     ,EXTRACT(MONTH FROM INVOICE_DATE) MES_NUMERO
                ,CASE
                WHEN INVOICE_DATE BETWEEN {self.dict_bc_analisis_var['date_dash']} THEN 'ACTUAL'
                WHEN INVOICE_DATE BETWEEN {self.dict_bc_analisis_var['date_dash_aa']} THEN 'ANO_ANTERIOR'
                END AS PERIODO
                ,CASE WHEN INVOICE_DATE BETWEEN {self.dict_bc_analisis_var['date_dash_campana']} THEN 1 ELSE 0 END AS IND_CAMPANA
                
            FROM FCT_SALE_HEADER
            WHERE INVOICE_DATE BETWEEN '{self.dict_bc_analisis_var['vigencia_ini_aa']}' AND '{self.dict_bc_analisis_var['vigencia_fin']}'
            )
            SELECT
            *
            ,ROW_NUMBER() OVER(ORDER BY MES) IND_MES
            FROM __MESES
            );
        '''

        query_segmentos = '''
            DROP TABLE IF EXISTS #INDICADORES;
            CREATE TABLE #INDICADORES AS (
            SELECT
                CUSTOMER_CODE_TY
                ,'TOTAL' PROVEEDOR
                ,'TOTAL' MARCA
                
                --INDICADORES DE COMPRA
                --MARCA
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
                ,MAX(CASE WHEN IND_MES = 13 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_13
                ,MAX(CASE WHEN IND_MES = 14 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_14
                ,MAX(CASE WHEN IND_MES = 15 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_15
                ,MAX(CASE WHEN IND_MES = 16 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_16
                ,MAX(CASE WHEN IND_MES = 17 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_17
                ,MAX(CASE WHEN IND_MES = 18 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_18
                ,MAX(CASE WHEN IND_MES = 19 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_19
                ,MAX(CASE WHEN IND_MES = 20 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_20
                ,MAX(CASE WHEN IND_MES = 21 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_21
                ,MAX(CASE WHEN IND_MES = 22 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_22
                ,MAX(CASE WHEN IND_MES = 23 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_23
                ,MAX(CASE WHEN IND_MES = 24 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_24
                ,MAX(CASE WHEN IND_MES = 25 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_25
                ,MAX(CASE WHEN IND_MES = 26 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_26
                ,MAX(CASE WHEN IND_MES = 27 AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_27

                --CAT
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
                ,MAX(CASE WHEN IND_MES = 13 THEN 1 ELSE 0 END) CAT_IND_13
                ,MAX(CASE WHEN IND_MES = 14 THEN 1 ELSE 0 END) CAT_IND_14
                ,MAX(CASE WHEN IND_MES = 15 THEN 1 ELSE 0 END) CAT_IND_15
                ,MAX(CASE WHEN IND_MES = 16 THEN 1 ELSE 0 END) CAT_IND_16
                ,MAX(CASE WHEN IND_MES = 17 THEN 1 ELSE 0 END) CAT_IND_17
                ,MAX(CASE WHEN IND_MES = 18 THEN 1 ELSE 0 END) CAT_IND_18
                ,MAX(CASE WHEN IND_MES = 19 THEN 1 ELSE 0 END) CAT_IND_19
                ,MAX(CASE WHEN IND_MES = 20 THEN 1 ELSE 0 END) CAT_IND_20
                ,MAX(CASE WHEN IND_MES = 21 THEN 1 ELSE 0 END) CAT_IND_21
                ,MAX(CASE WHEN IND_MES = 22 THEN 1 ELSE 0 END) CAT_IND_22
                ,MAX(CASE WHEN IND_MES = 23 THEN 1 ELSE 0 END) CAT_IND_23
                ,MAX(CASE WHEN IND_MES = 24 THEN 1 ELSE 0 END) CAT_IND_24
                ,MAX(CASE WHEN IND_MES = 25 THEN 1 ELSE 0 END) CAT_IND_25
                ,MAX(CASE WHEN IND_MES = 26 THEN 1 ELSE 0 END) CAT_IND_26
                ,MAX(CASE WHEN IND_MES = 27 THEN 1 ELSE 0 END) CAT_IND_27

            FROM #VENTA
            INNER JOIN #TX_CONDICION USING(INVOICE_NO)
            LEFT JOIN #MESES ON DATE_TRUNC('MONTH', INVOICE_DATE)::DATE::VARCHAR = MES
            WHERE IND_MC = 1
            GROUP BY 1,2,3
            );

            -- SELECT * FROM #INDICADORES LIMIT 10000;

            DROP TABLE IF EXISTS #SEGMENTOS_CLIENTES;
            CREATE TABLE #SEGMENTOS_CLIENTES AS (
            WITH __INDICADORES_SEGMENTOS AS (
            SELECT
                *
                --POS
                --PO RECOMPRA
                ,IND_13 AS PO_RECOMPRA_13
                ,IND_14 AS PO_RECOMPRA_14
                ,IND_15 AS PO_RECOMPRA_15
                ,IND_16 AS PO_RECOMPRA_16
                ,IND_17 AS PO_RECOMPRA_17
                ,IND_18 AS PO_RECOMPRA_18
                ,IND_19 AS PO_RECOMPRA_19
                ,IND_20 AS PO_RECOMPRA_20
                ,IND_21 AS PO_RECOMPRA_21
                ,IND_22 AS PO_RECOMPRA_22
                ,IND_23 AS PO_RECOMPRA_23
                ,IND_24 AS PO_RECOMPRA_24
                
                --PO FID
                ,CASE WHEN IND_10 = 1 OR IND_11 = 1 OR IND_12 = 1 THEN 1 ELSE 0 END AS PO_FID_13
                ,CASE WHEN IND_11 = 1 OR IND_12 = 1 OR IND_13 = 1 THEN 1 ELSE 0 END AS PO_FID_14
                ,CASE WHEN IND_12 = 1 OR IND_13 = 1 OR IND_14 = 1 THEN 1 ELSE 0 END AS PO_FID_15
                ,CASE WHEN IND_13 = 1 OR IND_14 = 1 OR IND_15 = 1 THEN 1 ELSE 0 END AS PO_FID_16
                ,CASE WHEN IND_14 = 1 OR IND_15 = 1 OR IND_16 = 1 THEN 1 ELSE 0 END AS PO_FID_17
                ,CASE WHEN IND_15 = 1 OR IND_16 = 1 OR IND_17 = 1 THEN 1 ELSE 0 END AS PO_FID_18
                ,CASE WHEN IND_16 = 1 OR IND_17 = 1 OR IND_18 = 1 THEN 1 ELSE 0 END AS PO_FID_19
                ,CASE WHEN IND_17 = 1 OR IND_18 = 1 OR IND_19 = 1 THEN 1 ELSE 0 END AS PO_FID_20
                ,CASE WHEN IND_18 = 1 OR IND_19 = 1 OR IND_20 = 1 THEN 1 ELSE 0 END AS PO_FID_21
                ,CASE WHEN IND_19 = 1 OR IND_20 = 1 OR IND_21 = 1 THEN 1 ELSE 0 END AS PO_FID_22
                ,CASE WHEN IND_20 = 1 OR IND_21 = 1 OR IND_22 = 1 THEN 1 ELSE 0 END AS PO_FID_23
                ,CASE WHEN IND_21 = 1 OR IND_22 = 1 OR IND_23 = 1 THEN 1 ELSE 0 END AS PO_FID_24

                --PO DOR
                ,CASE WHEN (IND_7 = 1 OR IND_8 = 1 OR IND_9 = 1) AND IND_10 = 0 AND IND_11 = 0 AND IND_12 = 0 THEN 1 ELSE 0 END AS PO_DOR_13
                ,CASE WHEN (IND_8 = 1 OR IND_9 = 1 OR IND_10 = 1) AND IND_11 = 0 AND IND_12 = 0 AND IND_13 = 0 THEN 1 ELSE 0 END AS PO_DOR_14
                ,CASE WHEN (IND_9 = 1 OR IND_10 = 1 OR IND_11 = 1) AND IND_12 = 0 AND IND_13 = 0 AND IND_14 = 0 THEN 1 ELSE 0 END AS PO_DOR_15
                ,CASE WHEN (IND_10 = 1 OR IND_11 = 1 OR IND_12 = 1) AND IND_13 = 0 AND IND_14 = 0 AND IND_15 = 0 THEN 1 ELSE 0 END AS PO_DOR_16
                ,CASE WHEN (IND_11 = 1 OR IND_12 = 1 OR IND_13 = 1) AND IND_14 = 0 AND IND_15 = 0 AND IND_16 = 0 THEN 1 ELSE 0 END AS PO_DOR_17
                ,CASE WHEN (IND_12 = 1 OR IND_13 = 1 OR IND_14 = 1) AND IND_15 = 0 AND IND_16 = 0 AND IND_17 = 0 THEN 1 ELSE 0 END AS PO_DOR_18
                ,CASE WHEN (IND_13 = 1 OR IND_14 = 1 OR IND_15 = 1) AND IND_16 = 0 AND IND_17 = 0 AND IND_18 = 0 THEN 1 ELSE 0 END AS PO_DOR_19
                ,CASE WHEN (IND_14 = 1 OR IND_15 = 1 OR IND_16 = 1) AND IND_17 = 0 AND IND_18 = 0 AND IND_19 = 0 THEN 1 ELSE 0 END AS PO_DOR_20
                ,CASE WHEN (IND_15 = 1 OR IND_16 = 1 OR IND_17 = 1) AND IND_18 = 0 AND IND_19 = 0 AND IND_20 = 0 THEN 1 ELSE 0 END AS PO_DOR_21
                ,CASE WHEN (IND_16 = 1 OR IND_17 = 1 OR IND_18 = 1) AND IND_19 = 0 AND IND_20 = 0 AND IND_21 = 0 THEN 1 ELSE 0 END AS PO_DOR_22
                ,CASE WHEN (IND_17 = 1 OR IND_18 = 1 OR IND_19 = 1) AND IND_20 = 0 AND IND_21 = 0 AND IND_22 = 0 THEN 1 ELSE 0 END AS PO_DOR_23
                ,CASE WHEN (IND_18 = 1 OR IND_19 = 1 OR IND_20 = 1) AND IND_21 = 0 AND IND_22 = 0 AND IND_23 = 0 THEN 1 ELSE 0 END AS PO_DOR_24

                --PO PER
                ,CASE WHEN (IND_1 = 1 OR IND_2 = 1 OR IND_3 = 1 OR IND_4 = 1 OR IND_5 = 1 OR IND_6 = 1) AND IND_7 = 0 AND IND_8 = 0 AND IND_9 = 0 AND IND_10 = 0 AND IND_11 = 0 AND IND_12 = 0 THEN 1 ELSE 0 END AS PO_PER_13
                ,CASE WHEN (IND_2 = 1 OR IND_3 = 1 OR IND_4 = 1 OR IND_5 = 1 OR IND_6 = 1 OR IND_7 = 1) AND IND_8 = 0 AND IND_9 = 0 AND IND_10 = 0 AND IND_11 = 0 AND IND_12 = 0 AND IND_13 = 0 THEN 1 ELSE 0 END AS PO_PER_14
                ,CASE WHEN (IND_3 = 1 OR IND_4 = 1 OR IND_5 = 1 OR IND_6 = 1 OR IND_7 = 1 OR IND_8 = 1) AND IND_9 = 0 AND IND_10 = 0 AND IND_11 = 0 AND IND_12 = 0 AND IND_13 = 0 AND IND_14 = 0 THEN 1 ELSE 0 END AS PO_PER_15
                ,CASE WHEN (IND_4 = 1 OR IND_5 = 1 OR IND_6 = 1 OR IND_7 = 1 OR IND_8 = 1 OR IND_9 = 1) AND IND_10 = 0 AND IND_11 = 0 AND IND_12 = 0 AND IND_13 = 0 AND IND_14 = 0 AND IND_15 = 0 THEN 1 ELSE 0 END AS PO_PER_16
                ,CASE WHEN (IND_5 = 1 OR IND_6 = 1 OR IND_7 = 1 OR IND_8 = 1 OR IND_9 = 1 OR IND_10 = 1) AND IND_11 = 0 AND IND_12 = 0 AND IND_13 = 0 AND IND_14 = 0 AND IND_15 = 0 AND IND_16 = 0 THEN 1 ELSE 0 END AS PO_PER_17
                ,CASE WHEN (IND_6 = 1 OR IND_7 = 1 OR IND_8 = 1 OR IND_9 = 1 OR IND_10 = 1 OR IND_11 = 1) AND IND_12 = 0 AND IND_13 = 0 AND IND_14 = 0 AND IND_15 = 0 AND IND_16 = 0 AND IND_17 = 0 THEN 1 ELSE 0 END AS PO_PER_18
                ,CASE WHEN (IND_7 = 1 OR IND_8 = 1 OR IND_9 = 1 OR IND_10 = 1 OR IND_11 = 1 OR IND_12 = 1) AND IND_13 = 0 AND IND_14 = 0 AND IND_15 = 0 AND IND_16 = 0 AND IND_17 = 0 AND IND_18 = 0 THEN 1 ELSE 0 END AS PO_PER_19
                ,CASE WHEN (IND_8 = 1 OR IND_9 = 1 OR IND_10 = 1 OR IND_11 = 1 OR IND_12 = 1 OR IND_13 = 1) AND IND_14 = 0 AND IND_15 = 0 AND IND_16 = 0 AND IND_17 = 0 AND IND_18 = 0 AND IND_19 = 0 THEN 1 ELSE 0 END AS PO_PER_20
                ,CASE WHEN (IND_9 = 1 OR IND_10 = 1 OR IND_11 = 1 OR IND_12 = 1 OR IND_13 = 1 OR IND_14 = 1) AND IND_15 = 0 AND IND_16 = 0 AND IND_17 = 0 AND IND_18 = 0 AND IND_19 = 0 AND IND_20 = 0 THEN 1 ELSE 0 END AS PO_PER_21
                ,CASE WHEN (IND_10 = 1 OR IND_11 = 1 OR IND_12 = 1 OR IND_13 = 1 OR IND_14 = 1 OR IND_15 = 1) AND IND_16 = 0 AND IND_17 = 0 AND IND_18 = 0 AND IND_19 = 0 AND IND_20 = 0 AND IND_21 = 0 THEN 1 ELSE 0 END AS PO_PER_22
                ,CASE WHEN (IND_11 = 1 OR IND_12 = 1 OR IND_13 = 1 OR IND_14 = 1 OR IND_15 = 1 OR IND_16 = 1) AND IND_17 = 0 AND IND_18 = 0 AND IND_19 = 0 AND IND_20 = 0 AND IND_21 = 0 AND IND_22 = 0 THEN 1 ELSE 0 END AS PO_PER_23
                ,CASE WHEN (IND_12 = 1 OR IND_13 = 1 OR IND_14 = 1 OR IND_15 = 1 OR IND_16 = 1 OR IND_17 = 1) AND IND_18 = 0 AND IND_19 = 0 AND IND_20 = 0 AND IND_21 = 0 AND IND_22 = 0 AND IND_23 = 0 THEN 1 ELSE 0 END AS PO_PER_24

                --PO REC
                ,CASE WHEN PO_DOR_13 = 1 OR PO_PER_13 = 1 THEN 1 ELSE 0 END AS PO_REC_13
                ,CASE WHEN PO_DOR_14 = 1 OR PO_PER_14 = 1 THEN 1 ELSE 0 END AS PO_REC_14
                ,CASE WHEN PO_DOR_15 = 1 OR PO_PER_15 = 1 THEN 1 ELSE 0 END AS PO_REC_15
                ,CASE WHEN PO_DOR_16 = 1 OR PO_PER_16 = 1 THEN 1 ELSE 0 END AS PO_REC_16
                ,CASE WHEN PO_DOR_17 = 1 OR PO_PER_17 = 1 THEN 1 ELSE 0 END AS PO_REC_17
                ,CASE WHEN PO_DOR_18 = 1 OR PO_PER_18 = 1 THEN 1 ELSE 0 END AS PO_REC_18
                ,CASE WHEN PO_DOR_19 = 1 OR PO_PER_19 = 1 THEN 1 ELSE 0 END AS PO_REC_19
                ,CASE WHEN PO_DOR_20 = 1 OR PO_PER_20 = 1 THEN 1 ELSE 0 END AS PO_REC_20
                ,CASE WHEN PO_DOR_21 = 1 OR PO_PER_21 = 1 THEN 1 ELSE 0 END AS PO_REC_21
                ,CASE WHEN PO_DOR_22 = 1 OR PO_PER_22 = 1 THEN 1 ELSE 0 END AS PO_REC_22
                ,CASE WHEN PO_DOR_23 = 1 OR PO_PER_23 = 1 THEN 1 ELSE 0 END AS PO_REC_23
                ,CASE WHEN PO_DOR_24 = 1 OR PO_PER_24 = 1 THEN 1 ELSE 0 END AS PO_REC_24
                
                --PO NUEVOS
                ,CASE WHEN PO_FID_13 = 0 AND PO_REC_13 = 0 THEN 1 ELSE 0 END PO_NUEVOS_13
                ,CASE WHEN PO_FID_14 = 0 AND PO_REC_14 = 0 THEN 1 ELSE 0 END PO_NUEVOS_14
                ,CASE WHEN PO_FID_15 = 0 AND PO_REC_15 = 0 THEN 1 ELSE 0 END PO_NUEVOS_15
                ,CASE WHEN PO_FID_16 = 0 AND PO_REC_16 = 0 THEN 1 ELSE 0 END PO_NUEVOS_16
                ,CASE WHEN PO_FID_17 = 0 AND PO_REC_17 = 0 THEN 1 ELSE 0 END PO_NUEVOS_17
                ,CASE WHEN PO_FID_18 = 0 AND PO_REC_18 = 0 THEN 1 ELSE 0 END PO_NUEVOS_18
                ,CASE WHEN PO_FID_19 = 0 AND PO_REC_19 = 0 THEN 1 ELSE 0 END PO_NUEVOS_19
                ,CASE WHEN PO_FID_20 = 0 AND PO_REC_20 = 0 THEN 1 ELSE 0 END PO_NUEVOS_20
                ,CASE WHEN PO_FID_21 = 0 AND PO_REC_21 = 0 THEN 1 ELSE 0 END PO_NUEVOS_21
                ,CASE WHEN PO_FID_22 = 0 AND PO_REC_22 = 0 THEN 1 ELSE 0 END PO_NUEVOS_22
                ,CASE WHEN PO_FID_23 = 0 AND PO_REC_23 = 0 THEN 1 ELSE 0 END PO_NUEVOS_23
                ,CASE WHEN PO_FID_24 = 0 AND PO_REC_24 = 0 THEN 1 ELSE 0 END PO_NUEVOS_24
                
                --PO CON COMPRA  
                --RECOMPRA
                ,CASE WHEN IND_13 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END AS RECOMPRA_13
                ,CASE WHEN IND_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS RECOMPRA_14
                ,CASE WHEN IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS RECOMPRA_15
                ,CASE WHEN IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS RECOMPRA_16
                ,CASE WHEN IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS RECOMPRA_17
                ,CASE WHEN IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS RECOMPRA_18
                ,CASE WHEN IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS RECOMPRA_19
                ,CASE WHEN IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS RECOMPRA_20
                ,CASE WHEN IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS RECOMPRA_21
                ,CASE WHEN IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS RECOMPRA_22
                ,CASE WHEN IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS RECOMPRA_23
                ,CASE WHEN IND_24 = 1 AND IND_25 = 1 THEN 1 ELSE 0 END AS RECOMPRA_24

                --RECOMPRA 2 MESES
                ,CASE WHEN IND_13 = 1 AND (IND_14 = 1 OR IND_15 = 1) THEN 1 ELSE 0 END AS RECOMPRA_2M_13
                ,CASE WHEN IND_14 = 1 AND (IND_15 = 1 OR IND_16 = 1) THEN 1 ELSE 0 END AS RECOMPRA_2M_14
                ,CASE WHEN IND_15 = 1 AND (IND_16 = 1 OR IND_17 = 1) THEN 1 ELSE 0 END AS RECOMPRA_2M_15
                ,CASE WHEN IND_16 = 1 AND (IND_17 = 1 OR IND_18 = 1) THEN 1 ELSE 0 END AS RECOMPRA_2M_16
                ,CASE WHEN IND_17 = 1 AND (IND_18 = 1 OR IND_19 = 1) THEN 1 ELSE 0 END AS RECOMPRA_2M_17
                ,CASE WHEN IND_18 = 1 AND (IND_19 = 1 OR IND_20 = 1) THEN 1 ELSE 0 END AS RECOMPRA_2M_18
                ,CASE WHEN IND_19 = 1 AND (IND_20 = 1 OR IND_21 = 1) THEN 1 ELSE 0 END AS RECOMPRA_2M_19
                ,CASE WHEN IND_20 = 1 AND (IND_21 = 1 OR IND_22 = 1) THEN 1 ELSE 0 END AS RECOMPRA_2M_20
                ,CASE WHEN IND_21 = 1 AND (IND_22 = 1 OR IND_23 = 1) THEN 1 ELSE 0 END AS RECOMPRA_2M_21
                ,CASE WHEN IND_22 = 1 AND (IND_23 = 1 OR IND_24 = 1) THEN 1 ELSE 0 END AS RECOMPRA_2M_22
                ,CASE WHEN IND_23 = 1 AND (IND_24 = 1 OR IND_25 = 1) THEN 1 ELSE 0 END AS RECOMPRA_2M_23
                ,CASE WHEN IND_24 = 1 AND (IND_25 = 1 OR IND_26 = 1) THEN 1 ELSE 0 END AS RECOMPRA_2M_24
                
                --RECOMPRA 3 MESES
                ,CASE WHEN IND_13 = 1 AND (IND_14 = 1 OR IND_15 = 1 OR IND_16 = 1) THEN 1 ELSE 0 END AS RECOMPRA_3M_13
                ,CASE WHEN IND_14 = 1 AND (IND_15 = 1 OR IND_16 = 1 OR IND_17 = 1) THEN 1 ELSE 0 END AS RECOMPRA_3M_14
                ,CASE WHEN IND_15 = 1 AND (IND_16 = 1 OR IND_17 = 1 OR IND_18 = 1) THEN 1 ELSE 0 END AS RECOMPRA_3M_15
                ,CASE WHEN IND_16 = 1 AND (IND_17 = 1 OR IND_18 = 1 OR IND_19 = 1) THEN 1 ELSE 0 END AS RECOMPRA_3M_16
                ,CASE WHEN IND_17 = 1 AND (IND_18 = 1 OR IND_19 = 1 OR IND_20 = 1) THEN 1 ELSE 0 END AS RECOMPRA_3M_17
                ,CASE WHEN IND_18 = 1 AND (IND_19 = 1 OR IND_20 = 1 OR IND_21 = 1) THEN 1 ELSE 0 END AS RECOMPRA_3M_18
                ,CASE WHEN IND_19 = 1 AND (IND_20 = 1 OR IND_21 = 1 OR IND_22 = 1) THEN 1 ELSE 0 END AS RECOMPRA_3M_19
                ,CASE WHEN IND_20 = 1 AND (IND_21 = 1 OR IND_22 = 1 OR IND_23 = 1) THEN 1 ELSE 0 END AS RECOMPRA_3M_20
                ,CASE WHEN IND_21 = 1 AND (IND_22 = 1 OR IND_23 = 1 OR IND_24 = 1) THEN 1 ELSE 0 END AS RECOMPRA_3M_21
                ,CASE WHEN IND_22 = 1 AND (IND_23 = 1 OR IND_24 = 1 OR IND_25 = 1) THEN 1 ELSE 0 END AS RECOMPRA_3M_22
                ,CASE WHEN IND_23 = 1 AND (IND_24 = 1 OR IND_25 = 1 OR IND_26 = 1) THEN 1 ELSE 0 END AS RECOMPRA_3M_23
                ,CASE WHEN IND_24 = 1 AND (IND_25 = 1 OR IND_26 = 1 OR IND_27 = 1) THEN 1 ELSE 0 END AS RECOMPRA_3M_24
                
                --FID
                ,CASE WHEN PO_FID_13 = 1 AND IND_13 = 1 THEN 1 ELSE 0 END AS FID_13
                ,CASE WHEN PO_FID_14 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END AS FID_14
                ,CASE WHEN PO_FID_15 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS FID_15
                ,CASE WHEN PO_FID_16 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS FID_16
                ,CASE WHEN PO_FID_17 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS FID_17
                ,CASE WHEN PO_FID_18 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS FID_18
                ,CASE WHEN PO_FID_19 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS FID_19
                ,CASE WHEN PO_FID_20 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS FID_20
                ,CASE WHEN PO_FID_21 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS FID_21
                ,CASE WHEN PO_FID_22 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS FID_22
                ,CASE WHEN PO_FID_23 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS FID_23
                ,CASE WHEN PO_FID_24 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS FID_24

                --DOR
                ,CASE WHEN PO_DOR_13 = 1 AND IND_13 = 1 THEN 1 ELSE 0 END DOR_13
                ,CASE WHEN PO_DOR_14 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END DOR_14
                ,CASE WHEN PO_DOR_15 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END DOR_15
                ,CASE WHEN PO_DOR_16 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END DOR_16
                ,CASE WHEN PO_DOR_17 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END DOR_17
                ,CASE WHEN PO_DOR_18 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END DOR_18
                ,CASE WHEN PO_DOR_19 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END DOR_19
                ,CASE WHEN PO_DOR_20 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END DOR_20
                ,CASE WHEN PO_DOR_21 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END DOR_21
                ,CASE WHEN PO_DOR_22 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END DOR_22
                ,CASE WHEN PO_DOR_23 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END DOR_23
                ,CASE WHEN PO_DOR_24 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END DOR_24
                
                --PER
                ,CASE WHEN PO_PER_13 = 1 AND IND_13 = 1 THEN 1 ELSE 0 END PER_13
                ,CASE WHEN PO_PER_14 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END PER_14
                ,CASE WHEN PO_PER_15 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END PER_15
                ,CASE WHEN PO_PER_16 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END PER_16
                ,CASE WHEN PO_PER_17 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END PER_17
                ,CASE WHEN PO_PER_18 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END PER_18
                ,CASE WHEN PO_PER_19 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END PER_19
                ,CASE WHEN PO_PER_20 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END PER_20
                ,CASE WHEN PO_PER_21 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END PER_21
                ,CASE WHEN PO_PER_22 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END PER_22
                ,CASE WHEN PO_PER_23 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END PER_23
                ,CASE WHEN PO_PER_24 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END PER_24
                
                --REC
                ,CASE WHEN PO_REC_13 = 1 AND IND_13 = 1 THEN 1 ELSE 0 END REC_13
                ,CASE WHEN PO_REC_14 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END REC_14
                ,CASE WHEN PO_REC_15 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END REC_15
                ,CASE WHEN PO_REC_16 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END REC_16
                ,CASE WHEN PO_REC_17 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END REC_17
                ,CASE WHEN PO_REC_18 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END REC_18
                ,CASE WHEN PO_REC_19 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END REC_19
                ,CASE WHEN PO_REC_20 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END REC_20
                ,CASE WHEN PO_REC_21 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END REC_21
                ,CASE WHEN PO_REC_22 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END REC_22
                ,CASE WHEN PO_REC_23 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END REC_23
                ,CASE WHEN PO_REC_24 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END REC_24
                
                --NUEVOS
                ,CASE WHEN PO_NUEVOS_13 = 1 AND IND_13 = 1 THEN 1 ELSE 0 END NUEVOS_13
                ,CASE WHEN PO_NUEVOS_14 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END NUEVOS_14
                ,CASE WHEN PO_NUEVOS_15 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END NUEVOS_15
                ,CASE WHEN PO_NUEVOS_16 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END NUEVOS_16
                ,CASE WHEN PO_NUEVOS_17 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END NUEVOS_17
                ,CASE WHEN PO_NUEVOS_18 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END NUEVOS_18
                ,CASE WHEN PO_NUEVOS_19 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END NUEVOS_19
                ,CASE WHEN PO_NUEVOS_20 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END NUEVOS_20
                ,CASE WHEN PO_NUEVOS_21 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END NUEVOS_21
                ,CASE WHEN PO_NUEVOS_22 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END NUEVOS_22
                ,CASE WHEN PO_NUEVOS_23 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END NUEVOS_23
                ,CASE WHEN PO_NUEVOS_24 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END NUEVOS_24
                
            FROM #INDICADORES
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
                
                --PO RECOMPRA
                ,SUM(PO_RECOMPRA_13) PO_RECOMPRA_13
                ,SUM(PO_RECOMPRA_14) PO_RECOMPRA_14
                ,SUM(PO_RECOMPRA_15) PO_RECOMPRA_15
                ,SUM(PO_RECOMPRA_16) PO_RECOMPRA_16
                ,SUM(PO_RECOMPRA_17) PO_RECOMPRA_17
                ,SUM(PO_RECOMPRA_18) PO_RECOMPRA_18
                ,SUM(PO_RECOMPRA_19) PO_RECOMPRA_19
                ,SUM(PO_RECOMPRA_20) PO_RECOMPRA_20
                ,SUM(PO_RECOMPRA_21) PO_RECOMPRA_21
                ,SUM(PO_RECOMPRA_22) PO_RECOMPRA_22
                ,SUM(PO_RECOMPRA_23) PO_RECOMPRA_23
                ,SUM(PO_RECOMPRA_24) PO_RECOMPRA_24

                --PO_FID
                ,SUM(PO_FID_13) PO_FID_13
                ,SUM(PO_FID_14) PO_FID_14
                ,SUM(PO_FID_15) PO_FID_15
                ,SUM(PO_FID_16) PO_FID_16
                ,SUM(PO_FID_17) PO_FID_17
                ,SUM(PO_FID_18) PO_FID_18
                ,SUM(PO_FID_19) PO_FID_19
                ,SUM(PO_FID_20) PO_FID_20
                ,SUM(PO_FID_21) PO_FID_21
                ,SUM(PO_FID_22) PO_FID_22
                ,SUM(PO_FID_23) PO_FID_23
                ,SUM(PO_FID_24) PO_FID_24

                --PO_DOR
                ,SUM(PO_DOR_13) PO_DOR_13
                ,SUM(PO_DOR_14) PO_DOR_14
                ,SUM(PO_DOR_15) PO_DOR_15
                ,SUM(PO_DOR_16) PO_DOR_16
                ,SUM(PO_DOR_17) PO_DOR_17
                ,SUM(PO_DOR_18) PO_DOR_18
                ,SUM(PO_DOR_19) PO_DOR_19
                ,SUM(PO_DOR_20) PO_DOR_20
                ,SUM(PO_DOR_21) PO_DOR_21
                ,SUM(PO_DOR_22) PO_DOR_22
                ,SUM(PO_DOR_23) PO_DOR_23
                ,SUM(PO_DOR_24) PO_DOR_24

                --PO_PER
                ,SUM(PO_PER_13) PO_PER_13
                ,SUM(PO_PER_14) PO_PER_14
                ,SUM(PO_PER_15) PO_PER_15
                ,SUM(PO_PER_16) PO_PER_16
                ,SUM(PO_PER_17) PO_PER_17
                ,SUM(PO_PER_18) PO_PER_18
                ,SUM(PO_PER_19) PO_PER_19
                ,SUM(PO_PER_20) PO_PER_20
                ,SUM(PO_PER_21) PO_PER_21
                ,SUM(PO_PER_22) PO_PER_22
                ,SUM(PO_PER_23) PO_PER_23
                ,SUM(PO_PER_24) PO_PER_24

                --PO_REC
                ,SUM(PO_REC_13) PO_REC_13
                ,SUM(PO_REC_14) PO_REC_14
                ,SUM(PO_REC_15) PO_REC_15
                ,SUM(PO_REC_16) PO_REC_16
                ,SUM(PO_REC_17) PO_REC_17
                ,SUM(PO_REC_18) PO_REC_18
                ,SUM(PO_REC_19) PO_REC_19
                ,SUM(PO_REC_20) PO_REC_20
                ,SUM(PO_REC_21) PO_REC_21
                ,SUM(PO_REC_22) PO_REC_22
                ,SUM(PO_REC_23) PO_REC_23
                ,SUM(PO_REC_24) PO_REC_24

                --PO_NUEVOS
                ,SUM(PO_NUEVOS_13) PO_NUEVOS_13
                ,SUM(PO_NUEVOS_14) PO_NUEVOS_14
                ,SUM(PO_NUEVOS_15) PO_NUEVOS_15
                ,SUM(PO_NUEVOS_16) PO_NUEVOS_16
                ,SUM(PO_NUEVOS_17) PO_NUEVOS_17
                ,SUM(PO_NUEVOS_18) PO_NUEVOS_18
                ,SUM(PO_NUEVOS_19) PO_NUEVOS_19
                ,SUM(PO_NUEVOS_20) PO_NUEVOS_20
                ,SUM(PO_NUEVOS_21) PO_NUEVOS_21
                ,SUM(PO_NUEVOS_22) PO_NUEVOS_22
                ,SUM(PO_NUEVOS_23) PO_NUEVOS_23
                ,SUM(PO_NUEVOS_24) PO_NUEVOS_24
                
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

                --COMPRA RECOMPRA 2 MESES
                ,SUM(RECOMPRA_2M_13) RECOMPRA_2M_13
                ,SUM(RECOMPRA_2M_14) RECOMPRA_2M_14
                ,SUM(RECOMPRA_2M_15) RECOMPRA_2M_15
                ,SUM(RECOMPRA_2M_16) RECOMPRA_2M_16
                ,SUM(RECOMPRA_2M_17) RECOMPRA_2M_17
                ,SUM(RECOMPRA_2M_18) RECOMPRA_2M_18
                ,SUM(RECOMPRA_2M_19) RECOMPRA_2M_19
                ,SUM(RECOMPRA_2M_20) RECOMPRA_2M_20
                ,SUM(RECOMPRA_2M_21) RECOMPRA_2M_21
                ,SUM(RECOMPRA_2M_22) RECOMPRA_2M_22
                ,SUM(RECOMPRA_2M_23) RECOMPRA_2M_23
                ,SUM(RECOMPRA_2M_24) RECOMPRA_2M_24

                --COMPRA RECOMPRA 3 MESES
                ,SUM(RECOMPRA_3M_13) RECOMPRA_3M_13
                ,SUM(RECOMPRA_3M_14) RECOMPRA_3M_14
                ,SUM(RECOMPRA_3M_15) RECOMPRA_3M_15
                ,SUM(RECOMPRA_3M_16) RECOMPRA_3M_16
                ,SUM(RECOMPRA_3M_17) RECOMPRA_3M_17
                ,SUM(RECOMPRA_3M_18) RECOMPRA_3M_18
                ,SUM(RECOMPRA_3M_19) RECOMPRA_3M_19
                ,SUM(RECOMPRA_3M_20) RECOMPRA_3M_20
                ,SUM(RECOMPRA_3M_21) RECOMPRA_3M_21
                ,SUM(RECOMPRA_3M_22) RECOMPRA_3M_22
                ,SUM(RECOMPRA_3M_23) RECOMPRA_3M_23
                ,SUM(RECOMPRA_3M_24) RECOMPRA_3M_24

                --COMPRA FID
                ,SUM(FID_13) FID_13
                ,SUM(FID_14) FID_14
                ,SUM(FID_15) FID_15
                ,SUM(FID_16) FID_16
                ,SUM(FID_17) FID_17
                ,SUM(FID_18) FID_18
                ,SUM(FID_19) FID_19
                ,SUM(FID_20) FID_20
                ,SUM(FID_21) FID_21
                ,SUM(FID_22) FID_22
                ,SUM(FID_23) FID_23
                ,SUM(FID_24) FID_24
                
                --COMPRA DOR
                ,SUM(DOR_13) DOR_13
                ,SUM(DOR_14) DOR_14
                ,SUM(DOR_15) DOR_15
                ,SUM(DOR_16) DOR_16
                ,SUM(DOR_17) DOR_17
                ,SUM(DOR_18) DOR_18
                ,SUM(DOR_19) DOR_19
                ,SUM(DOR_20) DOR_20
                ,SUM(DOR_21) DOR_21
                ,SUM(DOR_22) DOR_22
                ,SUM(DOR_23) DOR_23
                ,SUM(DOR_24) DOR_24

                --COMPRA PER
                ,SUM(PER_13) PER_13
                ,SUM(PER_14) PER_14
                ,SUM(PER_15) PER_15
                ,SUM(PER_16) PER_16
                ,SUM(PER_17) PER_17
                ,SUM(PER_18) PER_18
                ,SUM(PER_19) PER_19
                ,SUM(PER_20) PER_20
                ,SUM(PER_21) PER_21
                ,SUM(PER_22) PER_22
                ,SUM(PER_23) PER_23
                ,SUM(PER_24) PER_24

                --COMPRA REC
                ,SUM(REC_13) REC_13
                ,SUM(REC_14) REC_14
                ,SUM(REC_15) REC_15
                ,SUM(REC_16) REC_16
                ,SUM(REC_17) REC_17
                ,SUM(REC_18) REC_18
                ,SUM(REC_19) REC_19
                ,SUM(REC_20) REC_20
                ,SUM(REC_21) REC_21
                ,SUM(REC_22) REC_22
                ,SUM(REC_23) REC_23
                ,SUM(REC_24) REC_24
                
                --COMPRA NUEVOS
                ,SUM(NUEVOS_13) NUEVOS_13
                ,SUM(NUEVOS_14) NUEVOS_14
                ,SUM(NUEVOS_15) NUEVOS_15
                ,SUM(NUEVOS_16) NUEVOS_16
                ,SUM(NUEVOS_17) NUEVOS_17
                ,SUM(NUEVOS_18) NUEVOS_18
                ,SUM(NUEVOS_19) NUEVOS_19
                ,SUM(NUEVOS_20) NUEVOS_20
                ,SUM(NUEVOS_21) NUEVOS_21
                ,SUM(NUEVOS_22) NUEVOS_22
                ,SUM(NUEVOS_23) NUEVOS_23
                ,SUM(NUEVOS_24) NUEVOS_24

                FROM __INDICADORES_SEGMENTOS
                GROUP BY 1,2
            )
            ,__RESUMEN_SEGMENTOS AS (
                SELECT PROVEEDOR, MARCA, 13 IND_MES, CLIENTES_13 AS CLIENTES, PO_RECOMPRA_13 AS PO_RECOMPRA, RECOMPRA_13 AS RECOMPRA, RECOMPRA_2M_13 AS RECOMPRA_2M, RECOMPRA_3M_13 AS RECOMPRA_3M, PO_FID_13 AS PO_FID, FID_13 AS FID, PO_DOR_13 AS PO_DOR, DOR_13 AS DOR, PO_PER_13 AS PO_PER, PER_13 AS PER, PO_REC_13 AS PO_REC, REC_13 AS REC, PO_NUEVOS_13 AS PO_NUEVOS, NUEVOS_13 AS NUEVOS FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 14 IND_MES, CLIENTES_14 AS CLIENTES, PO_RECOMPRA_14 AS PO_RECOMPRA, RECOMPRA_14 AS RECOMPRA, RECOMPRA_2M_14 AS RECOMPRA_2M, RECOMPRA_3M_14 AS RECOMPRA_3M, PO_FID_14 AS PO_FID, FID_14 AS FID, PO_DOR_14 AS PO_DOR, DOR_14 AS DOR, PO_PER_14 AS PO_PER, PER_14 AS PER, PO_REC_14 AS PO_REC, REC_14 AS REC, PO_NUEVOS_14 AS PO_NUEVOS, NUEVOS_14 AS NUEVOS FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 15 IND_MES, CLIENTES_15 AS CLIENTES, PO_RECOMPRA_15 AS PO_RECOMPRA, RECOMPRA_15 AS RECOMPRA, RECOMPRA_2M_15 AS RECOMPRA_2M, RECOMPRA_3M_15 AS RECOMPRA_3M, PO_FID_15 AS PO_FID, FID_15 AS FID, PO_DOR_15 AS PO_DOR, DOR_15 AS DOR, PO_PER_15 AS PO_PER, PER_15 AS PER, PO_REC_15 AS PO_REC, REC_15 AS REC, PO_NUEVOS_15 AS PO_NUEVOS, NUEVOS_15 AS NUEVOS FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 16 IND_MES, CLIENTES_16 AS CLIENTES, PO_RECOMPRA_16 AS PO_RECOMPRA, RECOMPRA_16 AS RECOMPRA, RECOMPRA_2M_16 AS RECOMPRA_2M, RECOMPRA_3M_16 AS RECOMPRA_3M, PO_FID_16 AS PO_FID, FID_16 AS FID, PO_DOR_16 AS PO_DOR, DOR_16 AS DOR, PO_PER_16 AS PO_PER, PER_16 AS PER, PO_REC_16 AS PO_REC, REC_16 AS REC, PO_NUEVOS_16 AS PO_NUEVOS, NUEVOS_16 AS NUEVOS FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 17 IND_MES, CLIENTES_17 AS CLIENTES, PO_RECOMPRA_17 AS PO_RECOMPRA, RECOMPRA_17 AS RECOMPRA, RECOMPRA_2M_17 AS RECOMPRA_2M, RECOMPRA_3M_17 AS RECOMPRA_3M, PO_FID_17 AS PO_FID, FID_17 AS FID, PO_DOR_17 AS PO_DOR, DOR_17 AS DOR, PO_PER_17 AS PO_PER, PER_17 AS PER, PO_REC_17 AS PO_REC, REC_17 AS REC, PO_NUEVOS_17 AS PO_NUEVOS, NUEVOS_17 AS NUEVOS FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 18 IND_MES, CLIENTES_18 AS CLIENTES, PO_RECOMPRA_18 AS PO_RECOMPRA, RECOMPRA_18 AS RECOMPRA, RECOMPRA_2M_18 AS RECOMPRA_2M, RECOMPRA_3M_18 AS RECOMPRA_3M, PO_FID_18 AS PO_FID, FID_18 AS FID, PO_DOR_18 AS PO_DOR, DOR_18 AS DOR, PO_PER_18 AS PO_PER, PER_18 AS PER, PO_REC_18 AS PO_REC, REC_18 AS REC, PO_NUEVOS_18 AS PO_NUEVOS, NUEVOS_18 AS NUEVOS FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 19 IND_MES, CLIENTES_19 AS CLIENTES, PO_RECOMPRA_19 AS PO_RECOMPRA, RECOMPRA_19 AS RECOMPRA, RECOMPRA_2M_19 AS RECOMPRA_2M, RECOMPRA_3M_19 AS RECOMPRA_3M, PO_FID_19 AS PO_FID, FID_19 AS FID, PO_DOR_19 AS PO_DOR, DOR_19 AS DOR, PO_PER_19 AS PO_PER, PER_19 AS PER, PO_REC_19 AS PO_REC, REC_19 AS REC, PO_NUEVOS_19 AS PO_NUEVOS, NUEVOS_19 AS NUEVOS FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 20 IND_MES, CLIENTES_20 AS CLIENTES, PO_RECOMPRA_20 AS PO_RECOMPRA, RECOMPRA_20 AS RECOMPRA, RECOMPRA_2M_20 AS RECOMPRA_2M, RECOMPRA_3M_20 AS RECOMPRA_3M, PO_FID_20 AS PO_FID, FID_20 AS FID, PO_DOR_20 AS PO_DOR, DOR_20 AS DOR, PO_PER_20 AS PO_PER, PER_20 AS PER, PO_REC_20 AS PO_REC, REC_20 AS REC, PO_NUEVOS_20 AS PO_NUEVOS, NUEVOS_20 AS NUEVOS FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 21 IND_MES, CLIENTES_21 AS CLIENTES, PO_RECOMPRA_21 AS PO_RECOMPRA, RECOMPRA_21 AS RECOMPRA, RECOMPRA_2M_21 AS RECOMPRA_2M, RECOMPRA_3M_21 AS RECOMPRA_3M, PO_FID_21 AS PO_FID, FID_21 AS FID, PO_DOR_21 AS PO_DOR, DOR_21 AS DOR, PO_PER_21 AS PO_PER, PER_21 AS PER, PO_REC_21 AS PO_REC, REC_21 AS REC, PO_NUEVOS_21 AS PO_NUEVOS, NUEVOS_21 AS NUEVOS FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 22 IND_MES, CLIENTES_22 AS CLIENTES, PO_RECOMPRA_22 AS PO_RECOMPRA, RECOMPRA_22 AS RECOMPRA, RECOMPRA_2M_22 AS RECOMPRA_2M, RECOMPRA_3M_22 AS RECOMPRA_3M, PO_FID_22 AS PO_FID, FID_22 AS FID, PO_DOR_22 AS PO_DOR, DOR_22 AS DOR, PO_PER_22 AS PO_PER, PER_22 AS PER, PO_REC_22 AS PO_REC, REC_22 AS REC, PO_NUEVOS_22 AS PO_NUEVOS, NUEVOS_22 AS NUEVOS FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 23 IND_MES, CLIENTES_23 AS CLIENTES, PO_RECOMPRA_23 AS PO_RECOMPRA, RECOMPRA_23 AS RECOMPRA, RECOMPRA_2M_23 AS RECOMPRA_2M, RECOMPRA_3M_23 AS RECOMPRA_3M, PO_FID_23 AS PO_FID, FID_23 AS FID, PO_DOR_23 AS PO_DOR, DOR_23 AS DOR, PO_PER_23 AS PO_PER, PER_23 AS PER, PO_REC_23 AS PO_REC, REC_23 AS REC, PO_NUEVOS_23 AS PO_NUEVOS, NUEVOS_23 AS NUEVOS FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 24 IND_MES, CLIENTES_24 AS CLIENTES, PO_RECOMPRA_24 AS PO_RECOMPRA, RECOMPRA_24 AS RECOMPRA, RECOMPRA_2M_24 AS RECOMPRA_2M, RECOMPRA_3M_24 AS RECOMPRA_3M, PO_FID_24 AS PO_FID, FID_24 AS FID, PO_DOR_24 AS PO_DOR, DOR_24 AS DOR, PO_PER_24 AS PO_PER, PER_24 AS PER, PO_REC_24 AS PO_REC, REC_24 AS REC, PO_NUEVOS_24 AS PO_NUEVOS, NUEVOS_24 AS NUEVOS FROM __CLIENTES
            )
            SELECT
                PROVEEDOR
                ,MARCA
                ,MES
                
                ,IND_CAMPANA
                
                ,CLIENTES
                ,CASE WHEN CLIENTES > 0 THEN RECOMPRA::NUMERIC / CLIENTES ELSE 0 END AS "%RECOMPRA"
                ,CASE WHEN CLIENTES > 0 THEN RECOMPRA_2M::NUMERIC / CLIENTES ELSE 0 END AS "%RECOMPRA_2M"
                ,CASE WHEN CLIENTES > 0 THEN RECOMPRA_3M::NUMERIC / CLIENTES ELSE 0 END AS "%RECOMPRA_3M"
                ,CASE WHEN CLIENTES > 0 THEN NUEVOS::NUMERIC / CLIENTES ELSE 0 END AS "%NUEVOS"
                ,CASE WHEN CLIENTES > 0 THEN FID::NUMERIC / CLIENTES ELSE 0 END AS "%FID"
                ,CASE WHEN CLIENTES > 0 THEN REC::NUMERIC / CLIENTES ELSE 0 END AS "%REC"
                ,CASE WHEN CLIENTES > 0 THEN DOR::NUMERIC / CLIENTES ELSE 0 END AS "%DOR"
                ,CASE WHEN CLIENTES > 0 THEN PER::NUMERIC / CLIENTES ELSE 0 END AS "%PER"
                
                ,CASE WHEN PO_RECOMPRA > 0 THEN RECOMPRA::NUMERIC / PO_RECOMPRA ELSE 0 END AS "%TASA_RECOMPRA"
                ,CASE WHEN PO_RECOMPRA > 0 THEN RECOMPRA_2M::NUMERIC / PO_RECOMPRA ELSE 0 END AS "%TASA_RECOMPRA_2M"
                ,CASE WHEN PO_RECOMPRA > 0 THEN RECOMPRA_3M::NUMERIC / PO_RECOMPRA ELSE 0 END AS "%TASA_RECOMPRA_3M"
                ,CASE WHEN PO_NUEVOS > 0 THEN NUEVOS::NUMERIC / PO_NUEVOS ELSE 0 END AS "%TASA_NUEVOS"
                ,CASE WHEN PO_FID > 0 THEN FID::NUMERIC / PO_FID ELSE 0 END AS "%TASA_FID"
                ,CASE WHEN PO_REC > 0 THEN REC::NUMERIC / PO_REC ELSE 0 END AS "%TASA_REC"
                ,CASE WHEN PO_DOR > 0 THEN DOR::NUMERIC / PO_DOR ELSE 0 END AS "%TASA_DOR"
                ,CASE WHEN PO_PER > 0 THEN PER::NUMERIC / PO_PER ELSE 0 END AS "%TASA_PER"

            FROM __RESUMEN_SEGMENTOS A
            LEFT JOIN #MESES B USING(IND_MES)
            WHERE MES IS NOT NULL
            );
        '''

        return [query_meses, query_segmentos]

    def get_query_analisis_bc_recompra(self):
        query_recompra = '''
            --CALCULAR DIAS DE RECOMPRA POR CLIENTE
            DROP TABLE IF EXISTS #RECOMPRA;
            CREATE TABLE #RECOMPRA AS (
            WITH
            --__CLIENTES AS (SELECT DISTINCT CUSTOMER_CODE_TY FROM #VENTA ORDER BY RANDOM() LIMIT 10000),
            __TX_DIA AS (
                SELECT DISTINCT
                CUSTOMER_CODE_TY
                ,'TOTAL' PROVEEDOR
                ,'TOTAL' MARCA
                ,INVOICE_DATE
                ,INVOICE_DATE_ADJ
                ,IND_ACTUAL
                ,IND_CAMPANA
                ,IND_MARCA
                FROM #VENTA
                INNER JOIN #TX_CONDICION USING(INVOICE_NO)
                --INNER JOIN __CLIENTES USING(CUSTOMER_CODE_TY)
            )
            ,__RECOMPRA_DIAS AS (
                SELECT
                *
                ,CASE WHEN IND_MARCA = 1 THEN LEAD(INVOICE_DATE) OVER(PARTITION BY CUSTOMER_CODE_TY, IND_MARCA ORDER BY CUSTOMER_CODE_TY, INVOICE_DATE, IND_MARCA) END NEXT_INVOICE_DATE
                ,LEAD(INVOICE_DATE) OVER(PARTITION BY CUSTOMER_CODE_TY ORDER BY INVOICE_DATE) CAT_NEXT_INVOICE_DATE
                ,DATEDIFF(DAYS, INVOICE_DATE, NEXT_INVOICE_DATE)::REAL DIAS_PARA_RECOMPRA
                ,NULLIF(DATEDIFF(DAYS, INVOICE_DATE, CAT_NEXT_INVOICE_DATE)::REAL, 0) CAT_DIAS_PARA_RECOMPRA
                FROM __TX_DIA
            --     WHERE CAT_DIAS_PARA_RECOMPRA > 0
            )
            --   SELECT * FROM __RECOMPRA_DIAS;
            ,__RECOMPRA_POR_CLIENTE AS (
                SELECT
                CUSTOMER_CODE_TY
                ,PROVEEDOR
                ,MARCA
                
                ,MAX(CASE WHEN IND_MARCA = 1 THEN INVOICE_DATE END) LAST_INVOICE_DATE
                ,MAX(INVOICE_DATE) CAT_LAST_INVOICE_DATE
                
                ,AVG(CASE WHEN IND_MARCA = 1 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA
                ,AVG(CAT_DIAS_PARA_RECOMPRA) CAT_PROMEDIO_DIAS_RECOMPRA
                
                ,AVG(CASE WHEN IND_MARCA = 1 AND DIAS_PARA_RECOMPRA <= 30 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA_30
                ,AVG(CASE WHEN IND_MARCA = 1 AND DIAS_PARA_RECOMPRA <= 90 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA_90
                ,AVG(CASE WHEN IND_MARCA = 1 AND DIAS_PARA_RECOMPRA <= 180 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA_180
                ,AVG(CASE WHEN CAT_DIAS_PARA_RECOMPRA <= 30 THEN CAT_DIAS_PARA_RECOMPRA END) CAT_PROMEDIO_DIAS_RECOMPRA_30
                ,AVG(CASE WHEN CAT_DIAS_PARA_RECOMPRA <= 90 THEN CAT_DIAS_PARA_RECOMPRA END) CAT_PROMEDIO_DIAS_RECOMPRA_90
                ,AVG(CASE WHEN CAT_DIAS_PARA_RECOMPRA <= 180 THEN CAT_DIAS_PARA_RECOMPRA END) CAT_PROMEDIO_DIAS_RECOMPRA_180
                
                FROM __RECOMPRA_DIAS
                GROUP BY 1,2,3
            )
            ,__RECOMPRA_POR_MES AS (
                SELECT
                PROVEEDOR
                ,MARCA
                ,DATE_TRUNC('MONTH', INVOICE_DATE_ADJ)::DATE::VARCHAR MES

                ,MAX(CASE WHEN IND_CAMPANA = 1 THEN 1 ELSE 0 END) AS IND_CAMPANA

                --CLIENTES RECOMPRA
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 1 AND IND_MARCA = 1 AND DIAS_PARA_RECOMPRA > 0 THEN CUSTOMER_CODE_TY END) CLIENTES_RECOMPRA
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 1 AND CAT_DIAS_PARA_RECOMPRA > 0 THEN CUSTOMER_CODE_TY END) CAT_CLIENTES_RECOMPRA
                --CLIENTES RECOMPRA AA
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 0 AND IND_MARCA = 1 AND DIAS_PARA_RECOMPRA > 0 THEN CUSTOMER_CODE_TY END) CLIENTES_RECOMPRA_AA
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 0 AND CAT_DIAS_PARA_RECOMPRA > 0 THEN CUSTOMER_CODE_TY END) CAT_CLIENTES_RECOMPRA_AA
                
                --DIAS PARA RECOMPRA
                ,AVG(CASE WHEN IND_ACTUAL = 1 AND IND_MARCA = 1 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA
                ,AVG(CASE WHEN IND_ACTUAL = 1 THEN CAT_DIAS_PARA_RECOMPRA END) CAT_PROMEDIO_DIAS_RECOMPRA
                --DIAS PARA RECOMPRA AA
                ,AVG(CASE WHEN IND_ACTUAL = 0 AND IND_MARCA = 1 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA_AA
                ,AVG(CASE WHEN IND_ACTUAL = 1 THEN CAT_DIAS_PARA_RECOMPRA END) CAT_PROMEDIO_DIAS_RECOMPRA_AA

                --CLIENTES RECOMPRA 30, 90 Y 180
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 1 AND IND_MARCA = 1 AND DIAS_PARA_RECOMPRA BETWEEN 0 AND 30 THEN DIAS_PARA_RECOMPRA END) CLIENTES_RECOMPRA_30
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 1 AND IND_MARCA = 1 AND DIAS_PARA_RECOMPRA BETWEEN 0 AND 90 THEN DIAS_PARA_RECOMPRA END) CLIENTES_RECOMPRA_90
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 1 AND IND_MARCA = 1 AND DIAS_PARA_RECOMPRA BETWEEN 0 AND 180 THEN DIAS_PARA_RECOMPRA END) CLIENTES_RECOMPRA_180
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 1 AND CAT_DIAS_PARA_RECOMPRA BETWEEN 0 AND 30 THEN CAT_DIAS_PARA_RECOMPRA END) CAT_CLIENTES_RECOMPRA_30
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 1 AND CAT_DIAS_PARA_RECOMPRA BETWEEN 0 AND 90 THEN CAT_DIAS_PARA_RECOMPRA END) CAT_CLIENTES_RECOMPRA_90
                ,COUNT(DISTINCT CASE WHEN IND_ACTUAL = 1 AND CAT_DIAS_PARA_RECOMPRA BETWEEN 0 AND 180 THEN CAT_DIAS_PARA_RECOMPRA END) CAT_CLIENTES_RECOMPRA_180
                --DIAS PARA RECOMPRA 30, 90 Y 180
                ,AVG(CASE WHEN IND_ACTUAL = 1 AND IND_MARCA = 1 AND DIAS_PARA_RECOMPRA <= 30 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA_30
                ,AVG(CASE WHEN IND_ACTUAL = 1 AND IND_MARCA = 1 AND DIAS_PARA_RECOMPRA <= 90 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA_90
                ,AVG(CASE WHEN IND_ACTUAL = 1 AND IND_MARCA = 1 AND DIAS_PARA_RECOMPRA <= 180 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA_180
                ,AVG(CASE WHEN IND_ACTUAL = 1 AND CAT_DIAS_PARA_RECOMPRA <= 30 THEN CAT_DIAS_PARA_RECOMPRA END) CAT_PROMEDIO_DIAS_RECOMPRA_30
                ,AVG(CASE WHEN IND_ACTUAL = 1 AND CAT_DIAS_PARA_RECOMPRA <= 90 THEN CAT_DIAS_PARA_RECOMPRA END) CAT_PROMEDIO_DIAS_RECOMPRA_90
                ,AVG(CASE WHEN IND_ACTUAL = 1 AND CAT_DIAS_PARA_RECOMPRA <= 180 THEN CAT_DIAS_PARA_RECOMPRA END) CAT_PROMEDIO_DIAS_RECOMPRA_180
                FROM __RECOMPRA_DIAS
                WHERE CAT_DIAS_PARA_RECOMPRA > 0
                GROUP BY 1,2,3
            )
            SELECT * FROM __RECOMPRA_POR_MES
            );
        '''

        return [query_recompra]
    
    def get_query_bc(self):
        query_bc = '''
            DROP TABLE IF EXISTS #BC;
            CREATE TABLE #BC AS (
            SELECT
                A.TABLA
                ,A.PROVEEDOR
                ,A.MARCA
                ,A.MES
            
                ,VENTA_ACTUAL
                ,VENTA_AA
            
                ,CLIENTES_ACTUAL
                ,CAT_CLIENTES_ACTUAL

                ,TX_MEDIO
                ,CAT_TX_MEDIO

                ,CRECIMIENTO_VENTA
                ,CAT_CRECIMIENTO_VENTA
                ,CRECIMIENTO_VENTA_ANUAL
                ,CAT_CRECIMIENTO_VENTA_ANUAL

                ,"%CLIENTES_CONDICION_1"
                ,"%CLIENTES_CONDICION_2"
                ,"%CLIENTES_CONDICION_3"
                ,"%CLIENTES_CONDICION_4"
                ,"%CLIENTES_CONDICION_5"
                ,"%CLIENTES_CONDICION_6"

                ,"%RECOMPRA"
                ,"%RECOMPRA_2M"
                ,"%RECOMPRA_3M"
            
                ,PROMEDIO_DIAS_RECOMPRA
                ,CAT_PROMEDIO_DIAS_RECOMPRA

                ,"%NUEVOS"
                ,"%FID"
                ,"%REC"
                ,"%DOR"
                ,"%PER"
            
                ,"%TASA_NUEVOS"
                ,"%TASA_FID"
                ,"%TASA_REC"
                ,"%TASA_DOR"
                ,"%TASA_PER"
            FROM #AGG A
            LEFT JOIN #SEGMENTOS_CLIENTES B --USING(PROVEEDOR, MARCA, MES)
                ON A.PROVEEDOR = B.PROVEEDOR AND A.MARCA = B.MARCA AND ((A.MES = B.MES AND TABLA = 'MES') OR (B.IND_CAMPANA = 1 AND TABLA = 'CAMPANA'))
            LEFT JOIN #RECOMPRA C --USING(PROVEEDOR, MARCA, MES)
                ON A.PROVEEDOR = C.PROVEEDOR AND A.MARCA = C.MARCA AND ((A.MES = C.MES AND TABLA = 'MES') OR (C.IND_CAMPANA = 1 AND TABLA = 'CAMPANA'))
            WHERE TABLA IN ('MES', 'CAMPANA')
            ORDER BY 1,2,3,4
            );
        '''

        query_analisis_bc = '''
            DROP TABLE IF EXISTS #ANALISIS_BC;
            CREATE TABLE #ANALISIS_BC AS (
            SELECT
            TABLA
            ,IND_MC
            ,A.PROVEEDOR
            ,A.MARCA
            ,A.MES
            ,REGION
            ,STATE
            ,FORMATO_TIENDA
            ,STORE_DESCRIPTION
            ,NSE
            ,TIPO_FAMILIA
            ,CLASS_DESC
            ,SUBCLASS_DESC
            ,PROD_TYPE_DESC
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
            ,CAT_VENTA_AA
            ,CAT_UNIDADES_AA
            ,CAT_CLIENTES_AA
            ,CAT_TX_AA
            ,VENTA_ACTUAL_ONLINE
            ,CAT_VENTA_ACTUAL_ONLINE
            ,VENTA_ACTUAL_ANUAL
            ,VENTA_AA_ANUAL
            ,CAT_VENTA_ACTUAL_ANUAL
            ,CAT_VENTA_AA_ANUAL
            ,VENTA_CONDICION_1
            ,VENTA_CONDICION_2
            ,VENTA_CONDICION_3
            ,VENTA_CONDICION_4
            ,VENTA_CONDICION_5
            ,VENTA_CONDICION_6
            ,CLIENTES_CONDICION_1
            ,CLIENTES_CONDICION_2
            ,CLIENTES_CONDICION_3
            ,CLIENTES_CONDICION_4
            ,CLIENTES_CONDICION_5
            ,CLIENTES_CONDICION_6
            ,TX_CONDICION_1
            ,TX_CONDICION_2
            ,TX_CONDICION_3
            ,TX_CONDICION_4
            ,TX_CONDICION_5
            ,TX_CONDICION_6
            ,FRECUENCIA_TX
            ,FRECUENCIA_TX_AA
            ,CAT_FRECUENCIA_TX
            ,CAT_FRECUENCIA_TX_AA
            ,TX_MEDIO
            ,TX_MEDIO_AA
            ,CAT_TX_MEDIO
            ,CAT_TX_MEDIO_AA
            ,PRECIO_MEDIO
            ,PRECIO_MEDIO_AA
            ,CAT_PRECIO_MEDIO
            ,CAT_PRECIO_MEDIO_AA
            ,SHARE
            ,SHARE_AA
            ,CRECIMIENTO_SHARE
            ,CRECIMIENTO_FRECUENCIA_TX
            ,CAT_CRECIMIENTO_FRECUENCIA_TX
            ,CRECIMIENTO_TX_MEDIO
            ,CAT_CRECIMIENTO_TX_MEDIO
            ,CRECIMIENTO_PRECIO_MEDIO
            ,CAT_CRECIMIENTO_PRECIO_MEDIO
            ,CRECIMIENTO_VENTA
            ,CAT_CRECIMIENTO_VENTA
            ,CRECIMIENTO_VENTA_ANUAL
            ,CAT_CRECIMIENTO_VENTA_ANUAL
            ,"%VENTA_ONLINE"
            ,"%CAT_VENTA_ONLINE"
            ,"%CLIENTES_CONDICION_1"
            ,"%CLIENTES_CONDICION_2"
            ,"%CLIENTES_CONDICION_3"
            ,"%CLIENTES_CONDICION_4"
            ,"%CLIENTES_CONDICION_5"
            ,"%CLIENTES_CONDICION_6"
            ,TX_MEDIO_CONDICION_1
            ,TX_MEDIO_CONDICION_2
            ,TX_MEDIO_CONDICION_3
            ,TX_MEDIO_CONDICION_4
            ,TX_MEDIO_CONDICION_5
            ,TX_MEDIO_CONDICION_6

            ,B.IND_CAMPANA
            ,B.CLIENTES
            ,"%RECOMPRA"
            ,"%RECOMPRA_2M"
            ,"%RECOMPRA_3M"
            ,"%NUEVOS"
            ,"%FID"
            ,"%REC"
            ,"%DOR"
            ,"%PER"
            ,"%TASA_RECOMPRA"
            ,"%TASA_RECOMPRA_2M"
            ,"%TASA_RECOMPRA_3M"
            ,"%TASA_NUEVOS"
            ,"%TASA_FID"
            ,"%TASA_REC"
            ,"%TASA_DOR"
            ,"%TASA_PER"

            ,CLIENTES_RECOMPRA
            ,CAT_CLIENTES_RECOMPRA
            ,CLIENTES_RECOMPRA_AA
            ,CAT_CLIENTES_RECOMPRA_AA      
            ,PROMEDIO_DIAS_RECOMPRA
            ,CAT_PROMEDIO_DIAS_RECOMPRA
            ,PROMEDIO_DIAS_RECOMPRA_AA
            ,CAT_PROMEDIO_DIAS_RECOMPRA_AA
            ,CLIENTES_RECOMPRA_30
            ,CLIENTES_RECOMPRA_90
            ,CLIENTES_RECOMPRA_180
            ,CAT_CLIENTES_RECOMPRA_30
            ,CAT_CLIENTES_RECOMPRA_90
            ,CAT_CLIENTES_RECOMPRA_180
            ,PROMEDIO_DIAS_RECOMPRA_30
            ,PROMEDIO_DIAS_RECOMPRA_90
            ,PROMEDIO_DIAS_RECOMPRA_180
            ,CAT_PROMEDIO_DIAS_RECOMPRA_30
            ,CAT_PROMEDIO_DIAS_RECOMPRA_90
            ,CAT_PROMEDIO_DIAS_RECOMPRA_180

            FROM #AGG A
            LEFT JOIN #SEGMENTOS_CLIENTES B --USING(PROVEEDOR, MARCA, MES)
                ON A.PROVEEDOR = B.PROVEEDOR AND A.MARCA = B.MARCA AND ((A.MES = B.MES AND TABLA = 'MES') OR (B.IND_CAMPANA = 1 AND TABLA = 'CAMPANA'))
            LEFT JOIN #RECOMPRA C --USING(PROVEEDOR, MARCA, MES)
                ON A.PROVEEDOR = C.PROVEEDOR AND A.MARCA = C.MARCA AND ((A.MES = C.MES AND TABLA = 'MES') OR (C.IND_CAMPANA = 1 AND TABLA = 'CAMPANA'))
            --   WHERE TABLA IN ('MES', 'CAMPANA')
            ORDER BY 1,2,3,4
            );

            --@WBRESULT DATOS PARA ANALISIS BC
            --SELECT * FROM #ANALISIS_BC;

        '''
        return [query_bc, query_analisis_bc]

    def set_bc_variables(self, nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion, elegible):
        from dateutil.relativedelta import relativedelta

        inicio_aa = (pd.to_datetime(inicio_analisis) - pd.DateOffset(months=12)).strftime('%Y-%m-%d')
        fin_aa = (pd.to_datetime(fin_analisis) - pd.DateOffset(months=12)).strftime('%Y-%m-%d')

        self.dict_bc_analisis_var['nombre'] = nombre
        self.dict_bc_analisis_var['vigencia_ini_campana'] = inicio_campana
        self.dict_bc_analisis_var['vigencia_fin_campana'] = fin_campana
        self.dict_bc_analisis_var['vigencia_ini'] = inicio_analisis
        self.dict_bc_analisis_var['vigencia_fin'] = fin_analisis
        self.dict_bc_analisis_var['condicion_compra'] = condicion if bool(condicion) else 0
        self.dict_bc_analisis_var['elegible'] = elegible

        self.dict_bc_analisis_var['vigencia_ini_aa'] = inicio_aa
        self.dict_bc_analisis_var['vigencia_fin_aa'] = fin_aa
        self.dict_bc_analisis_var['date_dash_campana'] = f"'{inicio_campana}' AND '{fin_campana}'"
        self.dict_bc_analisis_var['date_dash'] = f"'{inicio_analisis}' AND '{fin_analisis}'"
        self.dict_bc_analisis_var['date_dash_aa'] = f"'{inicio_aa}' AND '{fin_aa}'"

        self.dict_bc_analisis_var['condicion_1'] = 50
        self.dict_bc_analisis_var['condicion_2'] = 100
        self.dict_bc_analisis_var['condicion_3'] = 150
        self.dict_bc_analisis_var['condicion_4'] = 200
        self.dict_bc_analisis_var['condicion_5'] = 300
        
    def create_table_analisis_bc(self, conn, override):
        query_agg = self.get_query_analisis_bc_agg()
        query_segmentos = self.get_query_analisis_bc_segmentos()
        query_recompra = self.get_query_analisis_bc_recompra()
        query_bc = self.get_query_bc()

        for query in tqdm(query_agg + query_segmentos + query_recompra + query_bc):
            if override or override is None:
                conn.execute(query)

        self.df_bc = conn.select('SELECT * FROM #BC ORDER BY 1,2,3,4')
        
        self.dict_df_analisis_bc = {
            'agg': conn.select('SELECT * FROM #AGG ORDER BY 1,2,3,4'),
            'segmentos': conn.select('SELECT * FROM #SEGMENTOS_CLIENTES ORDER BY 1,2,3,4'),
            'recompra': conn.select('SELECT * FROM #RECOMPRA ORDER BY 1,2,3,4')
        }

        self.df_analisis_bc = conn.select('SELECT * FROM #ANALISIS_BC ORDER BY 1,2,3,4')

    def get_analisis_bc_data(self):
        return self.df_analisis_bc, self.dict_df_analisis_bc
    
    def get_bc_data(self):
        return self.df_bc
