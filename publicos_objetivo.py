import pandas as pd

# Create a Class to handle the Monetizacion data that inherits from the Conn class
class PublicosObjetivo():
    
    def __init__(self):
        self.df_pos_agg = pd.DataFrame()
        self.df_bc_tx = pd.DataFrame()
        self.df_bc_unidades = pd.DataFrame()
        self.df_bc_tx_medio = pd.DataFrame()
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
        {f'INNER JOIN DIM_STORE B ON A.STORE_KEY = B.STORE_KEY AND A.STORE_CODE = B.STORE_CODE AND STORE_CODE IN {self.tiendas}' if self.tiendas else ''}
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
            ,CASE WHEN IND_11 = 1 AND IND_12 = 1 THEN 1 ELSE 0 END RECOMPRA_12
            ,CASE WHEN IND_10 = 1 AND IND_11 = 1 THEN 1 ELSE 0 END RECOMPRA_11
            ,CASE WHEN IND_9 = 1 AND IND_10 = 1 THEN 1 ELSE 0 END RECOMPRA_10
            ,CASE WHEN IND_8 = 1 AND IND_9 = 1 THEN 1 ELSE 0 END RECOMPRA_9
            ,CASE WHEN IND_7 = 1 AND IND_8 = 1 THEN 1 ELSE 0 END RECOMPRA_8
            ,CASE WHEN IND_6 = 1 AND IND_7 = 1 THEN 1 ELSE 0 END RECOMPRA_7
                --CAT
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

    def get_query_create_bc_tables(self, from_table='#PO'):
        query_bc_tx_unidades = f'''
        --DATOS ESCALONADOS DE TX Y UNIDADES
        DROP TABLE IF EXISTS #NUM_TX_UNIDADES;
        CREATE TABLE #NUM_TX_UNIDADES AS (
        SELECT
            'TX' TIPO
            ,CASE
            WHEN CAP_12 = 1 THEN '1 CAP'
            WHEN REC_12_6 = 1 THEN '2 REC'
            WHEN FID_6 = 1 THEN '3 FID'
            END AS PO
            ,CASE
            WHEN TX_CAMPANA = 1 THEN 'a) 1'
            WHEN TX_CAMPANA = 2 THEN 'b) 2'
            WHEN TX_CAMPANA = 3 THEN 'c) 3'
            WHEN TX_CAMPANA = 4 THEN 'd) 4'
            WHEN TX_CAMPANA = 5 THEN 'e) 5'
            WHEN TX_CAMPANA = 6 THEN 'f) 6'
            ELSE 'g) +7'
            END TX
            ,COUNT(DISTINCT CUSTOMER_CODE_TY) CLIENTES
        FROM {from_table}
        WHERE CAP_12_REC_12_FID_6 = 1
        GROUP BY 1,2,3
        
        UNION
        
        SELECT
            'UNIDADES' TIPO
            ,CASE
            WHEN CAP_12 = 1 THEN '1 CAP'
            WHEN REC_12_6 = 1 THEN '2 REC'
            WHEN FID_6 = 1 THEN '3 FID'
            END AS PO
            ,CASE
            WHEN UNIDADES_CAMPANA = 1 THEN 'a) 1'
            WHEN UNIDADES_CAMPANA = 2 THEN 'b) 2'
            WHEN UNIDADES_CAMPANA = 3 THEN 'c) 3'
            WHEN UNIDADES_CAMPANA = 4 THEN 'd) 4'
            WHEN UNIDADES_CAMPANA = 5 THEN 'e) 5'
            WHEN UNIDADES_CAMPANA = 6 THEN 'f) 6'
            ELSE 'g) +7'
            END TX
            ,COUNT(DISTINCT CUSTOMER_CODE_TY) CLIENTES
        FROM {from_table}

        WHERE CAP_12_REC_12_FID_6 = 1
        GROUP BY 1,2,3
        );
            '''
        
        query_bc_tx_medio = f'''
        --TX MEDIO DE CLIENTES CONTACTABLES
        DROP TABLE IF EXISTS #TX_MEDIO;
        CREATE TABLE #TX_MEDIO AS (
        SELECT
            CASE
            WHEN IND_MES BETWEEN 10 AND 12 THEN '3 MESES ANTES'
            WHEN IND_MES_CAMPANA = 1 THEN 'CAMPANA'
            END AS PERIODO
            ,COUNT(DISTINCT CUSTOMER_CODE_TY) CLIENTES
            ,SUM(VENTA) VENTA
            ,COUNT(DISTINCT INVOICE_NO) TX
            ,SUM(VENTA)::NUMERIC / TX TX_MEDIO
            ,SUM(VENTA)::NUMERIC / CLIENTES CONSUMO_MEDIO
        FROM #VENTA
        LEFT JOIN #MESES USING(MES)
        WHERE (IND_MES BETWEEN 10 AND 12 OR IND_MES_CAMPANA = 1)
        AND IND_MARCA = 1
        GROUP BY 1
        );
            '''
        return query_bc_tx_unidades, query_bc_tx_medio

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
        query_bc_tx_unidades, query_bc_tx_medio = self.get_query_create_bc_tables(from_table=from_table)
        table_name_tx_unidades = '#NUM_TX_UNIDADES'
        table_name_tx_medio = '#TX_MEDIO'
        
        # Si override no se especifica, la tabla no existe, se crea
        if override is None:
            # Crear tablas BC
            conn.execute(query=query_bc_tx_unidades)
            conn.execute(query=query_bc_tx_medio)

        # Si override es True, se sobreescribe la tabla
        elif override:
            conn.override_table(table_name_tx_unidades, query_bc_tx_unidades)
            conn.override_table(table_name_tx_medio, query_bc_tx_medio)
        # Si override es False, se espera que la tabla exista, no se hace nada. Salir de la función
        else:
            return

        self.df_bc_tx_unidades = conn.select(query=f'SELECT * FROM {table_name_tx_unidades} ORDER BY 1,2,3')
        self.df_bc_tx_medio = conn.select(query=f'SELECT * FROM {table_name_tx_medio} ORDER BY 1')
        
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
    def __get_filtros_listas(self):
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

    def get_query_select_po_envios_conteo(self, from_table):
        venta_antes, venta_camp, cond_antes, cond_camp = self.__get_filtros_listas()

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
        venta_antes, venta_camp, cond_antes, cond_camp = self.__get_filtros_listas()
        
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
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} AS (
                WITH __PO_ENVIOS AS (
                    SELECT
                        ROW_NUMBER() OVER(PARTITION BY ORDEN_SEGMENTO, VALID_CONTACT_INFO ORDER BY ORDEN_SEGMENTO, VALID_CONTACT_INFO, VENTA_MARCA DESC, VENTA_CAT DESC, CUSTOMER_CODE) ROW_N
                        ,*
                    FROM {from_table}
                    WHERE 1 = 1
                    AND ORDEN_SEGMENTO IN ('1 FID', '2 REC')
                    {venta_antes}
                    {venta_camp}
                    {cond_antes}
                    {cond_camp}

                    UNION

                    SELECT
                        ROW_NUMBER() OVER(PARTITION BY ORDEN_SEGMENTO, VALID_CONTACT_INFO ORDER BY ORDEN_SEGMENTO, VALID_CONTACT_INFO, VENTA_CAT DESC, CUSTOMER_CODE) ROW_N
                        ,*
                    FROM {from_table}
                    WHERE 1 = 1
                    AND ORDEN_SEGMENTO IN ('3 CAP')
                    {venta_antes}
                    {venta_camp}
                    {cond_antes}
                    {cond_camp}
                )
                SELECT * FROM __PO_ENVIOS

                WHERE VALID_CONTACT_INFO IN ('01 SMS', '02 MAIL', '03 MAIL & SMS', '04 INVALID CONTACT') --('01 SMS', '02 MAIL', '03 MAIL & SMS', '04 INVALID CONTACT')
                AND (IND_FID = 1 OR IND_REC = 1 OR IND_CAP = 1) --(IND_FID = 1 OR IND_REC = 1 OR IND_CAP = 1)

                AND (
                    --FID
                    (IND_FID = 1 AND VALID_CONTACT_INFO = '01 SMS'                AND ROW_N <= {fid_sms}/{porcentaje_gt})
                    OR (IND_FID = 1 AND VALID_CONTACT_INFO = '02 MAIL'            AND ROW_N <= {fid_email}/{porcentaje_gt})
                    OR (IND_FID = 1 AND VALID_CONTACT_INFO = '03 MAIL & SMS'      AND ROW_N <= {fid_sms_mail}/{porcentaje_gt})

                    --REC
                    OR (IND_REC = 1 AND VALID_CONTACT_INFO = '01 SMS'             AND ROW_N <= {rec_sms}/{porcentaje_gt})
                    OR (IND_REC = 1 AND VALID_CONTACT_INFO = '02 MAIL'            AND ROW_N <= {rec_email}/{porcentaje_gt})
                    OR (IND_REC = 1 AND VALID_CONTACT_INFO = '03 MAIL & SMS'      AND ROW_N <= {rec_sms_mail}/{porcentaje_gt})

                    --CAP
                    OR (IND_CAP = 1 AND VALID_CONTACT_INFO = '01 SMS'             AND ROW_N <= {cap_sms}/{porcentaje_gt})
                    OR (IND_CAP = 1 AND VALID_CONTACT_INFO = '02 MAIL'            AND ROW_N <= {cap_email}/{porcentaje_gt})
                    OR (IND_CAP = 1 AND VALID_CONTACT_INFO = '03 MAIL & SMS'      AND ROW_N <= {cap_sms_mail}/{porcentaje_gt})
                )
                
                ORDER BY ROW_N
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

        conn.execute(query=query)

        query = f'SELECT * FROM {table_name} ORDER BY ROW_N'
        
        self.df_listas_total = conn.select(query=query)
        
        # Separar las listas de envio por canal y agregar a un diccionario
        df_sms = self.df_listas_total[self.df_listas_total['valid_contact_info'] == '01 SMS'][['customer_code']]
        df_email = self.df_listas_total[self.df_listas_total['valid_contact_info'] == '02 MAIL'][['customer_code']]
        df_sms_email = self.df_listas_total[self.df_listas_total['valid_contact_info'] == '03 MAIL & SMS'][['customer_code']]

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
        df_sms_fid = df_sms[df_sms['orden_segmento'] == '1 FID'][['customer_code']]
        df_sms_rec = df_sms[df_sms['orden_segmento'] == '2 REC'][['customer_code']]
        df_sms_cap = df_sms[df_sms['orden_segmento'] == '3 CAP'][['customer_code']]

        df_email_fid = df_email[df_email['orden_segmento'] == '1 FID'][['customer_code']]
        df_email_rec = df_email[df_email['orden_segmento'] == '2 REC'][['customer_code']]
        df_email_cap = df_email[df_email['orden_segmento'] == '3 CAP'][['customer_code']]

        df_sms_email_fid = df_sms_email[df_sms_email['orden_segmento'] == '1 FID'][['customer_code']]
        df_sms_email_rec = df_sms_email[df_sms_email['orden_segmento'] == '2 REC'][['customer_code']]
        df_sms_email_cap = df_sms_email[df_sms_email['orden_segmento'] == '3 CAP'][['customer_code']]

        lis_df = [df_sms_fid, df_sms_rec, df_sms_cap, df_email_fid, df_email_rec, df_email_cap, df_sms_email_fid, df_sms_email_rec, df_sms_email_cap]
        lis_names = ['list_sms_fid', 'list_sms_rec', 'list_sms_cap', 'list_email_fid', 'list_email_rec', 'list_email_cap', 'list_sms_email_fid', 'list_sms_email_rec', 'list_sms_email_cap']

        # Guardar listas de envio en un diccionario. Guardar solo los df no vacios
        self.dict_listas_envios = {}
        for df, name in zip(lis_df, lis_names):
            if not df.empty:
                self.dict_listas_envios[name] = df

        # Agregar lista de envíos total
        self.dict_listas_envios['list_total'] = self.df_listas_total
        