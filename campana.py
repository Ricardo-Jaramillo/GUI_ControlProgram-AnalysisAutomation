import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
import pandas as pd

# Create a Class to handle the Radiografia data
class Campana():
    def __init__(self):
        self.set_dict_tablas()
    
    def set_dict_tablas(self):
        self.dict_tablas_sql = {'Descripción': 'MON_CAMP_DESC',
                                'Listas': 'MON_CAMP_LIST',
                                'Cupones': 'MON_CAMP_COUPON',
                                'Ofertas': 'MON_CAMP_OFFER_CODE'}
        
    def set_campana_variables(self, conn, nombre_campana):
        # Obtener el codigo de la campaña y obtener los queries para actualizar la campaña
        codigo_campana = conn.select(f"SELECT CODIGO_CAMPANA FROM CHEDRAUI.MON_CAMP_DESC WHERE NOMBRE = '{nombre_campana}'").iloc[0,0]
        
        # Calcular las variables necesarias para actualizar la campaña
        lis_df = self.get_campana_info(conn, nombre_campana)
        df_desc, df_lis, df_cupon, df_ofertas = lis_df

        # Asignar variables de la campaña para calculo de tabla de Venta
        condicion_compra = df_desc.condicion_compra[0]
        vigencia_ini = df_desc.vigencia_ini[0]
        vigencia_fin_campana = df_desc.vigencia_fin[0]
        # Define vigencia_fin como la fecha actual menos 1 dia si la campaña aún no termina
        vigencia_fin = pd.Timestamp.now().date() - pd.DateOffset(days=1) if pd.Timestamp.now().date() <= vigencia_fin_campana else vigencia_fin_campana
        # Calcular los días transcurridos de la campaña
        dias_campana = (vigencia_fin - vigencia_ini).days
        # Ajuste de temporalidad mes anterior
        dias_desfase_mes = conn.select(f"SELECT EXTRACT(DOW FROM '{vigencia_ini}'::DATE) - EXTRACT(DOW FROM '{vigencia_ini}'::DATE - INTERVAL '1 DAY' - INTERVAL '{dias_campana} DAYS')").iloc[0,0]
        # Periodos mes y año anterior según días transcurridos de la campaña
        vigencia_fin_mes_anterior = vigencia_ini - pd.DateOffset(days=1) + pd.DateOffset(days=dias_desfase_mes)
        vigencia_ini_mes_anterior = vigencia_fin_mes_anterior - pd.DateOffset(days=dias_campana)
        # Ajuste de temporalidad año anterior
        dias_desfase_ano = conn.select(f"SELECT EXTRACT(DOW FROM '{vigencia_ini}'::DATE) - EXTRACT(DOW FROM '{vigencia_ini}'::DATE - INTERVAL '1 YEAR')").iloc[0,0]
        # Periodos mes y año anterior según días transcurridos de la campaña
        vigencia_ini_ano_anterior = vigencia_ini - pd.DateOffset(months=12) + pd.DateOffset(days=dias_desfase_ano)
        vigencia_fin_ano_anterior = vigencia_fin - pd.DateOffset(months=12) + pd.DateOffset(days=dias_desfase_ano)
        # Periodos pre y post campaña
        fin_post_campana = vigencia_fin + pd.DateOffset(months=1)
        vigencia_ini_precampana = vigencia_ini - pd.DateOffset(months=1)
        vigencia_fin_precampana = vigencia_ini - pd.DateOffset(days=1)
        vigencia_ini_postcampana = vigencia_fin + pd.DateOffset(days=1)
        vigencia_fin_postcampana = pd.Timestamp.now().date() - pd.DateOffset(days=1) if pd.Timestamp.now().date() <= pd.Timestamp(fin_post_campana).date() else fin_post_campana
        vigencia_ini_precampana_a = vigencia_ini_precampana - pd.DateOffset(years=1) + pd.DateOffset(days=dias_desfase_ano)
        vigencia_fin_precampana_a = vigencia_fin_precampana - pd.DateOffset(years=1) + pd.DateOffset(days=dias_desfase_ano)
        vigencia_ini_postcampana_a = vigencia_ini_postcampana - pd.DateOffset(years=1) + pd.DateOffset(days=dias_desfase_ano)
        vigencia_fin_postcampana_a = vigencia_fin_postcampana - pd.DateOffset(years=1) + pd.DateOffset(days=dias_desfase_ano)
        # Calcular periodos para segmento de clientes
        ini_3 = vigencia_ini - pd.DateOffset(months=3)
        fin_3 = vigencia_ini - pd.DateOffset(days=1)
        ini_6 = vigencia_ini - pd.DateOffset(months=6)
        fin_6 = vigencia_ini - pd.DateOffset(months=3) - pd.DateOffset(days=1)
        ini_12 = vigencia_ini - pd.DateOffset(years=1)
        fin_12 = vigencia_ini - pd.DateOffset(months=6) - pd.DateOffset(days=1)
        ini_3_ano_anterior = vigencia_ini_ano_anterior - pd.DateOffset(months=3)
        fin_3_ano_anterior = vigencia_ini_ano_anterior - pd.DateOffset(days=1)
        ini_6_ano_anterior = vigencia_ini_ano_anterior - pd.DateOffset(months=6)
        fin_6_ano_anterior = vigencia_ini_ano_anterior - pd.DateOffset(months=3) - pd.DateOffset(days=1)
        ini_12_ano_anterior = vigencia_ini_ano_anterior - pd.DateOffset(years=1)
        fin_12_ano_anterior = vigencia_ini_ano_anterior - pd.DateOffset(months=6) - pd.DateOffset(days=1)

        # Variables para Datos Roi
        vigencia_ini_mes_anterior_ano_anterior = vigencia_ini_mes_anterior - pd.DateOffset(years=1)
        vigencia_fin_mes_anterior_ano_anterior = vigencia_fin_mes_anterior - pd.DateOffset(years=1)
        # Definir la vigencia de inicio acumulada como el primer día del mes de la fecha de inicio de la campaña menos 11 meses
        vigencia_ini_acum = pd.Timestamp(f"{vigencia_ini.year}-{vigencia_ini.month}-01") - pd.DateOffset(months=11)
        vigencia_fin_acum = vigencia_ini - pd.DateOffset(days=1)
        vigencia_ini_acum_ano_anterior = vigencia_ini_acum - pd.DateOffset(years=1)
        vigencia_fin_acum_ano_anterior = vigencia_fin_acum - pd.DateOffset(years=1)

        # Calcular date dashes
        date_dash = f"'{vigencia_ini}'::DATE AND '{vigencia_fin}'::DATE"
        date_dash_mes_anterior = f"'{vigencia_ini_mes_anterior}'::DATE AND '{vigencia_fin_mes_anterior}'::DATE"
        date_dash_ano_anterior = f"'{vigencia_ini_ano_anterior}'::DATE AND '{vigencia_fin_ano_anterior}'::DATE"
        date_dash_precampana = f"'{vigencia_ini_precampana}'::DATE AND '{vigencia_fin_precampana}'::DATE"
        date_dash_postcampana = f"'{vigencia_ini_postcampana}'::DATE AND '{vigencia_fin_postcampana}'::DATE"
        date_dash_precampana_a = f"'{vigencia_ini_precampana_a}'::DATE AND '{vigencia_fin_precampana_a}'::DATE"
        date_dash_postcampana_a = f"'{vigencia_ini_postcampana_a}'::DATE AND '{vigencia_fin_postcampana_a}'::DATE"
        date_dash_3 = f"'{ini_3}'::DATE AND '{fin_3}'::DATE"
        date_dash_6 = f"'{ini_6}'::DATE AND '{fin_6}'::DATE"
        date_dash_12 = f"'{ini_12}'::DATE AND '{fin_12}'::DATE"
        date_dash_3_ano_anterior = f"'{ini_3_ano_anterior}'::DATE AND '{fin_3_ano_anterior}'::DATE"
        date_dash_6_ano_anterior = f"'{ini_6_ano_anterior}'::DATE AND '{fin_6_ano_anterior}'::DATE"
        date_dash_12_ano_anterior = f"'{ini_12_ano_anterior}'::DATE AND '{fin_12_ano_anterior}'::DATE"
        date_dash_mes_anterior_ano_anterior = f"'{vigencia_ini_mes_anterior_ano_anterior}'::DATE AND '{vigencia_fin_mes_anterior_ano_anterior}'::DATE"
        date_dash_acum = f"'{vigencia_ini_acum}'::DATE AND '{vigencia_fin_acum}'::DATE"
        date_dash_acum_ano_anterior = f"'{vigencia_ini_acum_ano_anterior}'::DATE AND '{vigencia_fin_acum_ano_anterior}'::DATE"

        # Guardar en un diccionario las variables de la campaña
        self.campana_variables = {  'codigo_campana': codigo_campana,
                                    'condicion_compra': condicion_compra,
                                    'vigencia_ini': vigencia_ini,
                                    'vigencia_fin': vigencia_fin,
                                    'dias_campana': dias_campana,
                                    'dias_desfase_mes': dias_desfase_mes,
                                    'vigencia_ini_mes_anterior': vigencia_ini_mes_anterior,
                                    'vigencia_fin_mes_anterior': vigencia_fin_mes_anterior,
                                    'dias_desfase_ano': dias_desfase_ano,
                                    'vigencia_ini_ano_anterior': vigencia_ini_ano_anterior,
                                    'vigencia_fin_ano_anterior': vigencia_fin_ano_anterior,
                                    'vigencia_ini_precampana': vigencia_ini_precampana,
                                    'vigencia_fin_precampana': vigencia_fin_precampana,
                                    'vigencia_ini_postcampana': vigencia_ini_postcampana,
                                    'vigencia_fin_postcampana': vigencia_fin_postcampana,
                                    'vigencia_ini_precampana_a': vigencia_ini_precampana_a,
                                    'vigencia_fin_precampana_a': vigencia_fin_precampana_a,
                                    'vigencia_ini_postcampana_a': vigencia_ini_postcampana_a,
                                    'vigencia_fin_postcampana_a': vigencia_fin_postcampana_a,
                                    'date_dash': date_dash,
                                    'date_dash_mes_anterior': date_dash_mes_anterior,
                                    'date_dash_ano_anterior': date_dash_ano_anterior,
                                    'date_dash_precampana': date_dash_precampana,
                                    'date_dash_postcampana': date_dash_postcampana,
                                    'date_dash_precampana_a': date_dash_precampana_a,
                                    'date_dash_postcampana_a': date_dash_postcampana_a,
                                    'date_dash_3': date_dash_3,
                                    'date_dash_6': date_dash_6,
                                    'date_dash_12': date_dash_12,
                                    'date_dash_3_ano_anterior': date_dash_3_ano_anterior,
                                    'date_dash_6_ano_anterior': date_dash_6_ano_anterior,
                                    'date_dash_12_ano_anterior': date_dash_12_ano_anterior,
                                    'date_dash_mes_anterior_ano_anterior': date_dash_mes_anterior_ano_anterior,
                                    'date_dash_acum': date_dash_acum,
                                    'date_dash_acum_ano_anterior': date_dash_acum_ano_anterior
                                    }
        
        print('Variables de la campaña asignadas')
        print(date_dash_mes_anterior_ano_anterior)
        print(date_dash_acum)
        print(date_dash_acum_ano_anterior)

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

    def actualizar_resultados_campana(self, conn):
        # Obtener las queries para actualizar la campaña
        lis_queries_venta = self.get_queries_campana_venta()
        lis_queries_resultados = self.get_queries_campana_resultados()
        lis_queries_datos_roi = self.get_queries_campana_datos_roi()

        lis_queries = lis_queries_venta + lis_queries_resultados # + lis_queries_datos_roi

        # Ejecutar cada query, mostrando una barra de progreso con tqdm
        for query in tqdm(lis_queries):
            conn.execute(query)
            # pass

    def get_queries_campana_venta(self):
        
        query_venta_redencion = f'''
            --CREAR TABLA DE VENTA DE REDENCIONES
            DROP TABLE IF EXISTS #VENTA_REDENCION;
            CREATE TABLE #VENTA_REDENCION AS (
            --SI SE AGRUPA A NIVEL PRODUCTO NO AGRUPAR POR OFFERCODE YA QUE AL PARECER SE PUEDEN APLICAR VARIOS OFFER_CODE A UN PRODUCTO. SE REPITE EL LINE_NO, SE DUPLICA EN TABLA VENTA.
            WITH __FCT_SALE_LINE_DISCOUNT AS (
                SELECT
                    CUSTOMER_CODE AS CUSTOMER_CODE_TY
                    ,INVOICE_NO
                    ,INVOICE_DATE
                    ,PRODUCT_CODE
                    ,LINE_NO
                    ,A.OFFER_CODE
                    ,NAME
                    ,"DESC"
                    ,SALE_TOT_DISC_QTY
                    ,SALE_TOT_DISC_VAL
                FROM FCT_SALE_LINE_DISCOUNT A
                INNER JOIN CHEDRAUI.MON_CAMP_OFFER_CODE B ON A.OFFER_CODE = B.OFFER_CODE AND CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}'
                WHERE A.INVOICE_DATE BETWEEN {self.campana_variables['date_dash']}
                AND SALE_TOT_DISC_VAL > 0
                )
                ,__DESCUENTO_NIVEL_PRODUCTO AS (
                SELECT
                CUSTOMER_CODE_TY
                ,INVOICE_NO
                ,PRODUCT_CODE
                ,LINE_NO
                ,AVG(SALE_TOT_DISC_QTY) UNIDADES_PROMOCIONADAS
                ,SUM(SALE_TOT_DISC_VAL) COSTO_PROMOCIONADO
                ,COUNT(INVOICE_NO) PROMOCIONES_APLICADAS
                ,MIN(INVOICE_DATE) MIN_DATE
                ,MAX(INVOICE_DATE) MAX_DATE
                FROM __FCT_SALE_LINE_DISCOUNT
                GROUP BY 1,2,3,4
                )
                ,__DESCUENTO_NIVEL_OFFER_CODE_TX AS (
                SELECT
                    A.CUSTOMER_CODE_TY
                    ,A.OFFER_CODE
                    ,A.NAME
                    ,A."DESC"
                    ,A.INVOICE_NO
                    ,A.PRODUCT_CODE
                    ,A.LINE_NO
                    ,AVG(A.SALE_TOT_DISC_QTY) UNIDADES_PROMOCIONADAS
                    ,SUM(A.SALE_TOT_DISC_VAL) COSTO_PROMOCIONADO
                    ,COUNT(A.INVOICE_NO) PROMOCIONES_APLICADAS
                    ,SUM(B.SALE_NET_VAL) VENTA_PROMOCIONADA
                    ,MIN(A.INVOICE_DATE) MIN_DATE
                    ,MAX(A.INVOICE_DATE) MAX_DATE
                FROM __FCT_SALE_LINE_DISCOUNT A
                INNER JOIN (SELECT INVOICE_NO, PRODUCT_CODE, LINE_NO, SALE_NET_VAL FROM FCT_SALE_LINE WHERE INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} UNION ALL SELECT INVOICE_NO, PRODUCT_CODE, LINE_NO, SALE_NET_VAL FROM FCT_SALE_LINE_NM WHERE INVOICE_DATE BETWEEN {self.campana_variables['date_dash']}) B USING(INVOICE_NO, PRODUCT_CODE, LINE_NO)
                GROUP BY 1,2,3,4,5,6,7
                )
                ,__DESCUENTO_NIVEL_OFFER_CODE AS (
                SELECT
                    OFFER_CODE
                    ,NAME
                    ,"DESC"
                    ,SUM(UNIDADES_PROMOCIONADAS) UNIDADES_PROMOCIONADAS
                    ,SUM(COSTO_PROMOCIONADO) COSTO_PROMOCIONADO
                    ,SUM(PROMOCIONES_APLICADAS) PROMOCIONES_APLICADAS
                    ,SUM(VENTA_PROMOCIONADA) VENTA_PROMOCIONADA
                    ,MIN(MIN_DATE) MIN_DATE
                    ,MAX(MAX_DATE) MAX_DATE
                FROM __DESCUENTO_NIVEL_OFFER_CODE_TX
                GROUP BY 1,2,3
                )
                SELECT * FROM __DESCUENTO_NIVEL_PRODUCTO
            --     SELECT * FROM __DESCUENTO_NIVEL_OFFER_CODE
            );

            -- SELECT * FROM #VENTA_REDENCION;
        '''

        query_indicadores_tx = f'''
            --CREAR TABLA INDICADORES DE TX
            DROP TABLE IF EXISTS #INDICADORES_TX;
            CREATE TABLE #INDICADORES_TX AS (
            WITH __TX_PROVEEDORES AS (
                --PROVEEDOR PARTICIPANTE POR TX
                SELECT DISTINCT
                    INVOICE_NO
                    ,PROVEEDOR
                    ,MARCA
                    ,IND_MARCA
                FROM FCT_SALE_LINE
                INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
                WHERE (
                    INVOICE_DATE BETWEEN {self.campana_variables['date_dash']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana_a']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana_a']}
                    )
                AND SALE_NET_VAL > 0
                AND BUSINESS_TYPE = 'R'
                
                UNION
                
                SELECT DISTINCT
                    INVOICE_NO
                    ,PROVEEDOR
                    ,MARCA
                    ,IND_MARCA
                FROM FCT_SALE_LINE_NM
                INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
                WHERE (
                    INVOICE_DATE BETWEEN {self.campana_variables['date_dash']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana_a']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana_a']}
                    )
                AND SALE_NET_VAL > 0
                AND BUSINESS_TYPE = 'R'
            )
            ,__DATOS_TX AS (
                --AGRUPADO SOLO POR TX, EL TX ES ELEGIBLE SUMANDO LA VENTA DE TODOS LOS PRODUCTOS PARTICIPANTES (IND_MARCA = 1 O 0 PARA CAT)
                SELECT
                CUSTOMER_CODE_TY
                ,INVOICE_DATE
                ,INVOICE_NO
                ,IND_MARCA
                ,COALESCE(IND_ONLINE, 0) IND_ONLINE
                ,COALESCE(IND_CUP, 0) IND_CUP
                ,CASE WHEN C.IND_REGISTRO = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} THEN 1 ELSE 0 END AS IND_REGISTRO
                ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.campana_variables['condicion_compra']}'::INT THEN 1 ELSE 0 END AS IND_ELEGIBLE
                ,CASE WHEN IND_ELEGIBLE = 1 AND IND_REGISTRO = 1 THEN 1 ELSE 0 END IND_REGISTRO_ELEGIBLE
                FROM FCT_SALE_LINE A
                INNER JOIN #PRODUCTOS B USING(PRODUCT_CODE)
                LEFT JOIN (SELECT DISTINCT INVOICE_NO, 1::INT AS IND_REGISTRO FROM CHEDRAUI.MON_CAMP_REGISTERS WHERE CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}') C USING(INVOICE_NO)
                LEFT JOIN (SELECT DISTINCT INVOICE_NO, 1::INT AS IND_ONLINE FROM FCT_SALE_HEADER WHERE CHANNEL_TYPE IN ('WEB','APP','CC HY')) USING(INVOICE_NO)
                LEFT JOIN (SELECT DISTINCT EMISSION_INVOICE_NO INVOICE_NO, 1::INT IND_CUP FROM FCT_OFFER_COUPON A INNER JOIN CHEDRAUI.MON_CAMP_COUPON B ON B.CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}' AND A.OFFER_CODE = B.COUPON_ID) USING(INVOICE_NO)
                WHERE (
                    INVOICE_DATE BETWEEN {self.campana_variables['date_dash']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana_a']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana_a']}
                    )
                AND SALE_NET_VAL > 0
                AND BUSINESS_TYPE = 'R'
                GROUP BY 1,2,3,4,5,6,7,C.IND_REGISTRO,INVOICE_DATE
                
                UNION

                SELECT
                NULL CUSTOMER_CODE_TY
                ,INVOICE_DATE
                ,INVOICE_NO
                ,IND_MARCA
                ,COALESCE(IND_ONLINE, 0) IND_ONLINE
                ,COALESCE(IND_CUP, 0) IND_CUP
                ,CASE WHEN C.IND_REGISTRO = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} THEN 1 ELSE 0 END AS IND_REGISTRO
                ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.campana_variables['condicion_compra']}'::INT THEN 1 ELSE 0 END AS IND_ELEGIBLE
                ,CASE WHEN IND_ELEGIBLE = 1 AND IND_REGISTRO = 1 THEN 1 ELSE 0 END IND_REGISTRO_ELEGIBLE
                FROM FCT_SALE_LINE_NM A
                INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
                LEFT JOIN (SELECT DISTINCT INVOICE_NO, 1::INT AS IND_REGISTRO FROM CHEDRAUI.MON_CAMP_REGISTERS WHERE CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}') C USING(INVOICE_NO)
                LEFT JOIN (SELECT DISTINCT INVOICE_NO, 1::INT AS IND_ONLINE FROM FCT_SALE_HEADER WHERE CHANNEL_TYPE IN ('WEB','APP','CC HY')) USING(INVOICE_NO)
                LEFT JOIN (SELECT DISTINCT EMISSION_INVOICE_NO INVOICE_NO, 1::INT IND_CUP FROM FCT_OFFER_COUPON A INNER JOIN CHEDRAUI.MON_CAMP_COUPON B ON B.CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}' AND A.OFFER_CODE = B.COUPON_ID) USING(INVOICE_NO)
                WHERE (
                    INVOICE_DATE BETWEEN {self.campana_variables['date_dash']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana_a']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana_a']}
                    )
                AND SALE_NET_VAL > 0
                AND BUSINESS_TYPE = 'R'
                GROUP BY 1,2,3,4,5,6,7,C.IND_REGISTRO,INVOICE_DATE
            )
            --REPITE LOS INDICADORES DE TX PARA CADA PROVEEDOR, NO CALCULA LOS INDICADORES POR PROVEEDOR SEPARADO.
            SELECT
                *
            FROM __DATOS_TX
            LEFT JOIN __TX_PROVEEDORES USING(INVOICE_NO, IND_MARCA)
            );
        '''

        query_base_clientes = f'''
            --ENVIOS Y CONTACTACIÓN
            --LISTAS
            DROP TABLE IF EXISTS #LISTAS;
            CREATE TABLE #LISTAS AS (
            WITH __LISTAS AS (
            SELECT DISTINCT
                GRUPO
                ,LIST_ID
                ,ARC_DATE
                ,CONCAT(RIGHT(EXTRACT(YEAR FROM ARC_DATE), 2), CONCAT('-W', TO_CHAR(EXTRACT(WEEK FROM ARC_DATE), 'FM00'))) "WEEK"
            FROM MAP_CUST_LIST
            INNER JOIN (SELECT DISTINCT LIST_ID, 'GT' GRUPO FROM CHEDRAUI.MON_CAMP_LIST WHERE CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}' UNION SELECT DISTINCT LIST_ID_GC, 'GC' GRUPO FROM CHEDRAUI.MON_CAMP_LIST WHERE CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}') USING(LIST_ID)
            )
            SELECT
                GRUPO
                ,LIST_ID
                ,C.ARC_DATE
                ,WEEK
                ,CASE
                WHEN MAX(CASE WHEN COMM_CHANNEL_KEY = 1 THEN 1 ELSE 0 END) = 1 THEN 'MAIL'
                WHEN MAX(CASE WHEN COMM_CHANNEL_KEY = 2 THEN 1 ELSE 0 END) = 1 THEN 'SMS'
                WHEN MAX(CASE WHEN COMM_CHANNEL_KEY = 14 THEN 1 ELSE 0 END) = 1 THEN 'WA'
                END CANAL
            FROM AGG_PROMO_CUST_RESPONSE A 
            INNER JOIN DIM_PROMOTION B USING(PROMOTION_CODE)
            INNER JOIN __LISTAS C USING(LIST_ID)
            GROUP BY 1,2,3,4
            );

            -- SELECT * FROM #LISTAS;

            --CUPONES
            --BASE A CONTACTAR CUPON
            DROP TABLE IF EXISTS #BASE_CUPONES;
            CREATE TABLE #BASE_CUPONES AS (
            SELECT
                'GT' GRUPO
                ,A.OFFER_CODE::INT COUPON_ID
                ,'{self.campana_variables['vigencia_ini']}'::DATE INVOICE_DATE
                ,CONCAT(RIGHT(EXTRACT(YEAR FROM CASE WHEN INVOICE_DATE IS NULL THEN '{self.campana_variables['vigencia_ini']}'::DATE ELSE INVOICE_DATE END), 2), CONCAT('-W', TO_CHAR(EXTRACT(WEEK FROM CASE WHEN INVOICE_DATE IS NULL THEN '{self.campana_variables['vigencia_ini']}'::DATE ELSE INVOICE_DATE END), 'FM00'))) "WEEK"
                ,'CUPON' CANAL
                ,A.CUSTOMER_CODE_TY CUSTOMER_CODE
            FROM FCT_OFFER_COUPON A
            INNER JOIN CHEDRAUI.MON_CAMP_COUPON C ON A.OFFER_CODE = C.COUPON_ID
            --   AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash']}
            AND CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}'
            );

            -- SELECT * FROM #BASE_CUPONES;

            --LISTA DE CLIENTES A CONTACTAR
            DROP TABLE IF EXISTS #BASE_CLIENTES;
            CREATE TABLE #BASE_CLIENTES AS (
            WITH __BASE_CONTACTAR AS (
                SELECT DISTINCT
                GRUPO
                ,LIST_ID
                ,B.ARC_DATE::DATE DATE
                ,WEEK
                ,CANAL
                ,CUSTOMER_CODE
                FROM MAP_CUST_LIST A
                INNER JOIN #LISTAS B USING(LIST_ID)
                WHERE SORT_DELETE_FLG = 'F'
                
                UNION
                
                SELECT
                *
                FROM #BASE_CUPONES
            )

            -- SELECT * FROM __BASE_CONTACTAR;

            ,__BASE_ENVIOS AS(
                SELECT
                GRUPO
                ,LIST_ID
                ,C.ARC_DATE::DATE DATE
                ,WEEK
                ,CANAL
                ,CUSTOMER_CODE --EN BASE A CONTACTAR
                ,COUNT(CUSTOMER_CODE) BASE --BASE
                ,COUNT(DELVRD_CUSTOMER_CODE) ENVIOS --ENTREGADOS
                ,COUNT(OPEN_CUSTOMER_CODE) ABIERTOS --ABIERTOS
                FROM AGG_PROMO_CUST_RESPONSE A 
                INNER JOIN DIM_PROMOTION B USING(PROMOTION_CODE)
                INNER JOIN #LISTAS C USING(LIST_ID)
                GROUP BY 1,2,3,4,5,6
                
                UNION
                
                SELECT
                GRUPO
                ,COUPON_ID
                ,INVOICE_DATE DATE
                ,WEEK
                ,CANAL
                ,CUSTOMER_CODE
                ,COUNT(COUPON_ID) BASE
                ,COUNT(COUPON_ID) ENVIOS
                ,COUNT(COUPON_ID) ABIERTOS
                FROM #BASE_CUPONES
                GROUP BY 1,2,3,4,5,6
            )

            -- SELECT * FROM __BASE_ENVIOS;

                SELECT
                GRUPO
                ,LIST_ID
                ,DATE
                ,WEEK
                ,CANAL
                ,COALESCE(A.CUSTOMER_CODE, B.CUSTOMER_CODE) CUSTOMER_CODE_TY
                ,COALESCE(BASE, 1) BASE
                ,COALESCE(ENVIOS, 0) ENVIOS
                ,COALESCE(CASE WHEN CANAL = 'SMS' THEN ENVIOS ELSE ABIERTOS END, 0) ABIERTOS
                FROM __BASE_CONTACTAR A
                FULL JOIN __BASE_ENVIOS B USING(CUSTOMER_CODE, LIST_ID, WEEK, CANAL, DATE, GRUPO)
            );
            --@WBRESULT BASE DE ENVIOS
            -- SELECT * FROM #BASE_CLIENTES;
        '''

        query_indicadores_clientes = f'''
            --CREAR TABLA DE INDICADORES POR CLIENTE - SOLO SE CONSIDERARÁN CLIENTES CON COMPRA AL SER UN LEFT JOIN SOBRE CLIENTES CON COMPRA, LOS CLIENTES CON ENVÍO Y SIN COMPRA NO APARECEN AQUÍ. PARA COMPARA ENVIOS VS CON COMPRA VER CODIGO DEL FUNNEL DE CONTACTACION
            DROP TABLE IF EXISTS #INDICADORES_CLIENTES;
            CREATE TABLE #INDICADORES_CLIENTES AS (
            WITH __BASE_CONTACTACION AS (
                SELECT
                GRUPO
                ,CUSTOMER_CODE_TY
                --BASE
                ,MAX(CASE WHEN BASE > 0 THEN 1 ELSE 0 END) IND_BASE
                ,MAX(CASE WHEN BASE > 0 AND CANAL = 'MAIL' THEN 1 ELSE 0 END) IND_BASE_MAIL
                ,MAX(CASE WHEN BASE > 0 AND CANAL = 'SMS' THEN 1 ELSE 0 END) IND_BASE_SMS
                ,MAX(CASE WHEN BASE > 0 AND CANAL = 'WA' THEN 1 ELSE 0 END) IND_BASE_WA
                ,0::INT /*MAX(CASE WHEN BASE > 0 AND CANAL = 'WIFI' THEN 1 ELSE 0 END)*/ IND_BASE_WIFI
                ,MAX(CASE WHEN BASE > 0 AND CANAL = 'CUPON' THEN 1 ELSE 0 END) IND_BASE_CUPON

                --ENVIOS
                ,MAX(CASE WHEN ENVIOS > 0 THEN 1 ELSE 0 END) IND_ENVIOS
                ,MAX(CASE WHEN ENVIOS > 0 AND CANAL = 'MAIL' THEN 1 ELSE 0 END) IND_ENVIOS_MAIL
                ,MAX(CASE WHEN ENVIOS > 0 AND CANAL = 'SMS' THEN 1 ELSE 0 END) IND_ENVIOS_SMS
                ,MAX(CASE WHEN ENVIOS > 0 AND CANAL = 'WA' THEN 1 ELSE 0 END) IND_ENVIOS_WA
                ,0::INT /*MAX(CASE WHEN ENVIOS > 0 AND CANAL = 'WIFI' THEN 1 ELSE 0 END)*/ IND_ENVIOS_WIFI
                ,MAX(CASE WHEN ENVIOS > 0 AND CANAL = 'CUPON' THEN 1 ELSE 0 END) IND_ENVIOS_CUPON
                
                --ABIERTOS
                ,MAX(CASE WHEN ABIERTOS > 0 THEN 1 ELSE 0 END) IND_ABIERTOS
                ,MAX(CASE WHEN ABIERTOS > 0 AND CANAL = 'MAIL' THEN 1 ELSE 0 END) IND_ABIERTOS_MAIL
                ,MAX(CASE WHEN ABIERTOS > 0 AND CANAL = 'SMS' THEN 1 ELSE 0 END) IND_ABIERTOS_SMS
                ,MAX(CASE WHEN ABIERTOS > 0 AND CANAL = 'WA' THEN 1 ELSE 0 END) IND_ABIERTOS_WA
                ,0::INT /*MAX(CASE WHEN ABIERTOS > 0 AND CANAL = 'WIFI' THEN 1 ELSE 0 END)*/ IND_ABIERTOS_WIFI
                ,MAX(CASE WHEN ABIERTOS > 0 AND CANAL = 'CUPON' THEN 1 ELSE 0 END) IND_ABIERTOS_CUPON
                
                FROM #BASE_CLIENTES
                GROUP BY 1,2
            )
            ,__IND_CLIENTES_ENVIOS AS (
                SELECT * FROM __BASE_CONTACTACION WHERE GRUPO = 'GT'
                UNION
                SELECT * FROM __BASE_CONTACTACION WHERE GRUPO = 'GC' AND CUSTOMER_CODE_TY NOT IN (SELECT DISTINCT CUSTOMER_CODE_TY FROM #BASE_CLIENTES WHERE GRUPO = 'GT')
            )
            ,__IND_CLIENTES_SEGMENTOS AS (
            SELECT
                CUSTOMER_CODE_TY
                ,IND_MARCA
                ,PROVEEDOR
            --     ,MARCA

                --INDICADORES DE TX
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} AND IND_REGISTRO = 1 THEN 1 ELSE 0 END) IND_CLIENTE_REGISTRO
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} AND IND_ELEGIBLE = 1 THEN 1 ELSE 0 END) IND_CLIENTE_ELEGIBLE
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} AND IND_REGISTRO_ELEGIBLE = 1 THEN 1 ELSE 0 END) IND_CLIENTE_REGISTRO_ELEGIBLE

                --INDICADORES DE PERIODO
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} THEN 1 ELSE 0 END) IND_CLIENTE_CAMPANA
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash_mes_anterior']} THEN 1 ELSE 0 END) IND_CLIENTE_MES_ANTERIOR
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']} THEN 1 ELSE 0 END) IND_CLIENTE_ANO_ANTERIOR
                
                --INIDICADORES DE SEGMENTO DE CLIENTES
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} THEN 1 ELSE 0 END) IND_CAMPANA
            --     ,MAX(CASE WHEN IND_MARCA = 1 AND B.IND_COMPRA_PREVIA IS NOT NULL THEN 1 ELSE 0 END) IND_ANTES_CAMPANA
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash_3']} THEN 1 ELSE 0 END) IND_3
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash_6']} THEN 1 ELSE 0 END) IND_6
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash_12']} THEN 1 ELSE 0 END) IND_12
                
                ,COUNT(DISTINCT CASE WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} AND IND_MARCA = 1 THEN INVOICE_NO END) TX_MARCA
                ,COUNT(DISTINCT CASE WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} AND IND_MARCA = 0 THEN INVOICE_NO END) TX_COMPETENCIA

                --INDICADORES_SEGMENTOS
                ,CASE WHEN IND_CAMPANA = 1 AND IND_3 = 1 THEN 1 ELSE 0 END AS IND_FID_3
                ,CASE WHEN IND_CAMPANA = 1 AND IND_6 = 1 AND IND_3 = 0 THEN 1 ELSE 0 END AS IND_REC_DOR
                ,CASE WHEN IND_CAMPANA = 1 AND IND_12 = 1 AND IND_6 = 0 AND IND_3 = 0 THEN 1 ELSE 0 END AS IND_REC_PER  
                ,CASE WHEN IND_CAMPANA = 1 AND IND_REC_DOR = 1 OR IND_REC_PER = 1 THEN 1 ELSE 0 END IND_REC
                ,CASE WHEN IND_CAMPANA = 1 AND IND_FID_3 = 0 AND IND_REC = 0 THEN 1 ELSE 0 END AS IND_NUEVO
                ,CASE WHEN TX_MARCA = 1 THEN 1 ELSE 0 END AS IND_COMPRA_UNA_VEZ
                ,CASE WHEN TX_MARCA > 1 THEN 1 ELSE 0 END AS IND_REPETIDOR
                ,CASE WHEN TX_MARCA = 1 AND TX_COMPETENCIA = 0 THEN 1 ELSE 0 END AS IND_COMPRA_SOLO_MARCA_UNA_VEZ  
                ,CASE WHEN TX_MARCA > 1 AND TX_COMPETENCIA = 0 THEN 1 ELSE 0 END AS IND_LEAL
                
                --AÑO ANTERIOR - INIDICADORES DE SEGMENTO DE CLIENTES
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']} THEN 1 ELSE 0 END) IND_CAMPANA_AA
            --     ,MAX(CASE WHEN IND_MARCA = 1 AND B.IND_COMPRA_PREVIA IS NOT NULL THEN 1 ELSE 0 END) IND_ANTES_CAMPANA
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash_3_ano_anterior']} THEN 1 ELSE 0 END) IND_3_AA
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash_6_ano_anterior']} THEN 1 ELSE 0 END) IND_6_AA
                ,MAX(CASE WHEN IND_MARCA = 1 AND INVOICE_DATE BETWEEN {self.campana_variables['date_dash_12_ano_anterior']} THEN 1 ELSE 0 END) IND_12_AA
                
                ,COUNT(DISTINCT CASE WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']} AND IND_MARCA = 1 THEN INVOICE_NO END) TX_MARCA_AA
                ,COUNT(DISTINCT CASE WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']} AND IND_MARCA = 0 THEN INVOICE_NO END) TX_COMPETENCIA_AA

                --INDICADORES_SEGMENTOS
                ,CASE WHEN IND_CAMPANA_AA = 1 AND IND_3_AA = 1 THEN 1 ELSE 0 END AS IND_FID_3_AA
                ,CASE WHEN IND_CAMPANA_AA = 1 AND IND_6_AA = 1 AND IND_3_AA = 0 THEN 1 ELSE 0 END AS IND_REC_DOR_AA
                ,CASE WHEN IND_CAMPANA_AA = 1 AND IND_12_AA = 1 AND IND_6_AA = 0 AND IND_3_AA = 0 THEN 1 ELSE 0 END AS IND_REC_PER_AA
                ,CASE WHEN IND_CAMPANA_AA = 1 AND IND_REC_DOR_AA = 1 OR IND_REC_PER_AA = 1 THEN 1 ELSE 0 END IND_REC_AA
                ,CASE WHEN IND_CAMPANA_AA = 1 AND IND_FID_3_AA = 0 AND IND_REC_AA = 0 THEN 1 ELSE 0 END AS IND_NUEVO_AA
                ,CASE WHEN TX_MARCA_AA = 1 THEN 1 ELSE 0 END AS IND_COMPRA_UNA_VEZ_AA
                ,CASE WHEN TX_MARCA_AA > 1 THEN 1 ELSE 0 END AS IND_REPETIDOR_AA
                ,CASE WHEN TX_MARCA_AA = 1 AND TX_COMPETENCIA_AA = 0 THEN 1 ELSE 0 END AS IND_COMPRA_SOLO_MARCA_UNA_VEZ_AA
                ,CASE WHEN TX_MARCA_AA > 1 AND TX_COMPETENCIA_AA = 0 THEN 1 ELSE 0 END AS IND_LEAL_AA
                
            FROM #INDICADORES_TX A
            GROUP BY 1,2,3--,4
            )
            SELECT
                CUSTOMER_CODE_TY
                ,GRUPO
                ,IND_MARCA
                ,PROVEEDOR
            --     ,MARCA
                --BASE
                ,COALESCE(IND_BASE, 0) IND_BASE
                ,COALESCE(IND_BASE_MAIL, 0) IND_BASE_MAIL
                ,COALESCE(IND_BASE_SMS, 0) IND_BASE_SMS
                ,COALESCE(IND_BASE_WA, 0) IND_BASE_WA
                ,COALESCE(IND_BASE_WIFI, 0) IND_BASE_WIFI
                ,COALESCE(IND_BASE_CUPON, 0) IND_BASE_CUPON
                --ENVIOS
                ,COALESCE(IND_ENVIOS, 0) IND_ENVIOS
                ,COALESCE(IND_ENVIOS_MAIL, 0) IND_ENVIOS_MAIL
                ,COALESCE(IND_ENVIOS_SMS, 0) IND_ENVIOS_SMS
                ,COALESCE(IND_ENVIOS_WA, 0) IND_ENVIOS_WA
                ,COALESCE(IND_ENVIOS_WIFI, 0) IND_ENVIOS_WIFI
                ,COALESCE(IND_ENVIOS_CUPON, 0) IND_ENVIOS_CUPON
                --ABIERTOS
                ,COALESCE(IND_ABIERTOS, 0) IND_ABIERTOS
                ,COALESCE(IND_ABIERTOS_MAIL, 0) IND_ABIERTOS_MAIL
                ,COALESCE(IND_ABIERTOS_SMS, 0) IND_ABIERTOS_SMS
                ,COALESCE(IND_ABIERTOS_WA, 0) IND_ABIERTOS_WA
                ,COALESCE(IND_ABIERTOS_WIFI, 0) IND_ABIERTOS_WIFI
                ,COALESCE(IND_ABIERTOS_CUPON, 0) IND_ABIERTOS_CUPON
                --INDICADORES
                ,IND_CLIENTE_REGISTRO
                ,IND_CLIENTE_ELEGIBLE
                ,IND_CLIENTE_REGISTRO_ELEGIBLE

                ,IND_CLIENTE_CAMPANA
                ,IND_CLIENTE_MES_ANTERIOR
                ,IND_CLIENTE_ANO_ANTERIOR

                ,IND_NUEVO AS IND_CLIENTE_NUEVO
                ,IND_FID_3 AS IND_CLIENTE_FID
                ,IND_REC AS IND_CLIENTE_REC
                ,IND_REC_DOR AS IND_CLIENTE_REC_DORMIDO
                ,IND_REC_PER AS IND_CLIENTE_REC_PERDIDO

                ,IND_COMPRA_UNA_VEZ AS IND_CLIENTE_COMPRA_UNA_VEZ
                ,IND_REPETIDOR AS IND_CLIENTE_REPETIDOR
                ,IND_COMPRA_SOLO_MARCA_UNA_VEZ AS IND_CLIENTE_COMPRA_SOLO_MARCA_UNA_VEZ
                ,IND_LEAL AS IND_CLIENTE_LEAL

                ,IND_NUEVO_AA AS IND_CLIENTE_NUEVO_ANO_ANTERIOR
                ,IND_FID_3_AA AS IND_CLIENTE_FID_ANO_ANTERIOR
                ,IND_REC_AA AS IND_CLIENTE_REC_ANO_ANTERIOR
            FROM __IND_CLIENTES_SEGMENTOS
            LEFT JOIN __IND_CLIENTES_ENVIOS USING(CUSTOMER_CODE_TY)
            );
        '''

        query_venta = f'''
            DROP TABLE IF EXISTS CHEDRAUI.MON_VTA;
            CREATE TABLE CHEDRAUI.MON_VTA AS (
            SELECT
                1::INT AS IND_MC
                ,A.PRODUCT_CODE
                ,A.INVOICE_NO
                ,A.INVOICE_DATE
                ,A.CUSTOMER_CODE_TY AS CUSTOMER_CODE

                ,COALESCE(B.NSE, 'NO SEGMENTADO') NSE
                ,COALESCE(B.TIPO_FAMILIA, 'NO SEGMENTADO') TIPO_FAMILIA
                ,COALESCE(B.CONTACT_INFO, 'NO SEGMENTADO') CONTACT_INFO

                ,C.REGION
                ,C.STATE
                ,C.FORMATO_TIENDA
                ,C.STORE_FORMAT2
                ,C.STORE_DESCRIPTION

                ,D.CLASS_DESC
                ,D.SUBCLASS_DESC
                ,D.PROD_TYPE_DESC
                ,D.PRODUCT_DESCRIPTION
            --         ,D.MEDIDA
            --         ,D.VALOR_MEDIDA
                ,D.PROVEEDOR
                ,D.MARCA
                ,D.IND_MARCA
                ,D.IND_DUPLICADO

                ,CASE
                    WHEN A.INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} THEN 'CAMPANA'
                    WHEN A.INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']} THEN 'ANO_ANTERIOR'
                    WHEN A.INVOICE_DATE BETWEEN {self.campana_variables['date_dash_mes_anterior']} THEN 'MES_ANTERIOR'
                END AS PERIODO
                ,CASE
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} THEN 'CAMPANA'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana']} THEN 'PRE_CAMPANA'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana']} THEN 'POST_CAMPANA'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']} THEN 'CAMPANA_A'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana_a']} THEN 'PRE_CAMPANA_A'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana_a']} THEN 'POST_CAMPANA_A'
                END AS PERIODO_ANALISIS

                ,IND_ONLINE
                ,IND_CUP      
                ,IND_ELEGIBLE
                ,IND_REGISTRO
                ,IND_REGISTRO_ELEGIBLE

                ,GRUPO
                ,IND_BASE
                ,IND_BASE_MAIL
                ,IND_BASE_SMS
                ,IND_BASE_WA
                ,IND_BASE_WIFI
                ,IND_BASE_CUPON
            --     
                ,IND_ENVIOS
                ,IND_ENVIOS_MAIL
                ,IND_ENVIOS_SMS
                ,IND_ENVIOS_WA
                ,IND_ENVIOS_WIFI
                ,IND_ENVIOS_CUPON
                
                ,IND_ABIERTOS
                ,IND_ABIERTOS_MAIL
                ,IND_ABIERTOS_SMS
                ,IND_ABIERTOS_WA
                ,IND_ABIERTOS_WIFI
                ,IND_ABIERTOS_CUPON

                ,IND_CLIENTE_REGISTRO
                ,IND_CLIENTE_ELEGIBLE
                ,IND_CLIENTE_REGISTRO_ELEGIBLE

                ,IND_CLIENTE_CAMPANA
                ,IND_CLIENTE_MES_ANTERIOR
                ,IND_CLIENTE_ANO_ANTERIOR

                ,IND_CLIENTE_NUEVO
                ,IND_CLIENTE_FID
                ,IND_CLIENTE_REC
                ,IND_CLIENTE_REC_DORMIDO
                ,IND_CLIENTE_REC_PERDIDO

                ,IND_CLIENTE_COMPRA_UNA_VEZ
                ,IND_CLIENTE_REPETIDOR
                ,IND_CLIENTE_COMPRA_SOLO_MARCA_UNA_VEZ
                ,IND_CLIENTE_LEAL

                ,IND_CLIENTE_NUEVO_ANO_ANTERIOR
                ,IND_CLIENTE_FID_ANO_ANTERIOR
                ,IND_CLIENTE_REC_ANO_ANTERIOR

                ,SUM(UNIDADES_PROMOCIONADAS) UNIDADES_PROMOCIONADAS
                ,SUM(COSTO_PROMOCIONADO) COSTO_PROMOCIONADO
                ,SUM(PROMOCIONES_APLICADAS) PROMOCIONES_APLICADAS

                ,SUM(A.SALE_NET_VAL) AS VENTA
                ,SUM(A.SALE_TOT_QTY) AS UNIDADES

            FROM FCT_SALE_LINE A
            INNER JOIN #PRODUCTOS D USING(PRODUCT_CODE)
            LEFT JOIN #INDICADORES_TX E USING(INVOICE_NO, IND_MARCA, CUSTOMER_CODE_TY, PROVEEDOR, MARCA, INVOICE_DATE)
            LEFT JOIN #INDICADORES_CLIENTES G USING(CUSTOMER_CODE_TY, IND_MARCA, PROVEEDOR)--, MARCA
            LEFT JOIN #VENTA_REDENCION I USING(INVOICE_NO, PRODUCT_CODE, LINE_NO)
            LEFT JOIN CHEDRAUI.V_CUSTOMER_CONTACT AS B ON A.CUSTOMER_CODE_TY = B.CUSTOMER_CODE-- AND B.CONTACT_INFO IS NOT NULL
            LEFT JOIN CHEDRAUI.V_STORE C ON A.STORE_KEY = C.STORE_KEY AND A.STORE_CODE = C.STORE_CODE --AND A.STORE_CODE IN $[TIENDAS]
            WHERE (
                    INVOICE_DATE BETWEEN {self.campana_variables['date_dash']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana_a']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana_a']}
                    )
            AND A.SALE_NET_VAL > 0
            AND A.BUSINESS_TYPE = 'R'
            --   AND C.FORMATO_TIENDA IN ('01 SELECTO','02 AB','03 CD')
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65

            UNION ALL
            
            SELECT
                0::INT AS IND_MC
                ,A.PRODUCT_CODE
                ,A.INVOICE_NO
                ,A.INVOICE_DATE
                ,NULL CUSTOMER_CODE

                ,NULL NSE
                ,NULL TIPO_FAMILIA
                ,NULL CONTACT_INFO

                ,C.REGION
                ,C.STATE
                ,C.FORMATO_TIENDA
                ,C.STORE_FORMAT2
                ,C.STORE_DESCRIPTION

                ,D.CLASS_DESC
                ,D.SUBCLASS_DESC
                ,D.PROD_TYPE_DESC
                ,D.PRODUCT_DESCRIPTION
            --         ,D.MEDIDA
            --         ,D.VALOR_MEDIDA
                ,D.PROVEEDOR
                ,D.MARCA
                ,D.IND_MARCA
                ,D.IND_DUPLICADO

                ,CASE
                    WHEN A.INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} THEN 'CAMPANA'
                    WHEN A.INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']} THEN 'ANO_ANTERIOR'
                    WHEN A.INVOICE_DATE BETWEEN {self.campana_variables['date_dash_mes_anterior']} THEN 'MES_ANTERIOR'
                END AS PERIODO
                ,CASE
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} THEN 'CAMPANA'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana']} THEN 'PRE_CAMPANA'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana']} THEN 'POST_CAMPANA'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']} THEN 'CAMPANA_A'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana_a']} THEN 'PRE_CAMPANA_A'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana_a']} THEN 'POST_CAMPANA_A'
                END AS PERIODO_ANALISIS

                ,IND_ONLINE
                ,IND_CUP      
                ,IND_ELEGIBLE
                ,IND_REGISTRO
                ,IND_REGISTRO_ELEGIBLE
            -- 
                ,NULL GRUPO
                ,NULL::INT IND_BASE
                ,NULL::INT IND_BASE_MAIL
                ,NULL::INT IND_BASE_SMS
                ,NULL::INT IND_BASE_WA
                ,NULL::INT IND_BASE_WIFI
                ,NULL::INT IND_BASE_CUPON
            --     
                ,NULL::INT IND_ENVIOS
                ,NULL::INT IND_ENVIOS_MAIL
                ,NULL::INT IND_ENVIOS_SMS
                ,NULL::INT IND_ENVIOS_WA
                ,NULL::INT IND_ENVIOS_WIFI
                ,NULL::INT IND_ENVIOS_CUPON
                
                ,NULL::INT IND_ABIERTOS
                ,NULL::INT IND_ABIERTOS_MAIL
                ,NULL::INT IND_ABIERTOS_SMS
                ,NULL::INT IND_ABIERTOS_WA
                ,NULL::INT IND_ABIERTOS_WIFI
                ,NULL::INT IND_ABIERTOS_CUPON

                ,NULL::INT IND_CLIENTE_REGISTRO
                ,NULL::INT IND_CLIENTE_ELEGIBLE
                ,NULL::INT IND_CLIENTE_REGISTRO_ELEGIBLE

                ,NULL::INT IND_CLIENTE_CAMPANA
                ,NULL::INT IND_CLIENTE_MES_ANTERIOR
                ,NULL::INT IND_CLIENTE_ANO_ANTERIOR

                ,NULL::INT IND_CLIENTE_NUEVO
                ,NULL::INT IND_CLIENTE_FID
                ,NULL::INT IND_CLIENTE_REC
                ,NULL::INT IND_CLIENTE_REC_DORMIDO
                ,NULL::INT IND_CLIENTE_REC_PERDIDO

                ,NULL::INT IND_CLIENTE_COMPRA_UNA_VEZ
                ,NULL::INT IND_CLIENTE_REPETIDOR
                ,NULL::INT IND_CLIENTE_COMPRA_SOLO_MARCA_UNA_VEZ
                ,NULL::INT IND_CLIENTE_LEAL

                ,NULL::INT IND_CLIENTE_NUEVO_ANO_ANTERIOR
                ,NULL::INT IND_CLIENTE_FID_ANO_ANTERIOR
                ,NULL::INT IND_CLIENTE_REC_ANO_ANTERIOR

                ,SUM(UNIDADES_PROMOCIONADAS) UNIDADES_PROMOCIONADAS
                ,SUM(COSTO_PROMOCIONADO) COSTO_PROMOCIONADO
                ,SUM(PROMOCIONES_APLICADAS) PROMOCIONES_APLICADAS

                ,SUM(A.SALE_NET_VAL) AS VENTA
                ,SUM(A.SALE_TOT_QTY) AS UNIDADES

            FROM FCT_SALE_LINE_NM A
            INNER JOIN #PRODUCTOS D ON A.PRODUCT_CODE = D.PRODUCT_CODE
            LEFT JOIN #INDICADORES_TX E USING(INVOICE_NO, IND_MARCA, PROVEEDOR, MARCA, INVOICE_DATE)
            --   LEFT JOIN #INDICADORES_CLIENTES G ON A.CUSTOMER_CODE = G.CUSTOMER_CODE AND D.IND_MARCA = G.IND_MARCA AND D.PROVEEDOR = G.PROVEEDOR
            LEFT JOIN #VENTA_REDENCION I ON A.INVOICE_NO = I.INVOICE_NO AND A.PRODUCT_CODE = I.PRODUCT_CODE
            --   LEFT JOIN CHEDRAUI.V_CUSTOMER_CONTACT AS B ON A.CUSTOMER_CODE_TY = B.CUSTOMER_CODE-- AND B.CONTACT_INFO IS NOT NULL
            LEFT JOIN CHEDRAUI.V_STORE C ON A.STORE_KEY = C.STORE_KEY AND A.STORE_CODE = C.STORE_CODE --AND A.STORE_CODE IN $[TIENDAS]
            WHERE (
                    INVOICE_DATE BETWEEN {self.campana_variables['date_dash']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_precampana_a']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_postcampana_a']}
                    )
            AND A.SALE_NET_VAL > 0
            AND A.BUSINESS_TYPE = 'R'
            --   AND C.FORMATO_TIENDA IN ('01 SELECTO','02 AB','03 CD')
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65
            );

        '''

        lis_queries = [query_venta_redencion, query_indicadores_tx, query_base_clientes, query_indicadores_clientes, query_venta]
        return lis_queries #['SELECT 1 AS COLUMNA']
    
    def get_queries_campana_resultados(self):
        query_dias_semana = f'''
            --CREAR DIAS Y SEMANAS DEL AÑO
            DROP TABLE IF EXISTS #DIAS;
            CREATE TABLE #DIAS AS (
            WITH __DIAS AS (
                SELECT DISTINCT
                INVOICE_DATE
                ,PERIODO
                ,CONCAT(RIGHT(EXTRACT(YEAR FROM INVOICE_DATE), 2), CONCAT('-W', TO_CHAR(EXTRACT(WEEK FROM INVOICE_DATE), 'FM00'))) "WEEK"
                FROM CHEDRAUI.MON_VTA
                WHERE PERIODO IN ('CAMPANA', 'ANO_ANTERIOR', 'MES_ANTERIOR')
                ORDER BY INVOICE_DATE
            )
            SELECT
                *
                ,RANK() OVER(PARTITION BY PERIODO ORDER BY PERIODO, INVOICE_DATE) DAY_NUMBER
            FROM __DIAS
            );
        '''

        query_res_agg = f'''
                    --CAMPANA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' THEN VENTA END) CAMPANA_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' THEN UNIDADES END) CAMPANA_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' THEN INVOICE_NO END) CAMPANA_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' THEN CUSTOMER_CODE END) CAMPANA_CLIENTES

                    --GRUPO TRATADO
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GT' THEN VENTA END) GT_CAMPANA_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GT' THEN UNIDADES END) GT_CAMPANA_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GT' THEN INVOICE_NO END) GT_CAMPANA_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GT' THEN CUSTOMER_CODE END) GT_CAMPANA_CLIENTES

                    --GRUPO CONTROL
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GC' THEN VENTA END) GC_CAMPANA_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GC' THEN UNIDADES END) GC_CAMPANA_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GC' THEN INVOICE_NO END) GC_CAMPANA_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GC' THEN CUSTOMER_CODE END) GC_CAMPANA_CLIENTES

                    --REDENCIONES
                    ,COUNT(DISTINCT CASE WHEN PROMOCIONES_APLICADAS > 0 THEN CUSTOMER_CODE END) RED_CLIENTES
                    ,SUM(CASE WHEN PROMOCIONES_APLICADAS > 0 THEN VENTA END) RED_VENTA_PROMOCIONADA
                    ,SUM(UNIDADES_PROMOCIONADAS) RED_UNIDADES_PROMOCIONADAS
                    ,SUM(COSTO_PROMOCIONADO) RED_COSTO_PROMOCIONADO
                    ,SUM(PROMOCIONES_APLICADAS) RED_PROMOCIONES_APLICADAS

                    --REDENCIONES ABIERTOS
                    ,COUNT(DISTINCT CASE WHEN IND_ABIERTOS = 1 AND PROMOCIONES_APLICADAS > 0 THEN CUSTOMER_CODE END) RED_ABIERTOS_CLIENTES
                    ,SUM(CASE WHEN IND_ABIERTOS = 1 AND PROMOCIONES_APLICADAS > 0 THEN VENTA END) RED_ABIERTOS_VENTA_PROMOCIONADA
                    ,SUM(CASE WHEN IND_ABIERTOS = 1 THEN UNIDADES_PROMOCIONADAS END) RED_ABIERTOS_UNIDADES_PROMOCIONADAS
                    ,SUM(CASE WHEN IND_ABIERTOS = 1 THEN COSTO_PROMOCIONADO END) RED_ABIERTOS_COSTO_PROMOCIONADO
                    ,SUM(CASE WHEN IND_ABIERTOS = 1 THEN PROMOCIONES_APLICADAS END) RED_ABIERTOS_PROMOCIONES_APLICADAS

                    --REDENCIONES ABIERTOS CANALES
                    ,COUNT(DISTINCT CASE WHEN IND_ABIERTOS_WA = 1 AND PROMOCIONES_APLICADAS > 0 THEN CUSTOMER_CODE END) RED_ABIERTOS_WA_CLIENTES
                    ,COUNT(DISTINCT CASE WHEN IND_ABIERTOS_SMS = 1 AND PROMOCIONES_APLICADAS > 0 THEN CUSTOMER_CODE END) RED_ABIERTOS_SMS_CLIENTES
                    ,COUNT(DISTINCT CASE WHEN IND_ABIERTOS_MAIL = 1 AND PROMOCIONES_APLICADAS > 0 THEN CUSTOMER_CODE END) RED_ABIERTOS_MAIL_CLIENTES
                    ,COUNT(DISTINCT CASE WHEN IND_ABIERTOS_CUPON = 1 AND PROMOCIONES_APLICADAS > 0 THEN CUSTOMER_CODE END) RED_ABIERTOS_CUPON_CLIENTES --NUEVO

                    --ABIERTOS
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 THEN VENTA END) ABIERTOS_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 THEN UNIDADES END) ABIERTOS_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 THEN INVOICE_NO END) ABIERTOS_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 THEN CUSTOMER_CODE END) ABIERTOS_CLIENTES
                    
                    --ABIERTOS WA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_WA = 1 THEN VENTA END) ABIERTOS_WA_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_WA = 1 THEN UNIDADES END) ABIERTOS_WA_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_WA = 1 THEN INVOICE_NO END) ABIERTOS_WA_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_WA = 1 THEN CUSTOMER_CODE END) ABIERTOS_WA_CLIENTES

                    --ABIERTOS SMS
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_SMS = 1 THEN VENTA END) ABIERTOS_SMS_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_SMS = 1 THEN UNIDADES END) ABIERTOS_SMS_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_SMS = 1 THEN INVOICE_NO END) ABIERTOS_SMS_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_SMS = 1 THEN CUSTOMER_CODE END) ABIERTOS_SMS_CLIENTES
                    
                    --ABIERTOS MAIL
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_MAIL = 1 THEN VENTA END) ABIERTOS_MAIL_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_MAIL = 1 THEN UNIDADES END) ABIERTOS_MAIL_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_MAIL = 1 THEN INVOICE_NO END) ABIERTOS_MAIL_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_MAIL = 1 THEN CUSTOMER_CODE END) ABIERTOS_MAIL_CLIENTES
                    
                    --ABIERTOS CUPON
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_CUPON = 1 THEN VENTA END) ABIERTOS_CUPON_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_CUPON = 1 THEN UNIDADES END) ABIERTOS_CUPON_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_CUPON = 1 THEN INVOICE_NO END) ABIERTOS_CUPON_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS_CUPON = 1 THEN CUSTOMER_CODE END) ABIERTOS_CUPON_CLIENTES

                    -- ABIERTOS SEGMENTOS
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_NUEVO = 1 THEN VENTA END) ABIERTOS_NUEVOS_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_NUEVO = 1 THEN UNIDADES END) ABIERTOS_NUEVOS_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_NUEVO = 1 THEN INVOICE_NO END) ABIERTOS_NUEVOS_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_NUEVO = 1 THEN CUSTOMER_CODE END) ABIERTOS_NUEVOS_CLIENTES

                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_FID = 1 THEN VENTA END) ABIERTOS_FID_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_FID = 1 THEN UNIDADES END) ABIERTOS_FID_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_FID = 1 THEN INVOICE_NO END) ABIERTOS_FID_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_FID = 1 THEN CUSTOMER_CODE END) ABIERTOS_FID_CLIENTES
                    
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_REC = 1 THEN VENTA END) ABIERTOS_REC_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_REC = 1 THEN UNIDADES END) ABIERTOS_REC_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_REC = 1 THEN INVOICE_NO END) ABIERTOS_REC_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_REC = 1 THEN CUSTOMER_CODE END) ABIERTOS_REC_CLIENTES

                    --SEGMENTOS
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_NUEVO = 1 THEN VENTA END) NUEVOS_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_NUEVO = 1 THEN UNIDADES END) NUEVOS_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_NUEVO = 1 THEN INVOICE_NO END) NUEVOS_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_NUEVO = 1 THEN CUSTOMER_CODE END) NUEVOS_CLIENTES

                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_FID = 1 THEN VENTA END) FID_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_FID = 1 THEN UNIDADES END) FID_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_FID = 1 THEN INVOICE_NO END) FID_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_FID = 1 THEN CUSTOMER_CODE END) FID_CLIENTES
                    
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_REC = 1 THEN VENTA END) REC_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_REC = 1 THEN UNIDADES END) REC_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_REC = 1 THEN INVOICE_NO END) REC_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_REC = 1 THEN CUSTOMER_CODE END) REC_CLIENTES

                    --SEGMENTOS AÑO_ANTERIOR
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_NUEVO_ANO_ANTERIOR = 1 THEN VENTA END) NUEVOS_VENTA_ANO_ANTERIOR
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_NUEVO_ANO_ANTERIOR = 1 THEN UNIDADES END) NUEVOS_UNIDADES_ANO_ANTERIOR
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_NUEVO_ANO_ANTERIOR = 1 THEN INVOICE_NO END) NUEVOS_TX_ANO_ANTERIOR
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_NUEVO_ANO_ANTERIOR = 1 THEN CUSTOMER_CODE END) NUEVOS_CLIENTES_ANO_ANTERIOR

                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_FID_ANO_ANTERIOR = 1 THEN VENTA END) FID_VENTA_ANO_ANTERIOR
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_FID_ANO_ANTERIOR = 1 THEN UNIDADES END) FID_UNIDADES_ANO_ANTERIOR
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_FID_ANO_ANTERIOR = 1 THEN INVOICE_NO END) FID_TX_ANO_ANTERIOR
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_FID_ANO_ANTERIOR = 1 THEN CUSTOMER_CODE END) FID_CLIENTES_ANO_ANTERIOR
                    
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_REC_ANO_ANTERIOR = 1 THEN VENTA END) REC_VENTA_ANO_ANTERIOR
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_REC_ANO_ANTERIOR = 1 THEN UNIDADES END) REC_UNIDADES_ANO_ANTERIOR
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_REC_ANO_ANTERIOR = 1 THEN INVOICE_NO END) REC_TX_ANO_ANTERIOR
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_REC_ANO_ANTERIOR = 1 THEN CUSTOMER_CODE END) REC_CLIENTES_ANO_ANTERIOR

                    --SEGMENTOS CON TX_ELEGIBLES
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_NUEVO = 1 THEN VENTA END) ELEGIBLE_NUEVOS_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_NUEVO = 1 THEN UNIDADES END) ELEGIBLE_NUEVOS_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_NUEVO = 1 THEN INVOICE_NO END) ELEGIBLE_NUEVOS_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_NUEVO = 1 THEN CUSTOMER_CODE END) ELEGIBLE_NUEVOS_CLIENTES

                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_FID = 1 THEN VENTA END) ELEGIBLE_FID_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_FID = 1 THEN UNIDADES END) ELEGIBLE_FID_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_FID = 1 THEN INVOICE_NO END) ELEGIBLE_FID_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_FID = 1 THEN CUSTOMER_CODE END) ELEGIBLE_FID_CLIENTES
                    
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_REC = 1 THEN VENTA END) ELEGIBLE_REC_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_REC = 1 THEN UNIDADES END) ELEGIBLE_REC_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_REC = 1 THEN INVOICE_NO END) ELEGIBLE_REC_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_REC = 1 THEN CUSTOMER_CODE END) ELEGIBLE_REC_CLIENTES

                    --SEGMENTOS CON TX_ELEGIBLES AÑO_ANTERIOR
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_NUEVO_ANO_ANTERIOR = 1 THEN VENTA END) ELEGIBLE_NUEVOS_VENTA_ANO_ANTERIOR
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_NUEVO_ANO_ANTERIOR = 1 THEN UNIDADES END) ELEGIBLE_NUEVOS_UNIDADES_ANO_ANTERIOR
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_NUEVO_ANO_ANTERIOR = 1 THEN INVOICE_NO END) ELEGIBLE_NUEVOS_TX_ANO_ANTERIOR
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_NUEVO_ANO_ANTERIOR = 1 THEN CUSTOMER_CODE END) ELEGIBLE_NUEVOS_CLIENTES_ANO_ANTERIOR

                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_FID_ANO_ANTERIOR = 1 THEN VENTA END) ELEGIBLE_FID_VENTA_ANO_ANTERIOR
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_FID_ANO_ANTERIOR = 1 THEN UNIDADES END) ELEGIBLE_FID_UNIDADES_ANO_ANTERIOR
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_FID_ANO_ANTERIOR = 1 THEN INVOICE_NO END) ELEGIBLE_FID_TX_ANO_ANTERIOR
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_FID_ANO_ANTERIOR = 1 THEN CUSTOMER_CODE END) ELEGIBLE_FID_CLIENTES_ANO_ANTERIOR
                    
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_REC_ANO_ANTERIOR = 1 THEN VENTA END) ELEGIBLE_REC_VENTA_ANO_ANTERIOR
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_REC_ANO_ANTERIOR = 1 THEN UNIDADES END) ELEGIBLE_REC_UNIDADES_ANO_ANTERIOR
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_REC_ANO_ANTERIOR = 1 THEN INVOICE_NO END) ELEGIBLE_REC_TX_ANO_ANTERIOR
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 AND IND_CLIENTE_REC_ANO_ANTERIOR = 1 THEN CUSTOMER_CODE END) ELEGIBLE_REC_CLIENTES_ANO_ANTERIOR

                    --ONLINE
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ONLINE = 1 THEN VENTA END) CAMPANA_ONLINE_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ONLINE = 1 THEN UNIDADES END) CAMPANA_ONLINE_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ONLINE = 1 THEN INVOICE_NO END) CAMPANA_ONLINE_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ONLINE = 1 THEN CUSTOMER_CODE END) CAMPANA_ONLINE_CLIENTES
                    
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND IND_ONLINE = 1 THEN VENTA END) MES_ANTERIOR_ONLINE_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ONLINE = 1 THEN VENTA END) ANO_ANTERIOR_ONLINE_VENTA

                    --TX ELEGIBLES
                    --ACTUAL
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 THEN VENTA END) CAMPANA_VENTA_ELEGIBLE
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 THEN UNIDADES END) CAMPANA_UNIDADES_ELEGIBLE
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) CAMPANA_TX_ELEGIBLE
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 THEN CUSTOMER_CODE END) CAMPANA_CLIENTES_ELEGIBLE
                    --AÑO ANTERIOR
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN VENTA END) ANO_ANTERIOR_VENTA_ELEGIBLE
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN UNIDADES END) ANO_ANTERIOR_UNIDADES_ELEGIBLE
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) ANO_ANTERIOR_TX_ELEGIBLE
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN CUSTOMER_CODE END) ANO_ANTERIOR_CLIENTES_ELEGIBLE

                    --REGISTROS Y REGISTROS ELEGIBLES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_REGISTRO = 1 THEN INVOICE_NO END) CAMPANA_TX_REGISTRO
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_REGISTRO_ELEGIBLE = 1 THEN INVOICE_NO END) CAMPANA_TX_REGISTRO_ELEGIBLE

                    --MES Y AÑO ANTERIOR
                    --CAMPANA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' THEN VENTA END) MES_ANTERIOR_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' THEN UNIDADES END) MES_ANTERIOR_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' THEN INVOICE_NO END) MES_ANTERIOR_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' THEN CUSTOMER_CODE END) MES_ANTERIOR_CLIENTES

                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' THEN VENTA END) ANO_ANTERIOR_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' THEN UNIDADES END) ANO_ANTERIOR_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' THEN INVOICE_NO END) ANO_ANTERIOR_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' THEN CUSTOMER_CODE END) ANO_ANTERIOR_CLIENTES

                    --GRUPO TRATADO
                    --MES ANTERIOR
                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GT' THEN VENTA END) GT_INCREMENTO_MES_ANTERIOR_CAMPANA_VENTA
                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GT' THEN UNIDADES END) GT_INCREMENTO_MES_ANTERIOR_CAMPANA_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GT' THEN INVOICE_NO END) GT_INCREMENTO_MES_ANTERIOR_CAMPANA_TX
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GT' THEN CUSTOMER_CODE END) GT_INCREMENTO_MES_ANTERIOR_CAMPANA_CLIENTES

                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND GRUPO = 'GT' THEN VENTA END) GT_INCREMENTO_MES_ANTERIOR_VENTA
                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND GRUPO = 'GT' THEN UNIDADES END) GT_INCREMENTO_MES_ANTERIOR_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND GRUPO = 'GT' THEN INVOICE_NO END) GT_INCREMENTO_MES_ANTERIOR_TX
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND GRUPO = 'GT' THEN CUSTOMER_CODE END) GT_INCREMENTO_MES_ANTERIOR_CLIENTES

                    --ANO_ANTERIOR
                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GT' THEN VENTA END) GT_INCREMENTO_ANO_ANTERIOR_CAMPANA_VENTA
                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GT' THEN UNIDADES END) GT_INCREMENTO_ANO_ANTERIOR_CAMPANA_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GT' THEN INVOICE_NO END) GT_INCREMENTO_ANO_ANTERIOR_CAMPANA_TX
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GT' THEN CUSTOMER_CODE END) GT_INCREMENTO_ANO_ANTERIOR_CAMPANA_CLIENTES

                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND GRUPO = 'GT' THEN VENTA END) GT_INCREMENTO_ANO_ANTERIOR_VENTA
                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND GRUPO = 'GT' THEN UNIDADES END) GT_INCREMENTO_ANO_ANTERIOR_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND GRUPO = 'GT' THEN INVOICE_NO END) GT_INCREMENTO_ANO_ANTERIOR_TX
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND GRUPO = 'GT' THEN CUSTOMER_CODE END) GT_INCREMENTO_ANO_ANTERIOR_CLIENTES

                    --GRUPO CONTROL
                    --MES ANTERIOR
                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GC' THEN VENTA END) GC_INCREMENTO_MES_ANTERIOR_CAMPANA_VENTA
                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GC' THEN UNIDADES END) GC_INCREMENTO_MES_ANTERIOR_CAMPANA_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GC' THEN INVOICE_NO END) GC_INCREMENTO_MES_ANTERIOR_CAMPANA_TX
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GC' THEN CUSTOMER_CODE END) GC_INCREMENTO_MES_ANTERIOR_CAMPANA_CLIENTES

                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND GRUPO = 'GC' THEN VENTA END) GC_INCREMENTO_MES_ANTERIOR_VENTA
                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND GRUPO = 'GC' THEN UNIDADES END) GC_INCREMENTO_MES_ANTERIOR_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND GRUPO = 'GC' THEN INVOICE_NO END) GC_INCREMENTO_MES_ANTERIOR_TX
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND GRUPO = 'GC' THEN CUSTOMER_CODE END) GC_INCREMENTO_MES_ANTERIOR_CLIENTES

                    --ANO_ANTERIOR
                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GC' THEN VENTA END) GC_INCREMENTO_ANO_ANTERIOR_CAMPANA_VENTA
                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GC' THEN UNIDADES END) GC_INCREMENTO_ANO_ANTERIOR_CAMPANA_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GC' THEN INVOICE_NO END) GC_INCREMENTO_ANO_ANTERIOR_CAMPANA_TX
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND GRUPO = 'GC' THEN CUSTOMER_CODE END) GC_INCREMENTO_ANO_ANTERIOR_CAMPANA_CLIENTES

                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND GRUPO = 'GC' THEN VENTA END) GC_INCREMENTO_ANO_ANTERIOR_VENTA
                    ,SUM(CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND GRUPO = 'GC' THEN UNIDADES END) GC_INCREMENTO_ANO_ANTERIOR_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND GRUPO = 'GC' THEN INVOICE_NO END) GC_INCREMENTO_ANO_ANTERIOR_TX
                    ,COUNT(DISTINCT CASE WHEN IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 AND IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND GRUPO = 'GC' THEN CUSTOMER_CODE END) GC_INCREMENTO_ANO_ANTERIOR_CLIENTES

                    --INCREMENTOS EN ABIERTOS. CLIENTES CON COMPRA EN CAMPAÑA Y EN UN A.PERIODO ANTERIOR. PARA COMPROBAR INCREMENTO EN VENTA EN EL A.PERIODO CAMPAÑA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN VENTA END) INCREMENTO_MES_ANTERIOR_CAMPANA_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN UNIDADES END) INCREMENTO_MES_ANTERIOR_CAMPANA_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN INVOICE_NO END) INCREMENTO_MES_ANTERIOR_CAMPANA_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN CUSTOMER_CODE END) INCREMENTO_MES_ANTERIOR_CAMPANA_CLIENTES

                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN VENTA END) INCREMENTO_MES_ANTERIOR_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN UNIDADES END) INCREMENTO_MES_ANTERIOR_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN INVOICE_NO END) INCREMENTO_MES_ANTERIOR_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN CUSTOMER_CODE END) INCREMENTO_MES_ANTERIOR_CLIENTES

                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN VENTA END) INCREMENTO_ANO_ANTERIOR_CAMPANA_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN UNIDADES END) INCREMENTO_ANO_ANTERIOR_CAMPANA_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN INVOICE_NO END) INCREMENTO_ANO_ANTERIOR_CAMPANA_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN CUSTOMER_CODE END) INCREMENTO_ANO_ANTERIOR_CAMPANA_CLIENTES

                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN VENTA END) INCREMENTO_ANO_ANTERIOR_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN UNIDADES END) INCREMENTO_ANO_ANTERIOR_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN INVOICE_NO END) INCREMENTO_ANO_ANTERIOR_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_ABIERTOS = 1 AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN CUSTOMER_CODE END) INCREMENTO_ANO_ANTERIOR_CLIENTES

                    --INCREMENTOS TOTALES. CLIENTES CON COMPRA EN CAMPAÑA Y EN UN A.PERIODO ANTERIOR. PARA COMPROBAR INCREMENTO EN VENTA EN EL A.PERIODO CAMPAÑA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN VENTA END) INCREMENTO_TOTAL_MES_ANTERIOR_CAMPANA_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN UNIDADES END) INCREMENTO_TOTAL_MES_ANTERIOR_CAMPANA_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN INVOICE_NO END) INCREMENTO_TOTAL_MES_ANTERIOR_CAMPANA_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN CUSTOMER_CODE END) INCREMENTO_TOTAL_MES_ANTERIOR_CAMPANA_CLIENTES

                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN VENTA END) INCREMENTO_TOTAL_MES_ANTERIOR_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN UNIDADES END) INCREMENTO_TOTAL_MES_ANTERIOR_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN INVOICE_NO END) INCREMENTO_TOTAL_MES_ANTERIOR_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'MES_ANTERIOR' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_MES_ANTERIOR = 1 THEN CUSTOMER_CODE END) INCREMENTO_TOTAL_MES_ANTERIOR_CLIENTES

                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN VENTA END) INCREMENTO_TOTAL_ANO_ANTERIOR_CAMPANA_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN UNIDADES END) INCREMENTO_TOTAL_ANO_ANTERIOR_CAMPANA_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN INVOICE_NO END) INCREMENTO_TOTAL_ANO_ANTERIOR_CAMPANA_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'CAMPANA' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN CUSTOMER_CODE END) INCREMENTO_TOTAL_ANO_ANTERIOR_CAMPANA_CLIENTES

                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN VENTA END) INCREMENTO_TOTAL_ANO_ANTERIOR_VENTA
                    ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN UNIDADES END) INCREMENTO_TOTAL_ANO_ANTERIOR_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN INVOICE_NO END) INCREMENTO_TOTAL_ANO_ANTERIOR_TX
                    ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO = 'ANO_ANTERIOR' AND IND_CLIENTE_CAMPANA = 1 AND IND_CLIENTE_ANO_ANTERIOR = 1 THEN CUSTOMER_CODE END) INCREMENTO_TOTAL_ANO_ANTERIOR_CLIENTES

                    --CATEGORIA
                    ,SUM(CASE WHEN A.PERIODO = 'CAMPANA' THEN VENTA END) CAT_CAMPANA_VENTA
                    ,SUM(CASE WHEN A.PERIODO = 'CAMPANA' THEN UNIDADES END) CAT_CAMPANA_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'CAMPANA' THEN INVOICE_NO END) CAT_CAMPANA_TX
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'CAMPANA' THEN CUSTOMER_CODE END) CAT_CAMPANA_CLIENTES
                    
                    ,SUM(CASE WHEN A.PERIODO = 'CAMPANA' AND IND_ONLINE = 1 THEN VENTA END) CAT_CAMPANA_ONLINE_VENTA
                    ,SUM(CASE WHEN A.PERIODO = 'CAMPANA' AND IND_ONLINE = 1 THEN UNIDADES END) CAT_CAMPANA_ONLINE_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'CAMPANA' AND IND_ONLINE = 1 THEN INVOICE_NO END) CAT_CAMPANA_ONLINE_TX
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'CAMPANA' AND IND_ONLINE = 1 THEN CUSTOMER_CODE END) CAT_CAMPANA_ONLINE_CLIENTES

                    ,SUM(CASE WHEN A.PERIODO = 'MES_ANTERIOR' THEN VENTA END) CAT_MES_ANTERIOR_VENTA
                    ,SUM(CASE WHEN A.PERIODO = 'MES_ANTERIOR' THEN UNIDADES END) CAT_MES_ANTERIOR_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'MES_ANTERIOR' THEN INVOICE_NO END) CAT_MES_ANTERIOR_TX
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'MES_ANTERIOR' THEN CUSTOMER_CODE END) CAT_MES_ANTERIOR_CLIENTES

                    ,SUM(CASE WHEN A.PERIODO = 'ANO_ANTERIOR' THEN VENTA END) CAT_ANO_ANTERIOR_VENTA
                    ,SUM(CASE WHEN A.PERIODO = 'ANO_ANTERIOR' THEN UNIDADES END) CAT_ANO_ANTERIOR_UNIDADES
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'ANO_ANTERIOR' THEN INVOICE_NO END) CAT_ANO_ANTERIOR_TX
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'ANO_ANTERIOR' THEN CUSTOMER_CODE END) CAT_ANO_ANTERIOR_CLIENTES

                    --CATEGORIA ELEGIBLE
                    ,SUM(CASE WHEN A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 THEN VENTA END) CAT_CAMPANA_VENTA_ELEGIBLE
                    ,SUM(CASE WHEN A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 THEN UNIDADES END) CAT_CAMPANA_UNIDADES_ELEGIBLE
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) CAT_CAMPANA_TX_ELEGIBLE
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'CAMPANA' AND IND_ELEGIBLE = 1 THEN CUSTOMER_CODE END) CAT_CAMPANA_CLIENTES_ELEGIBLE
                    
                    ,SUM(CASE WHEN A.PERIODO = 'CAMPANA' AND IND_ONLINE = 1 AND IND_ELEGIBLE = 1 THEN VENTA END) CAT_CAMPANA_ONLINE_VENTA_ELEGIBLE
                    ,SUM(CASE WHEN A.PERIODO = 'CAMPANA' AND IND_ONLINE = 1 AND IND_ELEGIBLE = 1 THEN UNIDADES END) CAT_CAMPANA_ONLINE_UNIDADES_ELEGIBLE
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'CAMPANA' AND IND_ONLINE = 1 AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) CAT_CAMPANA_ONLINE_TX_ELEGIBLE
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'CAMPANA' AND IND_ONLINE = 1 AND IND_ELEGIBLE = 1 THEN CUSTOMER_CODE END) CAT_CAMPANA_ONLINE_CLIENTES_ELEGIBLE

                    ,SUM(CASE WHEN A.PERIODO = 'MES_ANTERIOR' AND IND_ELEGIBLE = 1 THEN VENTA END) CAT_MES_ANTERIOR_VENTA_ELEGIBLE
                    ,SUM(CASE WHEN A.PERIODO = 'MES_ANTERIOR' AND IND_ELEGIBLE = 1 THEN UNIDADES END) CAT_MES_ANTERIOR_UNIDADES_ELEGIBLE
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'MES_ANTERIOR' AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) CAT_MES_ANTERIOR_TX_ELEGIBLE
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'MES_ANTERIOR' AND IND_ELEGIBLE = 1 THEN CUSTOMER_CODE END) CAT_MES_ANTERIOR_CLIENTES_ELEGIBLE

                    ,SUM(CASE WHEN A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN VENTA END) CAT_ANO_ANTERIOR_VENTA_ELEGIBLE
                    ,SUM(CASE WHEN A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN UNIDADES END) CAT_ANO_ANTERIOR_UNIDADES_ELEGIBLE
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) CAT_ANO_ANTERIOR_TX_ELEGIBLE
                    ,COUNT(DISTINCT CASE WHEN A.PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN CUSTOMER_CODE END) CAT_ANO_ANTERIOR_CLIENTES_ELEGIBLE
        '''

        query_res_global = f'''
            --AGRUPADOS DE VENTA
            DROP TABLE IF EXISTS #GLOBAL;
            CREATE TABLE #GLOBAL AS (
                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'PROVEEDOR' TABLA
                    ,'GLOBAL' PROVEEDOR
                    ,'TOTAL' MARCA
                    ,'TOTAL' REGION
                    ,'TOTAL' FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'TOTAL' CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,'TOTAL' "WEEK"
                    
                    {query_res_agg}

                FROM CHEDRAUI.MON_VTA A
                WHERE PERIODO IN ('CAMPANA', 'ANO_ANTERIOR', 'MES_ANTERIOR')
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT * FROM #GLOBAL;
        '''

        query_res_proveedor = f'''
            DROP TABLE IF EXISTS #PROVEEDOR;
            CREATE TABLE #PROVEEDOR AS (
                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'PROVEEDOR' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,'TOTAL' REGION
                    ,'TOTAL' FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'TOTAL' CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,'TOTAL' "WEEK"
                    
                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT TOP 10 * FROM #PROVEEDOR;
        '''

        query_res_region = f'''
            DROP TABLE IF EXISTS #REGION;
            CREATE TABLE #REGION AS (
                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'REGION' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,REGION
                    ,'TOTAL' FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'TOTAL' CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,'TOTAL' "WEEK"
                    
                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT TOP 10 * FROM #REGION;
        '''

        query_res_formato = f'''
            DROP TABLE IF EXISTS #FORMATO;
            CREATE TABLE #FORMATO AS (
                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'FORMATO' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,'TOTAL' REGION
                    ,FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'TOTAL' CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,'TOTAL' "WEEK"
                    
                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT TOP 10 * FROM #FORMATO;
        '''

        query_res_tienda = f'''
            DROP TABLE IF EXISTS #TIENDA;
            CREATE TABLE #TIENDA AS (
                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'TIENDA' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,REGION
                    ,FORMATO_TIENDA
                    ,STORE_DESCRIPTION TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'TOTAL' CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,'TOTAL' "WEEK"
                    
                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT TOP 10 * FROM #TIENDA;
            '''

        query_res_tienda_semanal = f'''
            DROP TABLE IF EXISTS #TIENDA_SEMANAL;
            CREATE TABLE #TIENDA_SEMANAL AS (
                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'TIENDA_SEMANAL' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,REGION
                    ,FORMATO_TIENDA
                    ,STORE_DESCRIPTION TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'TOTAL' CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,C."WEEK"
                    
                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                LEFT JOIN #DIAS B USING(INVOICE_DATE)
                LEFT JOIN #DIAS C ON B.DAY_NUMBER = C.DAY_NUMBER AND C.PERIODO = 'CAMPANA'
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT TOP 10 * FROM #TIENDA_SEMANAL;
            '''
        
        query_res_tienda_semanal_total = f'''
            DROP TABLE IF EXISTS #TIENDA_SEMANAL_TOTAL;
            CREATE TABLE #TIENDA_SEMANAL_TOTAL AS (
                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'TIENDA_SEMANAL' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,REGION
                    ,FORMATO_TIENDA
                    ,STORE_DESCRIPTION TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'TOTAL' CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,'TOTAL' "WEEK"
                    
                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                LEFT JOIN #DIAS B USING(INVOICE_DATE)
                LEFT JOIN #DIAS C ON B.DAY_NUMBER = C.DAY_NUMBER AND C.PERIODO = 'CAMPANA'
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT TOP 10 * FROM #TIENDA_SEMANAL_TOTAL;
            '''
        
        query_res_tipo_familia = f'''
            DROP TABLE IF EXISTS #FAMILIA;
            CREATE TABLE #FAMILIA AS (
                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'FAMILIA' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,'TOTAL' REGION
                    ,'TOTAL' FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'TOTAL' CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,'TOTAL' "WEEK"
                    
                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT TOP 10 * FROM #FAMILIA;
            '''
        
        query_res_nse = f'''
            DROP TABLE IF EXISTS #NSE;
            CREATE TABLE #NSE AS (
                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'NSE' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,'TOTAL' REGION
                    ,'TOTAL' FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,NSE
                    ,'TOTAL' CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,'TOTAL' "WEEK"
                    
                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT TOP 10 * FROM #NSE;
            '''
        
        query_res_clase = f'''
            DROP TABLE IF EXISTS #CLASE;
            CREATE TABLE #CLASE AS (
                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'CLASE' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,'TOTAL' REGION
                    ,'TOTAL' FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,'TOTAL' "WEEK"
                    
                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT TOP 10 * FROM #CLASE;
            '''
        
        query_res_subclase = f'''
            DROP TABLE IF EXISTS #SUBCLASE;
            CREATE TABLE #SUBCLASE AS (
            WITH __SUBCLASS_TOP5 AS (
                SELECT DISTINCT
                PROVEEDOR
                ,SUBCLASS_DESC
            --     ,VENTA
                FROM (
                SELECT
                    PROVEEDOR
                    ,SUBCLASS_DESC
                    ,SUM(VENTA) VENTA
                ,ROW_NUMBER() OVER (PARTITION BY PROVEEDOR ORDER BY SUM(VENTA) DESC) ROW_N
                FROM CHEDRAUI.MON_VTA
                WHERE PERIODO = 'CAMPANA'
                AND IND_MARCA = 1
                AND IND_MC = 1
                GROUP BY 1,2
                ) A
                WHERE A.ROW_N <= 10
            )

            -- SELECT * FROM __SUBCLASS_TOP5;

                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'SUBCLASE' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,'TOTAL' REGION
                    ,'TOTAL' FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'TOTAL' CLASS_DESC
                    ,SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,'TOTAL' "WEEK"
                    
                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                INNER JOIN (SELECT * FROM __SUBCLASS_TOP5) USING(SUBCLASS_DESC, PROVEEDOR)
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT TOP 10 * FROM #SUBCLASE;
            '''
        
        query_res_producto = f'''
            DROP TABLE IF EXISTS #PRODUCTO;
            CREATE TABLE #PRODUCTO AS (
            WITH __PRODUCTO_TOP10 AS (
                SELECT DISTINCT
            --     PROVEEDOR
                PRODUCT_CODE
            --     ,VENTA
                FROM (
                SELECT
                    PROVEEDOR
                    ,PRODUCT_CODE
                    ,SUM(VENTA) VENTA
                ,ROW_NUMBER() OVER (PARTITION BY PROVEEDOR ORDER BY SUM(VENTA) DESC) ROW_N
                FROM CHEDRAUI.MON_VTA
                WHERE PERIODO = 'CAMPANA'
                AND IND_MARCA = 1
                AND IND_MC = 1
                GROUP BY 1, 2
                ) A
                WHERE A.ROW_N <= 20
            )

            -- SELECT * FROM __PRODUCTO_TOP5;

                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'PRODUCTO' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,'TOTAL' REGION
                    ,'TOTAL' FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'TOTAL' CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,'TOTAL' "WEEK"
                    
                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                INNER JOIN (SELECT DISTINCT PRODUCT_CODE FROM __PRODUCTO_TOP10) USING(PRODUCT_CODE)
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );
            '''
        
        query_res_venta_diaria = f'''
            --VENTA_DIARIA
            DROP TABLE IF EXISTS #VENTA_DIARIA;
            CREATE TABLE #VENTA_DIARIA AS (
                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'VENTA_DIARIA' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,'TOTAL' REGION
                    ,'TOTAL' FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'TOTAL' CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,C.INVOICE_DATE::TEXT "DATE"
                    ,C.WEEK

                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                LEFT JOIN #DIAS B USING(INVOICE_DATE)
                LEFT JOIN #DIAS C ON B.DAY_NUMBER = C.DAY_NUMBER AND C.PERIODO = 'CAMPANA'
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT * FROM #VENTA_DIARIA;
            '''
        
        query_res_venta_semanal = f'''
            --VENTA_SEMANAL
            DROP TABLE IF EXISTS #VENTA_SEMANAL;
            CREATE TABLE #VENTA_SEMANAL AS (
                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'VENTA_SEMANAL' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,'TOTAL' REGION
                    ,'TOTAL' FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'TOTAL' CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,C.WEEK

                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                LEFT JOIN #DIAS B USING(INVOICE_DATE)
                LEFT JOIN #DIAS C ON B.DAY_NUMBER = C.DAY_NUMBER AND C.PERIODO = 'CAMPANA'
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT * FROM #VENTA_SEMANAL;
            '''
        
        query_res_venta_semanal_total = f'''
            --VENTA_SEMANAL_TOTAL
            DROP TABLE IF EXISTS #VENTA_SEMANAL_TOTAL;
            CREATE TABLE #VENTA_SEMANAL_TOTAL AS (
            SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'VENTA_SEMANAL' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,'TOTAL' REGION
                    ,'TOTAL' FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,'TOTAL' CLASS_DESC
                    ,'TOTAL' SUBCLASS_DESC
                    ,'TOTAL' PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,'TOTAL' "WEEK"

                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                LEFT JOIN #DIAS B USING(INVOICE_DATE)
                LEFT JOIN #DIAS C ON B.DAY_NUMBER = C.DAY_NUMBER AND C.PERIODO = 'CAMPANA'
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT * FROM #VENTA_SEMANAL_TOTAL;
            '''
        
        query_res_venta_semanal_producto = f'''
            --VENTA_SEMANAL
            DROP TABLE IF EXISTS #VENTA_SEMANAL_PRODUCTO;
            CREATE TABLE #VENTA_SEMANAL_PRODUCTO AS (
            WITH __PRODUCTO_TOP10 AS (
                SELECT DISTINCT
            --     PROVEEDOR
                PRODUCT_CODE
            --     ,VENTA
                FROM (
                SELECT
                    PROVEEDOR
                    ,PRODUCT_CODE
                    ,SUM(VENTA) VENTA
                ,ROW_NUMBER() OVER (PARTITION BY PROVEEDOR ORDER BY SUM(VENTA) DESC) ROW_N
                FROM CHEDRAUI.MON_VTA
                WHERE PERIODO = 'CAMPANA'
                AND IND_MARCA = 1
                AND IND_MC = 1
                GROUP BY 1, 2
                ) A
                WHERE A.ROW_N <= 20
            )

            -- SELECT * FROM __PRODUCTO_TOP5;

                SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'VENTA_SEMANAL_CLASS' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,'TOTAL' REGION
                    ,'TOTAL' FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,CLASS_DESC
                    ,SUBCLASS_DESC
                    ,PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,C.WEEK

                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                INNER JOIN (SELECT DISTINCT PRODUCT_CODE FROM __PRODUCTO_TOP10) USING(PRODUCT_CODE)
                LEFT JOIN #DIAS B USING(INVOICE_DATE)
                LEFT JOIN #DIAS C ON B.DAY_NUMBER = C.DAY_NUMBER AND C.PERIODO = 'CAMPANA'
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT * FROM #VENTA_SEMANAL_PRODUCTO;
            '''
        
        query_res_venta_semanal_total_producto = f'''
            --VENTA_SEMANAL_TOTAL
            DROP TABLE IF EXISTS #VENTA_SEMANAL_TOTAL_PRODUCTO;
            CREATE TABLE #VENTA_SEMANAL_TOTAL_PRODUCTO AS (
            SELECT
                    --VARIABLES DE TABLA
                    '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA

                    --VARIABLES DE VENTA
                    ,IND_MC
                    ,'VENTA_SEMANAL_CLASS' TABLA
                    ,PROVEEDOR
                    ,'TOTAL' MARCA
                    ,'TOTAL' REGION
                    ,'TOTAL' FORMATO_TIENDA
                    ,'TOTAL' TIENDA
                    ,'TOTAL' TIPO_FAMILIA
                    ,'TOTAL' NSE
                    ,CLASS_DESC
                    ,SUBCLASS_DESC
                    ,PRODUCT_DESCRIPTION
                    ,'TOTAL' "DATE"
                    ,'TOTAL' "WEEK"

                    {query_res_agg}
                        
                FROM CHEDRAUI.MON_VTA A
                LEFT JOIN #DIAS B USING(INVOICE_DATE)
                LEFT JOIN #DIAS C ON B.DAY_NUMBER = C.DAY_NUMBER AND C.PERIODO = 'CAMPANA'
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
            );

            -- SELECT * FROM #VENTA_SEMANAL_TOTAL_PRODUCTO;
            '''
        
        query_res = f'''
            DROP TABLE IF EXISTS #CAMP_RESULTS;
            CREATE TABLE #CAMP_RESULTS AS (
                SELECT * FROM #GLOBAL
                UNION
                SELECT * FROM #PROVEEDOR
                UNION
                SELECT * FROM #REGION
                UNION
                SELECT * FROM #FORMATO
                UNION
                SELECT * FROM #TIENDA
                UNION
                SELECT * FROM #TIENDA_SEMANAL
                UNION
                SELECT * FROM #TIENDA_SEMANAL_TOTAL
                UNION
                SELECT * FROM #FAMILIA
                UNION
                SELECT * FROM #NSE
                UNION
                SELECT * FROM #CLASE
                UNION
                SELECT * FROM #SUBCLASE
                UNION
                SELECT * FROM #PRODUCTO
                UNION
                SELECT * FROM #VENTA_DIARIA
                UNION
                SELECT * FROM #VENTA_SEMANAL
                UNION
                SELECT * FROM #VENTA_SEMANAL_TOTAL
                UNION
                SELECT * FROM #VENTA_SEMANAL_PRODUCTO
                UNION
                SELECT * FROM #VENTA_SEMANAL_TOTAL_PRODUCTO
            );

            -- SELECT * FROM #CAMP_RESULTS;

            -- DROP TABLE #GLOBAL;
            -- DROP TABLE #PROVEEDOR;
            -- DROP TABLE #REGION;
            -- DROP TABLE #FORMATO;
            -- DROP TABLE #TIENDA;
            -- DROP TABLE #TIENDA_SEMANAL;
            -- DROP TABLE #TIENDA_SEMANAL_TOTAL;
            -- DROP TABLE #FAMILIA;
            -- DROP TABLE #NSE;
            -- DROP TABLE #CLASE;
            -- DROP TABLE #SUBCLASE;
            -- DROP TABLE #PRODUCTO;
            -- DROP TABLE #VENTA_DIARIA;
            -- DROP TABLE #VENTA_SEMANAL;
            -- DROP TABLE #VENTA_SEMANAL_TOTAL;
            -- DROP TABLE #VENTA_SEMANAL_PRODUCTO;
            -- DROP TABLE #VENTA_SEMANAL_TOTAL_PRODUCTO;
            -- DROP TABLE #CAMP_RESULTS;
            '''
        
        query_res_insert = f'''
            --INSERTAR RESULTADOS
            DELETE CHEDRAUI.MON_CAMP_RESULTS WHERE CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}';
            INSERT INTO CHEDRAUI.MON_CAMP_RESULTS SELECT * FROM #CAMP_RESULTS;

            --SELECT * FROM CHEDRAUI.MON_CAMP_RESULTS WHERE CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}';
        '''

        return [query_dias_semana, query_res_global, query_res_proveedor, query_res_region, query_res_formato, query_res_tienda, query_res_tienda_semanal, query_res_tienda_semanal_total, query_res_tipo_familia, query_res_nse, query_res_clase, query_res_subclase, query_res_producto, query_res_venta_diaria, query_res_venta_semanal, query_res_venta_semanal_total, query_res_venta_semanal_producto, query_res_venta_semanal_total_producto, query_res, query_res_insert]
    
    def get_queries_campana_datos_roi(self):
        query_venta = f'''
            --TABLA VENTA
            DROP TABLE IF EXISTS #VENTA;
            CREATE TABLE #VENTA AS (
                SELECT
                1::INT IND_MC
                ,CASE
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} THEN 'ACTUAL'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']} THEN 'ANO_ANTERIOR'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_mes_anterior']} THEN 'MES_ANTERIOR'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_mes_anterior_ano_anterior']} THEN 'MES_ANTERIOR_ANO_ANTERIOR'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_acum']} THEN 'ACUMULADO'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_acum_ano_anterior']} THEN 'ACUMULADO_ANO_ANTERIOR'
                END AS PERIODO
                ,PROVEEDOR
                ,B.MARCA
                ,B.IND_MARCA
                ,SUM(SALE_NET_VAL) VENTA
                ,SUM(SALE_TOT_QTY) UNIDADES
                ,COUNT(DISTINCT CUSTOMER_CODE_TY) CLIENTES
                ,COUNT(DISTINCT INVOICE_NO) TX
                FROM FCT_SALE_LINE A
                INNER JOIN #PRODUCTOS B USING(PRODUCT_CODE)
            --     LEFT JOIN (SELECT DISTINCT INVOICE_NO, CASE WHEN CHANNEL_TYPE IN ('WEB','APP','CC HY') THEN 1 ELSE 0 END IND_ONLINE FROM FCT_SALE_HEADER) H ON A.INVOICE_NO = H.INVOICE_NO
                WHERE SALE_NET_VAL > 0
                AND BUSINESS_TYPE = 'R'
                AND (INVOICE_DATE BETWEEN {self.campana_variables['date_dash']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_mes_anterior']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_mes_anterior_ano_anterior']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_acum']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_acum_ano_anterior']}
                )
            --     AND STORE_CODE IN $[TIENDAS]
            --     AND IND_ONLINE = 1
                GROUP BY 1,2,3,4,5
                
            UNION
            
                SELECT
                0::INT IND_MC
                ,CASE
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash']} THEN 'ACTUAL'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']} THEN 'ANO_ANTERIOR'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_mes_anterior']} THEN 'MES_ANTERIOR'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_mes_anterior_ano_anterior']} THEN 'MES_ANTERIOR_ANO_ANTERIOR'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_acum']} THEN 'ACUMULADO'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_acum_ano_anterior']} THEN 'ACUMULADO_ANO_ANTERIOR'
                END AS PERIODO
                ,PROVEEDOR
                ,B.MARCA
                ,B.IND_MARCA
                ,SUM(SALE_NET_VAL) VENTA
                ,SUM(SALE_TOT_QTY) UNIDADES
                ,NULL CLIENTES
                ,COUNT(DISTINCT INVOICE_NO) TX
                FROM FCT_SALE_LINE_NM A
                INNER JOIN #PRODUCTOS B USING(PRODUCT_CODE)
            --     LEFT JOIN (SELECT DISTINCT INVOICE_NO, CASE WHEN CHANNEL_TYPE IN ('WEB','APP','CC HY') THEN 1 ELSE 0 END IND_ONLINE FROM FCT_SALE_HEADER) H ON A.INVOICE_NO = H.INVOICE_NO
                WHERE SALE_NET_VAL > 0
                AND BUSINESS_TYPE = 'R'
                AND (INVOICE_DATE BETWEEN {self.campana_variables['date_dash']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_ano_anterior']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_mes_anterior']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_mes_anterior_ano_anterior']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_acum']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_acum_ano_anterior']}
                )
            --     AND STORE_CODE IN $[TIENDAS]
            --     AND IND_ONLINE = 1
                GROUP BY 1,2,3,4,5
            );
            '''
        
        query_venta_periodos = f'''
            DROP TABLE IF EXISTS #VENTA_PERIODOS;
            CREATE TABLE #VENTA_PERIODOS AS (
            SELECT
                '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA
                ,PROVEEDOR
                ,'TOTAL' MARCA

                --MARCA
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_MARCA = 1 THEN VENTA END) VENTA_ACTUAL
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_MARCA = 1 THEN UNIDADES END) UNIDADES_ACTUAL
                ,SUM(DISTINCT CASE WHEN PERIODO = 'ACTUAL' AND IND_MARCA = 1 THEN CLIENTES END) CLIENTES_ACTUAL
                ,SUM(DISTINCT CASE WHEN PERIODO = 'ACTUAL' AND IND_MARCA = 1 THEN TX END) TX_ACTUAL

                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_MARCA = 1 THEN VENTA END) VENTA_ANO_ANTERIOR
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_MARCA = 1 THEN UNIDADES END) UNIDADES_ANO_ANTERIOR
                ,SUM(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_MARCA = 1 THEN CLIENTES END) CLIENTES_ANO_ANTERIOR
                ,SUM(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_MARCA = 1 THEN TX END) TX_ANO_ANTERIOR

                ,SUM(CASE WHEN PERIODO = 'MES_ANTERIOR' AND IND_MARCA = 1 THEN VENTA END) VENTA_MES_ANTERIOR
                ,SUM(CASE WHEN PERIODO = 'MES_ANTERIOR' AND IND_MARCA = 1 THEN UNIDADES END) UNIDADES_MES_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'MES_ANTERIOR' AND IND_MARCA = 1 THEN CLIENTES END) CLIENTES_MES_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'MES_ANTERIOR' AND IND_MARCA = 1 THEN TX END) TX_MES_ANTERIOR
            
                ,SUM(CASE WHEN PERIODO = 'MES_ANTERIOR_ANO_ANTERIOR' AND IND_MARCA = 1 THEN VENTA END) VENTA_MES_ANTERIOR_ANO_ANTERIOR
                ,SUM(CASE WHEN PERIODO = 'MES_ANTERIOR_ANO_ANTERIOR' AND IND_MARCA = 1 THEN UNIDADES END) UNIDADES_MES_ANTERIOR_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'MES_ANTERIOR_ANO_ANTERIOR' AND IND_MARCA = 1 THEN CLIENTES END) CLIENTES_MES_ANTERIOR_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'MES_ANTERIOR_ANO_ANTERIOR' AND IND_MARCA = 1 THEN TX END) TX_MES_ANTERIOR_ANO_ANTERIOR
            
                ,SUM(CASE WHEN PERIODO = 'ACUMULADO' AND IND_MARCA = 1 THEN VENTA END) VENTA_ACUM
                ,SUM(CASE WHEN PERIODO = 'ACUMULADO' AND IND_MARCA = 1 THEN UNIDADES END) UNIDADES_ACUM
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACUMULADO' AND IND_MARCA = 1 THEN CLIENTES END) CLIENTES_ACUM
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACUMULADO' AND IND_MARCA = 1 THEN TX END) TX_ACUM
            
                ,SUM(CASE WHEN PERIODO = 'ACUMULADO_ANO_ANTERIOR' AND IND_MARCA = 1 THEN VENTA END) VENTA_ACUM_ANO_ANTERIOR
                ,SUM(CASE WHEN PERIODO = 'ACUMULADO_ANO_ANTERIOR' AND IND_MARCA = 1 THEN UNIDADES END) UNIDADES_ACUM_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACUMULADO_ANO_ANTERIOR' AND IND_MARCA = 1 THEN CLIENTES END) CLIENTES_ACUM_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACUMULADO_ANO_ANTERIOR' AND IND_MARCA = 1 THEN TX END) TX_ACUM_ANO_ANTERIOR
                
                --CAT
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' THEN VENTA END) CAT_VENTA_ACTUAL
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' THEN UNIDADES END) CAT_UNIDADES_ACTUAL
                ,SUM(DISTINCT CASE WHEN PERIODO = 'ACTUAL' THEN CLIENTES END) CAT_CLIENTES_ACTUAL
                ,SUM(DISTINCT CASE WHEN PERIODO = 'ACTUAL' THEN TX END) CAT_TX_ACTUAL

                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN VENTA END) CAT_VENTA_ANO_ANTERIOR
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN UNIDADES END) CAT_UNIDADES_ANO_ANTERIOR
                ,SUM(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN CLIENTES END) CAT_CLIENTES_ANO_ANTERIOR
                ,SUM(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN TX END) CAT_TX_ANO_ANTERIOR

                ,SUM(CASE WHEN PERIODO = 'MES_ANTERIOR' THEN VENTA END) CAT_VENTA_MES_ANTERIOR
                ,SUM(CASE WHEN PERIODO = 'MES_ANTERIOR' THEN UNIDADES END) CAT_UNIDADES_MES_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'MES_ANTERIOR' THEN CLIENTES END) CAT_CLIENTES_MES_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'MES_ANTERIOR' THEN TX END) CAT_TX_MES_ANTERIOR
            
                ,SUM(CASE WHEN PERIODO = 'MES_ANTERIOR_ANO_ANTERIOR' THEN VENTA END) CAT_VENTA_MES_ANTERIOR_ANO_ANTERIOR
                ,SUM(CASE WHEN PERIODO = 'MES_ANTERIOR_ANO_ANTERIOR' THEN UNIDADES END) CAT_UNIDADES_MES_ANTERIOR_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'MES_ANTERIOR_ANO_ANTERIOR' THEN CLIENTES END) CAT_CLIENTES_MES_ANTERIOR_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'MES_ANTERIOR_ANO_ANTERIOR' THEN TX END) CAT_TX_MES_ANTERIOR_ANO_ANTERIOR
            
                ,SUM(CASE WHEN PERIODO = 'ACUMULADO' THEN VENTA END) CAT_VENTA_ACUM
                ,SUM(CASE WHEN PERIODO = 'ACUMULADO' THEN UNIDADES END) CAT_UNIDADES_ACUM
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACUMULADO' THEN CLIENTES END) CAT_CLIENTES_ACUM
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACUMULADO' THEN TX END) CAT_TX_ACUM
            
                ,SUM(CASE WHEN PERIODO = 'ACUMULADO_ANO_ANTERIOR' THEN VENTA END) CAT_VENTA_ACUM_ANO_ANTERIOR
                ,SUM(CASE WHEN PERIODO = 'ACUMULADO_ANO_ANTERIOR' THEN UNIDADES END) CAT_UNIDADES_ACUM_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACUMULADO_ANO_ANTERIOR' THEN CLIENTES END) CAT_CLIENTES_ACUM_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACUMULADO_ANO_ANTERIOR' THEN TX END) CAT_TX_ACUM_ANO_ANTERIOR
            FROM #VENTA
            GROUP BY 1,2,3
            );

            -- SELECT * FROM #VENTA_PERIODOS;
            '''
        
        query_datos_roi = f'''
            DROP TABLE IF EXISTS #DATOS_ROI;
            CREATE TABLE #DATOS_ROI AS (
            SELECT
                CODIGO_CAMPANA
                ,PROVEEDOR
                ,MARCA

                ,VENTA_ACTUAL
                ,VENTA_ANO_ANTERIOR
                ,VENTA_MES_ANTERIOR
                ,VENTA_MES_ANTERIOR_ANO_ANTERIOR
                ,VENTA_ACUM
                ,VENTA_ACUM_ANO_ANTERIOR

                ,CAT_VENTA_ACTUAL
                ,CAT_VENTA_ANO_ANTERIOR
                ,CAT_VENTA_MES_ANTERIOR
                ,CAT_VENTA_MES_ANTERIOR_ANO_ANTERIOR
                ,CAT_VENTA_ACUM
                ,CAT_VENTA_ACUM_ANO_ANTERIOR

                ,(VENTA_ACTUAL / VENTA_ANO_ANTERIOR) - 1 CRECIMIENTO_CAMPANA
                ,(VENTA_MES_ANTERIOR / VENTA_MES_ANTERIOR_ANO_ANTERIOR) - 1 CRECIMIENTO_MES_ANTERIOR
                ,(VENTA_ANO_ANTERIOR / VENTA_MES_ANTERIOR_ANO_ANTERIOR) - 1 CRECIMIENTO_MES_ANTERIOR_ANO_ANTERIOR
                ,(VENTA_ACUM / VENTA_ACUM_ANO_ANTERIOR) - 1 CRECIMIENTO_ACUM

                ,(CAT_VENTA_ACTUAL / CAT_VENTA_ANO_ANTERIOR) - 1 CAT_CRECIMIENTO_CAMPANA
                ,(CAT_VENTA_MES_ANTERIOR / CAT_VENTA_MES_ANTERIOR_ANO_ANTERIOR) - 1 CAT_CRECIMIENTO_MES_ANTERIOR
                ,(CAT_VENTA_ANO_ANTERIOR / CAT_VENTA_MES_ANTERIOR_ANO_ANTERIOR) - 1 CAT_CRECIMIENTO_MES_ANTERIOR_ANO_ANTERIOR
                ,(CAT_VENTA_ACUM / CAT_VENTA_ACUM_ANO_ANTERIOR) - 1 CAT_CRECIMIENTO_ACUM

                ,CRECIMIENTO_CAMPANA - CRECIMIENTO_MES_ANTERIOR DIF_CRECIMIENTO_MES_ANTERIOR
                ,CRECIMIENTO_CAMPANA - CRECIMIENTO_MES_ANTERIOR_ANO_ANTERIOR DIF_CRECIMIENTO_MES_ANTERIOR_ANO_ANTERIOR
                ,CRECIMIENTO_CAMPANA - CRECIMIENTO_ACUM DIF_CRECIMIENTO_ACUM

                ,(CRECIMIENTO_MES_ANTERIOR + 1) * VENTA_ANO_ANTERIOR VENTA_PROYECTADA_CRECIMIENTO_MES_ANTERIOR
                ,(CRECIMIENTO_MES_ANTERIOR_ANO_ANTERIOR + 1) * VENTA_ANO_ANTERIOR VENTA_PROYECTADA_CRECIMIENTO_MES_ANTERIOR_ANO_ANTERIOR
                ,(CRECIMIENTO_ACUM + 1) * VENTA_ANO_ANTERIOR VENTA_PROYECTADA_CRECIMIENTO_ACUM
            
                ,VENTA_ACTUAL - VENTA_PROYECTADA_CRECIMIENTO_MES_ANTERIOR VENTA_INCREMENTAL_MES_ANTERIOR
                ,VENTA_ACTUAL - VENTA_PROYECTADA_CRECIMIENTO_MES_ANTERIOR_ANO_ANTERIOR VENTA_INCREMENTAL_MES_ANTERIOR_ANO_ANTERIOR
                ,VENTA_ACTUAL - VENTA_PROYECTADA_CRECIMIENTO_ACUM VENTA_INCREMENTAL_ACUM

            FROM #VENTA_PERIODOS
            );

            --SELECT * FROM #DATOS_ROI;
        '''

        query_insert = f'''
            DELETE CHEDRAUI.MON_CAMP_ROI WHERE CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}';
            INSERT INTO CHEDRAUI.MON_CAMP_ROI SELECT * FROM #DATOS_ROI;
            '''

        return [query_venta, query_venta_periodos, query_datos_roi, query_insert]