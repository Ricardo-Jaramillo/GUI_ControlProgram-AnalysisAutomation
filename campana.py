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

        # Variables para Evolución de Venta
        # Calcular el final del mes de la fecha final de la campaña
        vigencia_fin_mes_evo = pd.Timestamp(f"{vigencia_fin_campana.year}-{vigencia_fin_campana.month + 1}-01") - pd.DateOffset(days=1)
        # Calcular la vigencia_fin, si aún no termina el mes, se toma la fecha actual menos 1 día. Si ya terminó el mes, se toma el final del mes de la fecha final de la campaña
        vigencia_fin_evo = pd.Timestamp.now().date() - pd.DateOffset(days=1) if pd.Timestamp.now().date() <= pd.Timestamp(vigencia_fin_mes_evo).date() else vigencia_fin_mes_evo
        # Calcular la vigencia_ini como el dia 1 de la vigencia_fin_mes_evo menos 11 meses
        vigencia_ini_evo = pd.Timestamp(f"{vigencia_fin_mes_evo.year}-{vigencia_fin_mes_evo.month}-01") - pd.DateOffset(months=11)
        # Calcular el año anterior de las vigencias de evolución
        vigencia_ini_evo_ano_anterior = vigencia_ini_evo - pd.DateOffset(years=1)
        vigencia_fin_evo_ano_anterior = vigencia_fin_evo - pd.DateOffset(years=1)

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
        date_dash_evo = f"'{vigencia_ini_evo}'::DATE AND '{vigencia_fin_evo}'::DATE"
        date_dash_evo_ano_anterior = f"'{vigencia_ini_evo_ano_anterior}'::DATE AND '{vigencia_fin_evo_ano_anterior}'::DATE"

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
                                    'date_dash_acum_ano_anterior': date_dash_acum_ano_anterior,
                                    'date_dash_evo': date_dash_evo,
                                    'date_dash_evo_ano_anterior': date_dash_evo_ano_anterior
                                    }
        
        print('Variables de la campaña asignadas')
        print(date_dash_evo)
        print(date_dash_evo_ano_anterior)

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
        lis_queries_datos_funnel = self.get_queries_campana_datos_funnel()
        lis_queries_tendencia = self.get_queries_campana_tendencia()
        lis_queries_evolucion = self.get_queries_campana_evolucion()
        lis_queries_segmentos = self.get_queries_campana_segmentos()
        lis_queries_retencion = self.get_queries_campana_retencion()

        lis_queries = lis_queries_venta + lis_queries_resultados + lis_queries_datos_roi + lis_queries_datos_funnel + lis_queries_tendencia + lis_queries_evolucion + lis_queries_segmentos + lis_queries_retencion

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
    
    def get_queries_campana_datos_funnel(self):
        query_venta_cliente_semanal = f'''
            --CLIENTES CON COMPRA
            DROP TABLE IF EXISTS #VENTA_CLIENTE_SEMANAL;
            CREATE TABLE #VENTA_CLIENTE_SEMANAL AS (
                SELECT
                CUSTOMER_CODE
                ,CONCAT(RIGHT(EXTRACT(YEAR FROM INVOICE_DATE), 2), CONCAT('-W', TO_CHAR(EXTRACT(WEEK FROM INVOICE_DATE), 'FM00'))) "WEEK"
                ,IND_CLIENTE_ELEGIBLE
                ,SUM(VENTA) VENTA
                ,COUNT(DISTINCT INVOICE_NO) TX
                ,SUM(CASE WHEN IND_ELEGIBLE = 1 THEN VENTA ELSE 0 END) VENTA_ELEGIBLE
                ,COUNT(DISTINCT CASE WHEN IND_ELEGIBLE = 1 THEN INVOICE_NO END) TX_ELEGIBLES
                ,SUM(CASE WHEN PROMOCIONES_APLICADAS > 0 THEN VENTA ELSE 0 END) VENTA_PROMOCIONADA
                ,SUM(PROMOCIONES_APLICADAS) PROMOCIONES_APLICADAS
                ,SUM(COSTO_PROMOCIONADO) COSTO_PROMOCIONADO
                FROM CHEDRAUI.MON_VTA
                WHERE IND_MARCA = 1
                AND PERIODO = 'CAMPANA'
                AND IND_MC = 1
            --     AND IND_ABIERTOS = 1
                GROUP BY 1,2,3
            );

            -- SELECT * FROM #VENTA_CLIENTE_SEMANAL;
            '''
        
        query_funnel_semanal = f'''
            --POR SEMANA VENTA Y ENVIOS
            DROP TABLE IF EXISTS #FUNNEL_SEMANAL;
            CREATE TABLE #FUNNEL_SEMANAL AS (
            WITH __FUNNEL AS (
                SELECT
                A.WEEK::TEXT
                ,'TOTAL' DATE
                ,CANAL
                ,'TOTAL' LIST_ID
                ,SUM(BASE) BASE
                ,SUM(ENVIOS) ENVIOS
                ,SUM(ABIERTOS) ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA_ELEGIBLE
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA_ELEGIBLE
                ,COALESCE(SUM(VENTA), 0) VENTA
                ,COALESCE(SUM(VENTA_ELEGIBLE), 0) VENTA_ELEGIBLE
                ,COALESCE(SUM(TX), 0) TX
                ,COALESCE(SUM(TX_ELEGIBLES), 0) TX_ELEGIBLES
                ,COALESCE(SUM(CASE WHEN IND_CLIENTE_ELEGIBLE = 1 THEN TX END), 0) TX_CLIENTES_ELEGIBLES
                FROM #BASE_CLIENTES A
                LEFT JOIN #VENTA_CLIENTE_SEMANAL B ON A.CUSTOMER_CODE_TY = B.CUSTOMER_CODE AND A.WEEK = B.WEEK AND A.ABIERTOS > 0
                WHERE GRUPO = 'GT'
                GROUP BY 1,2,3,4

                UNION

                SELECT
                'TOTAL' WEEK
                ,'TOTAL' DATE
                ,CANAL
                ,'TOTAL' LIST_ID
                ,SUM(BASE) BASE
                ,SUM(ENVIOS) ENVIOS
                ,SUM(ABIERTOS) ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA_ELEGIBLE
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA_ELEGIBLE
                ,COALESCE(SUM(VENTA), 0) VENTA
                ,COALESCE(SUM(VENTA_ELEGIBLE), 0) VENTA_ELEGIBLE
                ,COALESCE(SUM(TX), 0) TX
                ,COALESCE(SUM(TX_ELEGIBLES), 0) TX_ELEGIBLES
                ,COALESCE(SUM(CASE WHEN IND_CLIENTE_ELEGIBLE = 1 THEN TX END), 0) TX_CLIENTES_ELEGIBLES
                FROM #BASE_CLIENTES A
                LEFT JOIN #VENTA_CLIENTE_SEMANAL B ON A.CUSTOMER_CODE_TY = B.CUSTOMER_CODE AND A.WEEK = B.WEEK AND A.ABIERTOS > 0
                WHERE GRUPO = 'GT'
                GROUP BY 1,2,3,4

                UNION

                SELECT
                A.WEEK::TEXT
                ,'TOTAL' DATE
                ,'TOTAL' CANAL
                ,'TOTAL' LIST_ID
                ,SUM(BASE) BASE
                ,SUM(ENVIOS) ENVIOS
                ,SUM(ABIERTOS) ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA_ELEGIBLE
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA_ELEGIBLE
                ,COALESCE(SUM(VENTA), 0) VENTA
                ,COALESCE(SUM(VENTA_ELEGIBLE), 0) VENTA_ELEGIBLE
                ,COALESCE(SUM(TX), 0) TX
                ,COALESCE(SUM(TX_ELEGIBLES), 0) TX_ELEGIBLES
                ,COALESCE(SUM(CASE WHEN IND_CLIENTE_ELEGIBLE = 1 THEN TX END), 0) TX_CLIENTES_ELEGIBLES
                FROM #BASE_CLIENTES A
                LEFT JOIN #VENTA_CLIENTE_SEMANAL B ON A.CUSTOMER_CODE_TY = B.CUSTOMER_CODE AND A.WEEK = B.WEEK AND A.ABIERTOS > 0
                WHERE GRUPO = 'GT'
                GROUP BY 1,2,3,4

                UNION

                SELECT
                'TOTAL' WEEK
                ,'TOTAL' DATE
                ,'TOTAL' CANAL
                ,'TOTAL' LIST_ID
                ,SUM(BASE) BASE
                ,SUM(ENVIOS) ENVIOS
                ,SUM(ABIERTOS) ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA_ELEGIBLE
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA_ELEGIBLE
                ,COALESCE(SUM(VENTA), 0) VENTA
                ,COALESCE(SUM(VENTA_ELEGIBLE), 0) VENTA_ELEGIBLE
                ,COALESCE(SUM(TX), 0) TX
                ,COALESCE(SUM(TX_ELEGIBLES), 0) TX_ELEGIBLES
                ,COALESCE(SUM(CASE WHEN IND_CLIENTE_ELEGIBLE = 1 THEN TX END), 0) TX_CLIENTES_ELEGIBLES
                FROM #BASE_CLIENTES A
                LEFT JOIN #VENTA_CLIENTE_SEMANAL B ON A.CUSTOMER_CODE_TY = B.CUSTOMER_CODE AND A.WEEK = B.WEEK AND A.ABIERTOS > 0
                WHERE GRUPO = 'GT'
                GROUP BY 1,2,3,4

                UNION

                SELECT
                A.WEEK::TEXT
                ,DATE::TEXT
                ,CANAL
                ,LIST_ID::TEXT
                ,SUM(BASE) BASE
                ,SUM(ENVIOS) ENVIOS
                ,SUM(ABIERTOS) ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA_ELEGIBLE
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA_ELEGIBLE
                ,COALESCE(SUM(VENTA), 0) VENTA
                ,COALESCE(SUM(VENTA_ELEGIBLE), 0) VENTA_ELEGIBLE
                ,COALESCE(SUM(TX), 0) TX
                ,COALESCE(SUM(TX_ELEGIBLES), 0) TX_ELEGIBLES
                ,COALESCE(SUM(CASE WHEN IND_CLIENTE_ELEGIBLE = 1 THEN TX END), 0) TX_CLIENTES_ELEGIBLES
                FROM #BASE_CLIENTES A
                LEFT JOIN #VENTA_CLIENTE_SEMANAL B ON A.CUSTOMER_CODE_TY = B.CUSTOMER_CODE AND A.WEEK = B.WEEK AND A.ABIERTOS > 0
                WHERE GRUPO = 'GT'
                GROUP BY 1,2,3,4
                
                UNION
                
                SELECT
                'TOTAL' WEEK
                ,DATE::TEXT
                ,CANAL
                ,LIST_ID::TEXT
                ,SUM(BASE) BASE
                ,SUM(ENVIOS) ENVIOS
                ,SUM(ABIERTOS) ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA_ELEGIBLE
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA_ELEGIBLE
                ,COALESCE(SUM(VENTA), 0) VENTA
                ,COALESCE(SUM(VENTA_ELEGIBLE), 0) VENTA_ELEGIBLE
                ,COALESCE(SUM(TX), 0) TX
                ,COALESCE(SUM(TX_ELEGIBLES), 0) TX_ELEGIBLES
                ,COALESCE(SUM(CASE WHEN IND_CLIENTE_ELEGIBLE = 1 THEN TX END), 0) TX_CLIENTES_ELEGIBLES
                FROM #BASE_CLIENTES A
                LEFT JOIN #VENTA_CLIENTE_SEMANAL B ON A.CUSTOMER_CODE_TY = B.CUSTOMER_CODE AND A.WEEK = B.WEEK AND A.ABIERTOS > 0
                WHERE GRUPO = 'GT'
                GROUP BY 1,2,3,4
            
                UNION
                
                SELECT
                'TOTAL' WEEK
                ,'TOTAL' DATE
                ,'TOTAL' CANAL
                ,'TOTAL' LIST_ID
                ,SUM(BASE) BASE
                ,SUM(ENVIOS) ENVIOS
                ,SUM(ABIERTOS) ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ENVIOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ENVIOS_CON_COMPRA_ELEGIBLE
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA
                ,COUNT(DISTINCT CASE WHEN ABIERTOS > 0 AND VENTA_ELEGIBLE > 0 THEN A.CUSTOMER_CODE_TY END) CLIENTES_ABIERTOS_CON_COMPRA_ELEGIBLE
                ,COALESCE(SUM(VENTA), 0) VENTA
                ,COALESCE(SUM(VENTA_ELEGIBLE), 0) VENTA_ELEGIBLE
                ,COALESCE(SUM(TX), 0) TX
                ,COALESCE(SUM(TX_ELEGIBLES), 0) TX_ELEGIBLES
                ,COALESCE(SUM(CASE WHEN IND_CLIENTE_ELEGIBLE = 1 THEN TX END), 0) TX_CLIENTES_ELEGIBLES
                FROM #BASE_CLIENTES A
                LEFT JOIN #VENTA_CLIENTE_SEMANAL B ON A.CUSTOMER_CODE_TY = B.CUSTOMER_CODE AND A.WEEK = B.WEEK AND A.ABIERTOS > 0
                WHERE GRUPO = 'GT'
                GROUP BY 1,2,3,4
            )
            SELECT
                '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA
                ,*
            FROM __FUNNEL
            );
            '''
        
        query_insert = f'''
            DELETE CHEDRAUI.MON_CAMP_CONTACT_FUNNEL WHERE CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}';
            INSERT INTO CHEDRAUI.MON_CAMP_CONTACT_FUNNEL SELECT * FROM #FUNNEL_SEMANAL ORDER BY 1,2,3,4;
            '''
        
        return [query_venta_cliente_semanal, query_funnel_semanal, query_insert]
    
    def get_queries_campana_tendencia(self):
        query_dias = f'''
            --CREAR DIAS Y SEMANAS DEL AÑO
            DROP TABLE IF EXISTS #DIAS;
            CREATE TABLE #DIAS AS (
            WITH __DIAS AS (
            SELECT DISTINCT
                INVOICE_DATE
                ,EXTRACT(DOW FROM INVOICE_DATE) DIA_SEMANA
                ,PERIODO_ANALISIS
                ,CASE
                WHEN PERIODO_ANALISIS IN ('CAMPANA', 'CAMPANA_A') THEN 'CAMPANA'
                WHEN PERIODO_ANALISIS IN ('PRE_CAMPANA', 'PRE_CAMPANA_A') THEN 'PRE_CAMPANA'
                WHEN PERIODO_ANALISIS IN ('POST_CAMPANA', 'POST_CAMPANA_A') THEN 'POST_CAMPANA'
                END PERIODO
                ,CONCAT(RIGHT(EXTRACT(YEAR FROM INVOICE_DATE), 2), CONCAT('-W', TO_CHAR(EXTRACT(WEEK FROM INVOICE_DATE), 'FM00'))) "WEEK"
            FROM CHEDRAUI.MON_VTA
            WHERE PERIODO_ANALISIS IN ('CAMPANA', 'CAMPANA_A', 'PRE_CAMPANA', 'PRE_CAMPANA_A', 'POST_CAMPANA', 'POST_CAMPANA_A')
            ORDER BY INVOICE_DATE
            )
            SELECT
            *
            ,RANK() OVER(PARTITION BY PERIODO_ANALISIS ORDER BY PERIODO_ANALISIS, INVOICE_DATE) DAY_NUMBER
            FROM __DIAS
            );

            --SELECT * FROM #DIAS;
            '''
        
        query_venta_diaria = f'''
            --VENTA_DIARIA
            DROP TABLE IF EXISTS #VENTA_DIARIA;
            CREATE TABLE #VENTA_DIARIA AS (
            SELECT
                IND_MC
                ,PROVEEDOR
                ,'TOTAL' MARCA
                ,B.PERIODO
                ,C.INVOICE_DATE
                ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO_ANALISIS IN ('PRE_CAMPANA', 'CAMPANA', 'POST_CAMPANA') THEN VENTA END) VENTA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO_ANALISIS IN ('PRE_CAMPANA', 'CAMPANA', 'POST_CAMPANA') THEN INVOICE_NO END) TX
                ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO_ANALISIS IN ('PRE_CAMPANA_A', 'CAMPANA_A', 'POST_CAMPANA_A') THEN VENTA END) VENTA_A
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO_ANALISIS IN ('PRE_CAMPANA_A', 'CAMPANA_A', 'POST_CAMPANA_A') THEN INVOICE_NO END) TX_A
                ,SUM(CASE WHEN A.PERIODO_ANALISIS IN ('PRE_CAMPANA', 'CAMPANA', 'POST_CAMPANA') THEN VENTA END) CAT_VENTA
                ,COUNT(DISTINCT CASE WHEN A.PERIODO_ANALISIS IN ('PRE_CAMPANA', 'CAMPANA', 'POST_CAMPANA') THEN INVOICE_NO END) CAT_TX
                ,SUM(CASE WHEN A.PERIODO_ANALISIS IN ('PRE_CAMPANA_A', 'CAMPANA_A', 'POST_CAMPANA_A') THEN VENTA END) CAT_VENTA_A
                ,COUNT(DISTINCT CASE WHEN A.PERIODO_ANALISIS IN ('PRE_CAMPANA_A', 'CAMPANA_A', 'POST_CAMPANA_A') THEN INVOICE_NO END) CAT_TX_A
            FROM CHEDRAUI.MON_VTA A
                LEFT JOIN #DIAS B USING(INVOICE_DATE)
                LEFT JOIN (SELECT * FROM #DIAS WHERE PERIODO_ANALISIS IN ('CAMPANA', 'PRE_CAMPANA', 'POST_CAMPANA')) C ON B.DAY_NUMBER = C.DAY_NUMBER AND C.PERIODO_ANALISIS = B.PERIODO
            WHERE A.PERIODO_ANALISIS IN ('PRE_CAMPANA', 'CAMPANA', 'POST_CAMPANA', 'PRE_CAMPANA_A', 'CAMPANA_A', 'POST_CAMPANA_A')
            GROUP BY 1,2,3,4,5--,6
            );

            -- SELECT * FROM #VENTA_DIARIA;
            '''
        
        query_venta_diaria_elegible = f'''
            --CUMPLE CONDICION
            DROP TABLE IF EXISTS #VENTA_DIARIA_ELEGIBLE;
            CREATE TABLE #VENTA_DIARIA_ELEGIBLE AS (
            SELECT
                IND_MC
                ,PROVEEDOR
                ,'TOTAL' MARCA
                ,B.PERIODO
                ,C.INVOICE_DATE
                ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO_ANALISIS IN ('PRE_CAMPANA', 'CAMPANA', 'POST_CAMPANA') THEN VENTA END) ELEGIBLE_VENTA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO_ANALISIS IN ('PRE_CAMPANA', 'CAMPANA', 'POST_CAMPANA') THEN INVOICE_NO END) ELEGIBLE_TX
                ,SUM(CASE WHEN IND_MARCA = 1 AND A.PERIODO_ANALISIS IN ('PRE_CAMPANA_A', 'CAMPANA_A', 'POST_CAMPANA_A') THEN VENTA END) ELEGIBLE_VENTA_A
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND A.PERIODO_ANALISIS IN ('PRE_CAMPANA_A', 'CAMPANA_A', 'POST_CAMPANA_A') THEN INVOICE_NO END) ELEGIBLE_TX_A
                ,SUM(CASE WHEN A.PERIODO_ANALISIS IN ('PRE_CAMPANA', 'CAMPANA', 'POST_CAMPANA') THEN VENTA END) ELEGIBLE_CAT_VENTA
                ,COUNT(DISTINCT CASE WHEN A.PERIODO_ANALISIS IN ('PRE_CAMPANA', 'CAMPANA', 'POST_CAMPANA') THEN INVOICE_NO END) ELEGIBLE_CAT_TX
                ,SUM(CASE WHEN A.PERIODO_ANALISIS IN ('PRE_CAMPANA_A', 'CAMPANA_A', 'POST_CAMPANA_A') THEN VENTA END) ELEGIBLE_CAT_VENTA_A
                ,COUNT(DISTINCT CASE WHEN A.PERIODO_ANALISIS IN ('PRE_CAMPANA_A', 'CAMPANA_A', 'POST_CAMPANA_A') THEN INVOICE_NO END) ELEGIBLE_CAT_TX_A
            FROM CHEDRAUI.MON_VTA A
                LEFT JOIN #DIAS B USING(INVOICE_DATE)
                LEFT JOIN (SELECT * FROM #DIAS WHERE PERIODO_ANALISIS IN ('CAMPANA', 'PRE_CAMPANA', 'POST_CAMPANA')) C ON B.DAY_NUMBER = C.DAY_NUMBER AND C.PERIODO_ANALISIS = B.PERIODO
            WHERE A.PERIODO_ANALISIS IN ('PRE_CAMPANA', 'CAMPANA', 'POST_CAMPANA', 'PRE_CAMPANA_A', 'CAMPANA_A', 'POST_CAMPANA_A')
            AND IND_ELEGIBLE = 1
            GROUP BY 1,2,3,4,5
            );

            -- SELECT * FROM #VENTA_DIARIA_ELEGIBLE;
            '''
        
        query_tendencia = f'''
            DROP TABLE IF EXISTS #TENDENCIA;
            CREATE TABLE #TENDENCIA AS (
            SELECT
                '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA
                ,IND_MC
                ,PROVEEDOR
                ,MARCA
                ,PERIODO
                ,INVOICE_DATE
                ,VENTA
                ,TX
                ,VENTA_A
                ,TX_A
                ,CAT_VENTA
                ,CAT_TX
                ,CAT_VENTA_A
                ,CAT_TX_A
                ,ELEGIBLE_VENTA
                ,ELEGIBLE_TX
                ,ELEGIBLE_VENTA_A
                ,ELEGIBLE_TX_A
                ,ELEGIBLE_CAT_VENTA
                ,ELEGIBLE_CAT_TX
                ,ELEGIBLE_CAT_VENTA_A
                ,ELEGIBLE_CAT_TX_A
            FROM #VENTA_DIARIA
            LEFT JOIN #VENTA_DIARIA_ELEGIBLE USING(IND_MC, PROVEEDOR, MARCA, PERIODO, INVOICE_DATE)
            );

            -- SELECT * FROM #TENDENCIA ORDER BY 1,2,3,4,5;
            '''
        
        query_insert = f'''
            DELETE CHEDRAUI.MON_CAMP_TENDENCY WHERE CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}';
            INSERT INTO CHEDRAUI.MON_CAMP_TENDENCY SELECT * FROM #TENDENCIA;
        '''

        return [query_dias, query_venta_diaria, query_venta_diaria_elegible, query_tendencia, query_insert]
    
    def get_queries_campana_evolucion(self):
        query_meses = f'''
            --CREAR FECHAS DE MES
            DROP TABLE IF EXISTS #MESES;
            CREATE TABLE #MESES AS (
            WITH __MESES AS (
            SELECT DISTINCT
                LEFT(INVOICE_DATE, 7) MES
                ,EXTRACT(MONTH FROM INVOICE_DATE) MES_NUMERO
                ,CASE
                WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo']} THEN 'ACTUAL'
                WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo_ano_anterior']} THEN 'ANO_ANTERIOR'
                END AS PERIODO
            FROM FCT_SALE_HEADER
            WHERE INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo_ano_anterior']}
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
                ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.campana_variables['condicion_compra']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE
            FROM FCT_SALE_LINE
            INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
            LEFT JOIN (SELECT DISTINCT INVOICE_NO, CASE WHEN CHANNEL_TYPE IN ('WEB','APP','CC HY') THEN 1 ELSE 0 END IND_ONLINE FROM FCT_SALE_HEADER) F USING(INVOICE_NO)
            WHERE (INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo_ano_anterior']})
            AND BUSINESS_TYPE = 'R'
            AND SALE_NET_VAL > 0
            GROUP BY 1,2,3,4

            UNION
            
            SELECT
                INVOICE_NO
                ,IND_MARCA
                ,MARCA
                ,IND_ONLINE
                ,CASE WHEN SUM(SALE_NET_VAL) >= '{self.campana_variables['condicion_compra']}' THEN 1 ELSE 0 END AS IND_ELEGIBLE
            FROM FCT_SALE_LINE_NM A
            INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
            LEFT JOIN (SELECT DISTINCT INVOICE_NO, CASE WHEN CHANNEL_TYPE IN ('WEB','APP','CC HY') THEN 1 ELSE 0 END IND_ONLINE FROM FCT_SALE_HEADER_NM) F USING(INVOICE_NO)
            WHERE (INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo']}
                OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo_ano_anterior']})
            AND BUSINESS_TYPE = 'R'
            AND SALE_NET_VAL > 0
            GROUP BY 1,2,3,4
            );
            '''
        
        query_venta = f'''
            --TABLA VENTA
            DROP TABLE IF EXISTS #VENTA;
            CREATE TABLE #VENTA AS (
                SELECT
                1::INT IND_MC
                ,CUSTOMER_CODE_TY
                ,EXTRACT(MONTH FROM INVOICE_DATE) MES_NUMERO
                ,CASE
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo']} THEN 'ACTUAL'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo_ano_anterior']} THEN 'ANO_ANTERIOR'
                END AS PERIODO
                ,A.INVOICE_NO
                ,IND_ELEGIBLE
                ,PROVEEDOR
                ,B.MARCA
                ,B.IND_MARCA
                ,SUM(SALE_TOT_QTY) UNIDADES
                ,SUM(SALE_NET_VAL) VENTA
                FROM FCT_SALE_LINE A
                INNER JOIN #PRODUCTOS B USING(PRODUCT_CODE)
                LEFT JOIN #TX C ON A.INVOICE_NO = C.INVOICE_NO AND B.IND_MARCA = C.IND_MARCA AND B.MARCA = C.MARCA
                WHERE (INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo']}
                    OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo_ano_anterior']})
                AND SALE_NET_VAL > 0
            --     AND STORE_CODE IN $[TIENDAS]
            --     AND IND_ONLINE = 1
                GROUP BY 1,2,3,4,5,6,7,8,9
                
            UNION
            
                SELECT
                0::INT IND_MC
                ,NULL CUSTOMER_CODE_TY
                ,EXTRACT(MONTH FROM INVOICE_DATE) MES_NUMERO
                ,CASE
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo']} THEN 'ACTUAL'
                    WHEN INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo_ano_anterior']} THEN 'ANO_ANTERIOR'
                END AS PERIODO
                ,A.INVOICE_NO
                ,IND_ELEGIBLE
                ,PROVEEDOR
                ,B.MARCA
                ,B.IND_MARCA
                ,SUM(SALE_TOT_QTY) UNIDADES
                ,SUM(SALE_NET_VAL) VENTA
                FROM FCT_SALE_LINE_NM A
                INNER JOIN #PRODUCTOS B USING(PRODUCT_CODE)
                LEFT JOIN #TX C ON A.INVOICE_NO = C.INVOICE_NO AND B.IND_MARCA = C.IND_MARCA AND B.MARCA = C.MARCA
                WHERE (INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo']}
                    OR INVOICE_DATE BETWEEN {self.campana_variables['date_dash_evo_ano_anterior']})
                AND SALE_NET_VAL > 0
            --     AND STORE_CODE IN $[TIENDAS]
            --     AND IND_ONLINE = 1
                GROUP BY 1,2,3,4,5,6,7,8,9
            );
            '''
        
        query_venta_mensual = f'''
            --VENTA AGRUPADA POR MES
            DROP TABLE IF EXISTS #VENTA_MENSUAL;
            CREATE TABLE #VENTA_MENSUAL AS (
            SELECT
                '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA
                ,IND_MC
                ,PROVEEDOR
                ,MARCA
                ,B.MES

                --MARCA
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' THEN VENTA END) VENTA_ACTUAL
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' THEN UNIDADES END) UNIDADES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' THEN CUSTOMER_CODE_TY END) CLIENTES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' THEN INVOICE_NO END) TX_ACTUAL

                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ANO_ANTERIOR' THEN VENTA END) VENTA_ANO_ANTERIOR
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ANO_ANTERIOR' THEN UNIDADES END) UNIDADES_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ANO_ANTERIOR' THEN CUSTOMER_CODE_TY END) CLIENTES_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ANO_ANTERIOR' THEN INVOICE_NO END) TX_ANO_ANTERIOR
                
                --ELEGIBLE
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE = 1 THEN VENTA END) VENTA_ELEGIBLE_ACTUAL
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE = 1 THEN UNIDADES END) UNIDADES_ELEGIBLE_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_ELEGIBLES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) TX_ELEGIBLES_ACTUAL

                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN VENTA END) VENTA_ELEGIBLE_ANO_ANTERIOR
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN UNIDADES END) UNIDADES_ELEGIBLE_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN CUSTOMER_CODE_TY END) CLIENTES_ELEGIBLES_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 AND PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) TX_ELEGIBLES_ANO_ANTERIOR
                
                --CAT
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' THEN VENTA END) CAT_VENTA_ACTUAL
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' THEN UNIDADES END) CAT_UNIDADES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACTUAL' THEN CUSTOMER_CODE_TY END) CAT_CLIENTES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACTUAL' THEN INVOICE_NO END) CAT_TX_ACTUAL

                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN VENTA END) CAT_VENTA_ANO_ANTERIOR
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN UNIDADES END) CAT_UNIDADES_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN CUSTOMER_CODE_TY END) CAT_CLIENTES_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN INVOICE_NO END) CAT_TX_ANO_ANTERIOR
                
                --ELEGIBLE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ELEGIBLE = 1 THEN VENTA END) CAT_VENTA_ELEGIBLE_ACTUAL
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ELEGIBLE = 1 THEN UNIDADES END) CAT_UNIDADES_ELEGIBLE_ACTUAL
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACTUAL' AND IND_ELEGIBLE = 1 THEN CUSTOMER_CODE_TY END) CAT_CLIENTES_ELEGIBLES_ACTUAL
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACTUAL' AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) CAT_TX_ELEGIBLES_ACTUAL

                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN VENTA END) CAT_VENTA_ELEGIBLE_ANO_ANTERIOR
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN UNIDADES END) CAT_UNIDADES_ELEGIBLE_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN CUSTOMER_CODE_TY END) CAT_CLIENTES_ELEGIBLES_ANO_ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_ELEGIBLE = 1 THEN INVOICE_NO END) CAT_TX_ELEGIBLES_ANO_ANTERIOR

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            GROUP BY 1,2,3,4,5
            );

            -- SELECT * FROM #VENTA_MENSUAL ORDER BY 1,2,3,4,5;
            '''
        
        query_insert = f'''
            DELETE CHEDRAUI.MON_CAMP_EVOLUTION_SALES WHERE CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}';
            INSERT INTO CHEDRAUI.MON_CAMP_EVOLUTION_SALES SELECT * FROM #VENTA_MENSUAL;
        '''

        return [query_meses, query_tx, query_venta, query_venta_mensual, query_insert]

    def get_queries_campana_segmentos(self):
        query_indicadores = f'''
            --SEGMENTOS
            --INDICADORES
            -- SELECT * FROM #INDICADORES;
            DROP TABLE IF EXISTS #INDICADORES;
            CREATE TABLE #INDICADORES AS (
            SELECT
                CUSTOMER_CODE_TY
                ,PROVEEDOR
                ,MARCA
                
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
                
                --VENTA
                ,SUM(CASE WHEN IND_MES = 1 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_1
                ,SUM(CASE WHEN IND_MES = 2 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_2
                ,SUM(CASE WHEN IND_MES = 3 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_3
                ,SUM(CASE WHEN IND_MES = 4 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_4
                ,SUM(CASE WHEN IND_MES = 5 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_5
                ,SUM(CASE WHEN IND_MES = 6 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_6
                ,SUM(CASE WHEN IND_MES = 7 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_7
                ,SUM(CASE WHEN IND_MES = 8 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_8
                ,SUM(CASE WHEN IND_MES = 9 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_9
                ,SUM(CASE WHEN IND_MES = 10 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_10
                ,SUM(CASE WHEN IND_MES = 11 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_11
                ,SUM(CASE WHEN IND_MES = 12 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_12
                ,SUM(CASE WHEN IND_MES = 13 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_13
                ,SUM(CASE WHEN IND_MES = 14 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_14
                ,SUM(CASE WHEN IND_MES = 15 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_15
                ,SUM(CASE WHEN IND_MES = 16 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_16
                ,SUM(CASE WHEN IND_MES = 17 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_17
                ,SUM(CASE WHEN IND_MES = 18 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_18
                ,SUM(CASE WHEN IND_MES = 19 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_19
                ,SUM(CASE WHEN IND_MES = 20 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_20
                ,SUM(CASE WHEN IND_MES = 21 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_21
                ,SUM(CASE WHEN IND_MES = 22 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_22
                ,SUM(CASE WHEN IND_MES = 23 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_23
                ,SUM(CASE WHEN IND_MES = 24 AND IND_MARCA = 1 THEN VENTA ELSE 0 END) VENTA_24

                --TX
                ,COUNT(CASE WHEN IND_MES = 1 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_1
                ,COUNT(CASE WHEN IND_MES = 2 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_2
                ,COUNT(CASE WHEN IND_MES = 3 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_3
                ,COUNT(CASE WHEN IND_MES = 4 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_4
                ,COUNT(CASE WHEN IND_MES = 5 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_5
                ,COUNT(CASE WHEN IND_MES = 6 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_6
                ,COUNT(CASE WHEN IND_MES = 7 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_7
                ,COUNT(CASE WHEN IND_MES = 8 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_8
                ,COUNT(CASE WHEN IND_MES = 9 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_9
                ,COUNT(CASE WHEN IND_MES = 10 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_10
                ,COUNT(CASE WHEN IND_MES = 11 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_11
                ,COUNT(CASE WHEN IND_MES = 12 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_12
                ,COUNT(CASE WHEN IND_MES = 13 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_13
                ,COUNT(CASE WHEN IND_MES = 14 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_14
                ,COUNT(CASE WHEN IND_MES = 15 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_15
                ,COUNT(CASE WHEN IND_MES = 16 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_16
                ,COUNT(CASE WHEN IND_MES = 17 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_17
                ,COUNT(CASE WHEN IND_MES = 18 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_18
                ,COUNT(CASE WHEN IND_MES = 19 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_19
                ,COUNT(CASE WHEN IND_MES = 20 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_20
                ,COUNT(CASE WHEN IND_MES = 21 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_21
                ,COUNT(CASE WHEN IND_MES = 22 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_22
                ,COUNT(CASE WHEN IND_MES = 23 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_23
                ,COUNT(CASE WHEN IND_MES = 24 AND IND_MARCA = 1 THEN INVOICE_NO END) TX_24
                
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
                
                --VENTA
                ,SUM(CASE WHEN IND_MES = 1 THEN VENTA ELSE 0 END) CAT_VENTA_1
                ,SUM(CASE WHEN IND_MES = 2 THEN VENTA ELSE 0 END) CAT_VENTA_2
                ,SUM(CASE WHEN IND_MES = 3 THEN VENTA ELSE 0 END) CAT_VENTA_3
                ,SUM(CASE WHEN IND_MES = 4 THEN VENTA ELSE 0 END) CAT_VENTA_4
                ,SUM(CASE WHEN IND_MES = 5 THEN VENTA ELSE 0 END) CAT_VENTA_5
                ,SUM(CASE WHEN IND_MES = 6 THEN VENTA ELSE 0 END) CAT_VENTA_6
                ,SUM(CASE WHEN IND_MES = 7 THEN VENTA ELSE 0 END) CAT_VENTA_7
                ,SUM(CASE WHEN IND_MES = 8 THEN VENTA ELSE 0 END) CAT_VENTA_8
                ,SUM(CASE WHEN IND_MES = 9 THEN VENTA ELSE 0 END) CAT_VENTA_9
                ,SUM(CASE WHEN IND_MES = 10 THEN VENTA ELSE 0 END) CAT_VENTA_10
                ,SUM(CASE WHEN IND_MES = 11 THEN VENTA ELSE 0 END) CAT_VENTA_11
                ,SUM(CASE WHEN IND_MES = 12 THEN VENTA ELSE 0 END) CAT_VENTA_12
                ,SUM(CASE WHEN IND_MES = 13 THEN VENTA ELSE 0 END) CAT_VENTA_13
                ,SUM(CASE WHEN IND_MES = 14 THEN VENTA ELSE 0 END) CAT_VENTA_14
                ,SUM(CASE WHEN IND_MES = 15 THEN VENTA ELSE 0 END) CAT_VENTA_15
                ,SUM(CASE WHEN IND_MES = 16 THEN VENTA ELSE 0 END) CAT_VENTA_16
                ,SUM(CASE WHEN IND_MES = 17 THEN VENTA ELSE 0 END) CAT_VENTA_17
                ,SUM(CASE WHEN IND_MES = 18 THEN VENTA ELSE 0 END) CAT_VENTA_18
                ,SUM(CASE WHEN IND_MES = 19 THEN VENTA ELSE 0 END) CAT_VENTA_19
                ,SUM(CASE WHEN IND_MES = 20 THEN VENTA ELSE 0 END) CAT_VENTA_20
                ,SUM(CASE WHEN IND_MES = 21 THEN VENTA ELSE 0 END) CAT_VENTA_21
                ,SUM(CASE WHEN IND_MES = 22 THEN VENTA ELSE 0 END) CAT_VENTA_22
                ,SUM(CASE WHEN IND_MES = 23 THEN VENTA ELSE 0 END) CAT_VENTA_23
                ,SUM(CASE WHEN IND_MES = 24 THEN VENTA ELSE 0 END) CAT_VENTA_24

                --COMPETENCIA
                --TX
                ,COUNT(CASE WHEN IND_MES = 1 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_1
                ,COUNT(CASE WHEN IND_MES = 2 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_2
                ,COUNT(CASE WHEN IND_MES = 3 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_3
                ,COUNT(CASE WHEN IND_MES = 4 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_4
                ,COUNT(CASE WHEN IND_MES = 5 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_5
                ,COUNT(CASE WHEN IND_MES = 6 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_6
                ,COUNT(CASE WHEN IND_MES = 7 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_7
                ,COUNT(CASE WHEN IND_MES = 8 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_8
                ,COUNT(CASE WHEN IND_MES = 9 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_9
                ,COUNT(CASE WHEN IND_MES = 10 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_10
                ,COUNT(CASE WHEN IND_MES = 11 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_11
                ,COUNT(CASE WHEN IND_MES = 12 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_12
                ,COUNT(CASE WHEN IND_MES = 13 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_13
                ,COUNT(CASE WHEN IND_MES = 14 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_14
                ,COUNT(CASE WHEN IND_MES = 15 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_15
                ,COUNT(CASE WHEN IND_MES = 16 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_16
                ,COUNT(CASE WHEN IND_MES = 17 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_17
                ,COUNT(CASE WHEN IND_MES = 18 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_18
                ,COUNT(CASE WHEN IND_MES = 19 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_19
                ,COUNT(CASE WHEN IND_MES = 20 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_20
                ,COUNT(CASE WHEN IND_MES = 21 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_21
                ,COUNT(CASE WHEN IND_MES = 22 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_22
                ,COUNT(CASE WHEN IND_MES = 23 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_23
                ,COUNT(CASE WHEN IND_MES = 24 AND IND_MARCA = 0 THEN INVOICE_NO END) COMP_TX_24
            FROM #VENTA
            LEFT JOIN #MESES USING(MES_NUMERO, PERIODO)
            WHERE IND_MC = 1
            GROUP BY 1,2,3
            );
            '''
        
        query_segmentos = f'''
            --SEGMENTOS
            DROP TABLE IF EXISTS #SEGMENTOS;
            CREATE TABLE #SEGMENTOS AS (
            WITH __INDICADORES AS (
                SELECT
                    *
                    --POS
                --PO RECOMPRA
                ,IND_12 AS PO_RECOMPRA_13
                ,IND_13 AS PO_RECOMPRA_14
                ,IND_14 AS PO_RECOMPRA_15
                ,IND_15 AS PO_RECOMPRA_16
                ,IND_16 AS PO_RECOMPRA_17
                ,IND_17 AS PO_RECOMPRA_18
                ,IND_18 AS PO_RECOMPRA_19
                ,IND_19 AS PO_RECOMPRA_20
                ,IND_20 AS PO_RECOMPRA_21
                ,IND_21 AS PO_RECOMPRA_22
                ,IND_22 AS PO_RECOMPRA_23
                ,IND_23 AS PO_RECOMPRA_24
                
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
                
                --PO REPETIDORES (COMPRAN MÁS DE UN TX)
                ,CASE WHEN TX_1 + TX_2 + TX_3 + TX_4 + TX_5 + TX_6 + TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_13 --      ,CASE WHEN TX_7 > 1 AND TX_8 > 1 AND TX_9 > 1 AND TX_10 > 1 AND TX_11 > 1 AND TX_12 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_13
                ,CASE WHEN TX_2 + TX_3 + TX_4 + TX_5 + TX_6 + TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_14
                ,CASE WHEN TX_3 + TX_4 + TX_5 + TX_6 + TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_15
                ,CASE WHEN TX_4 + TX_5 + TX_6 + TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_16
                ,CASE WHEN TX_5 + TX_6 + TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_17
                ,CASE WHEN TX_6 + TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_18
                ,CASE WHEN TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 + TX_18 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_19
                ,CASE WHEN TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 + TX_18 + TX_19 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_20
                ,CASE WHEN TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 + TX_18 + TX_19 + TX_20 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_21
                ,CASE WHEN TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 + TX_18 + TX_19 + TX_20 + TX_21 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_22
                ,CASE WHEN TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 + TX_18 + TX_19 + TX_20 + TX_21 + TX_22 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_23
                ,CASE WHEN TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 + TX_18 + TX_19 + TX_20 + TX_21 + TX_22 + TX_23 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_24
                
                --PO LEALES (COMPRAN MÁS DE UN TX SOLO DE LA MARCA)
                ,CASE WHEN PO_REPETIDORES_13 = 1 AND COMP_TX_1 + COMP_TX_2 + COMP_TX_3 + COMP_TX_4 + COMP_TX_5 + COMP_TX_6 + COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 = 0 THEN 1 ELSE 0 END PO_LEALES_13
                ,CASE WHEN PO_REPETIDORES_14 = 1 AND COMP_TX_2 + COMP_TX_3 + COMP_TX_4 + COMP_TX_5 + COMP_TX_6 + COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 = 0 THEN 1 ELSE 0 END PO_LEALES_14
                ,CASE WHEN PO_REPETIDORES_15 = 1 AND COMP_TX_3 + COMP_TX_4 + COMP_TX_5 + COMP_TX_6 + COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 = 0 THEN 1 ELSE 0 END PO_LEALES_15
                ,CASE WHEN PO_REPETIDORES_16 = 1 AND COMP_TX_4 + COMP_TX_5 + COMP_TX_6 + COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 = 0 THEN 1 ELSE 0 END PO_LEALES_16
                ,CASE WHEN PO_REPETIDORES_17 = 1 AND COMP_TX_5 + COMP_TX_6 + COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 = 0 THEN 1 ELSE 0 END PO_LEALES_17
                ,CASE WHEN PO_REPETIDORES_18 = 1 AND COMP_TX_6 + COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 = 0 THEN 1 ELSE 0 END PO_LEALES_18
                ,CASE WHEN PO_REPETIDORES_19 = 1 AND COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 + COMP_TX_18 = 0 THEN 1 ELSE 0 END PO_LEALES_19
                ,CASE WHEN PO_REPETIDORES_20 = 1 AND COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 + COMP_TX_18 + COMP_TX_19 = 0 THEN 1 ELSE 0 END PO_LEALES_20
                ,CASE WHEN PO_REPETIDORES_21 = 1 AND COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 + COMP_TX_18 + COMP_TX_19 + COMP_TX_20 = 0 THEN 1 ELSE 0 END PO_LEALES_21
                ,CASE WHEN PO_REPETIDORES_22 = 1 AND COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 + COMP_TX_18 + COMP_TX_19 + COMP_TX_20 + COMP_TX_21 = 0 THEN 1 ELSE 0 END PO_LEALES_22
                ,CASE WHEN PO_REPETIDORES_23 = 1 AND COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 + COMP_TX_18 + COMP_TX_19 + COMP_TX_20 + COMP_TX_21 + COMP_TX_22 = 0 THEN 1 ELSE 0 END PO_LEALES_23
                ,CASE WHEN PO_REPETIDORES_24 = 1 AND COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 + COMP_TX_18 + COMP_TX_19 + COMP_TX_20 + COMP_TX_21 + COMP_TX_22 + COMP_TX_23 = 0 THEN 1 ELSE 0 END PO_LEALES_24
                
                    --PO CON COMPRA  
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

                --REPETIDORES
                ,CASE WHEN PO_REPETIDORES_13 = 1 AND TX_13 > 0 THEN 1 ELSE 0 END REPETIDORES_13
                ,CASE WHEN PO_REPETIDORES_14 = 1 AND TX_14 > 0 THEN 1 ELSE 0 END REPETIDORES_14
                ,CASE WHEN PO_REPETIDORES_15 = 1 AND TX_15 > 0 THEN 1 ELSE 0 END REPETIDORES_15
                ,CASE WHEN PO_REPETIDORES_16 = 1 AND TX_16 > 0 THEN 1 ELSE 0 END REPETIDORES_16
                ,CASE WHEN PO_REPETIDORES_17 = 1 AND TX_17 > 0 THEN 1 ELSE 0 END REPETIDORES_17
                ,CASE WHEN PO_REPETIDORES_18 = 1 AND TX_18 > 0 THEN 1 ELSE 0 END REPETIDORES_18
                ,CASE WHEN PO_REPETIDORES_19 = 1 AND TX_19 > 0 THEN 1 ELSE 0 END REPETIDORES_19
                ,CASE WHEN PO_REPETIDORES_20 = 1 AND TX_20 > 0 THEN 1 ELSE 0 END REPETIDORES_20
                ,CASE WHEN PO_REPETIDORES_21 = 1 AND TX_21 > 0 THEN 1 ELSE 0 END REPETIDORES_21
                ,CASE WHEN PO_REPETIDORES_22 = 1 AND TX_22 > 0 THEN 1 ELSE 0 END REPETIDORES_22
                ,CASE WHEN PO_REPETIDORES_23 = 1 AND TX_23 > 0 THEN 1 ELSE 0 END REPETIDORES_23
                ,CASE WHEN PO_REPETIDORES_24 = 1 AND TX_24 > 0 THEN 1 ELSE 0 END REPETIDORES_24

                --LEALES
                ,CASE WHEN PO_LEALES_13 = 1 AND TX_13 > 0 AND COMP_TX_13 = 0 THEN 1 ELSE 0 END LEALES_13
                ,CASE WHEN PO_LEALES_14 = 1 AND TX_14 > 0 AND COMP_TX_14 = 0 THEN 1 ELSE 0 END LEALES_14
                ,CASE WHEN PO_LEALES_15 = 1 AND TX_15 > 0 AND COMP_TX_15 = 0 THEN 1 ELSE 0 END LEALES_15
                ,CASE WHEN PO_LEALES_16 = 1 AND TX_16 > 0 AND COMP_TX_16 = 0 THEN 1 ELSE 0 END LEALES_16
                ,CASE WHEN PO_LEALES_17 = 1 AND TX_17 > 0 AND COMP_TX_17 = 0 THEN 1 ELSE 0 END LEALES_17
                ,CASE WHEN PO_LEALES_18 = 1 AND TX_18 > 0 AND COMP_TX_18 = 0 THEN 1 ELSE 0 END LEALES_18
                ,CASE WHEN PO_LEALES_19 = 1 AND TX_19 > 0 AND COMP_TX_19 = 0 THEN 1 ELSE 0 END LEALES_19
                ,CASE WHEN PO_LEALES_20 = 1 AND TX_20 > 0 AND COMP_TX_20 = 0 THEN 1 ELSE 0 END LEALES_20
                ,CASE WHEN PO_LEALES_21 = 1 AND TX_21 > 0 AND COMP_TX_21 = 0 THEN 1 ELSE 0 END LEALES_21
                ,CASE WHEN PO_LEALES_22 = 1 AND TX_22 > 0 AND COMP_TX_22 = 0 THEN 1 ELSE 0 END LEALES_22
                ,CASE WHEN PO_LEALES_23 = 1 AND TX_23 > 0 AND COMP_TX_23 = 0 THEN 1 ELSE 0 END LEALES_23
                ,CASE WHEN PO_LEALES_24 = 1 AND TX_24 > 0 AND COMP_TX_24 = 0 THEN 1 ELSE 0 END LEALES_24
                
                    --VENTAS POS
                --VENTA PO RECOMPRA - VENTA DEL ÚLTIMO MES
                ,CASE WHEN PO_RECOMPRA_13 = 1 THEN VENTA_12 ELSE 0 END AS VENTA_PO_RECOMPRA_13
                ,CASE WHEN PO_RECOMPRA_14 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_PO_RECOMPRA_14
                ,CASE WHEN PO_RECOMPRA_15 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_PO_RECOMPRA_15
                ,CASE WHEN PO_RECOMPRA_16 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_PO_RECOMPRA_16
                ,CASE WHEN PO_RECOMPRA_17 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_PO_RECOMPRA_17
                ,CASE WHEN PO_RECOMPRA_18 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_PO_RECOMPRA_18
                ,CASE WHEN PO_RECOMPRA_19 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_PO_RECOMPRA_19
                ,CASE WHEN PO_RECOMPRA_20 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_PO_RECOMPRA_20
                ,CASE WHEN PO_RECOMPRA_21 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_PO_RECOMPRA_21
                ,CASE WHEN PO_RECOMPRA_22 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_PO_RECOMPRA_22
                ,CASE WHEN PO_RECOMPRA_23 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_PO_RECOMPRA_23
                ,CASE WHEN PO_RECOMPRA_24 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_PO_RECOMPRA_24

                --VENTA PO FID - VENTA DE LOS ÚLTIMOS 3 MESES
                ,CASE WHEN PO_FID_13 = 1 THEN VENTA_10 + VENTA_11 + VENTA_12 ELSE 0 END AS VENTA_PO_FID_13
                ,CASE WHEN PO_FID_14 = 1 THEN VENTA_11 + VENTA_12 + VENTA_13 ELSE 0 END AS VENTA_PO_FID_14
                ,CASE WHEN PO_FID_15 = 1 THEN VENTA_12 + VENTA_13 + VENTA_14 ELSE 0 END AS VENTA_PO_FID_15
                ,CASE WHEN PO_FID_16 = 1 THEN VENTA_13 + VENTA_14 + VENTA_15 ELSE 0 END AS VENTA_PO_FID_16
                ,CASE WHEN PO_FID_17 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_PO_FID_17
                ,CASE WHEN PO_FID_18 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_PO_FID_18
                ,CASE WHEN PO_FID_19 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_PO_FID_19
                ,CASE WHEN PO_FID_20 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_PO_FID_20
                ,CASE WHEN PO_FID_21 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_PO_FID_21
                ,CASE WHEN PO_FID_22 = 1 THEN VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_PO_FID_22
                ,CASE WHEN PO_FID_23 = 1 THEN VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_PO_FID_23
                ,CASE WHEN PO_FID_24 = 1 THEN VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_PO_FID_24

                --VENTA PO DOR - VENTA DE HACE 6 A 3 MESES
                ,CASE WHEN PO_DOR_13 = 1 THEN VENTA_7 + VENTA_8 + VENTA_9 ELSE 0 END AS VENTA_PO_DOR_13
                ,CASE WHEN PO_DOR_14 = 1 THEN VENTA_8 + VENTA_9 + VENTA_10 ELSE 0 END AS VENTA_PO_DOR_14
                ,CASE WHEN PO_DOR_15 = 1 THEN VENTA_9 + VENTA_10 + VENTA_11 ELSE 0 END AS VENTA_PO_DOR_15
                ,CASE WHEN PO_DOR_16 = 1 THEN VENTA_10 + VENTA_11 + VENTA_12 ELSE 0 END AS VENTA_PO_DOR_16
                ,CASE WHEN PO_DOR_17 = 1 THEN VENTA_11 + VENTA_12 + VENTA_13 ELSE 0 END AS VENTA_PO_DOR_17
                ,CASE WHEN PO_DOR_18 = 1 THEN VENTA_12 + VENTA_13 + VENTA_14 ELSE 0 END AS VENTA_PO_DOR_18
                ,CASE WHEN PO_DOR_19 = 1 THEN VENTA_13 + VENTA_14 + VENTA_15 ELSE 0 END AS VENTA_PO_DOR_19
                ,CASE WHEN PO_DOR_20 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_PO_DOR_20
                ,CASE WHEN PO_DOR_21 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_PO_DOR_21
                ,CASE WHEN PO_DOR_22 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_PO_DOR_22
                ,CASE WHEN PO_DOR_23 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_PO_DOR_23
                ,CASE WHEN PO_DOR_24 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_PO_DOR_24

                --VENTA PO PER - VENTA DE HACE 12 A 6 MESES
                ,CASE WHEN PO_PER_13 = 1 THEN VENTA_1 + VENTA_2 + VENTA_3 + VENTA_4 + VENTA_5 + VENTA_6 ELSE 0 END AS VENTA_PO_PER_13
                ,CASE WHEN PO_PER_14 = 1 THEN VENTA_2 + VENTA_3 + VENTA_4 + VENTA_5 + VENTA_6 + VENTA_7 ELSE 0 END AS VENTA_PO_PER_14
                ,CASE WHEN PO_PER_15 = 1 THEN VENTA_3 + VENTA_4 + VENTA_5 + VENTA_6 + VENTA_7 + VENTA_8 ELSE 0 END AS VENTA_PO_PER_15
                ,CASE WHEN PO_PER_16 = 1 THEN VENTA_4 + VENTA_5 + VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 ELSE 0 END AS VENTA_PO_PER_16
                ,CASE WHEN PO_PER_17 = 1 THEN VENTA_5 + VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 ELSE 0 END AS VENTA_PO_PER_17
                ,CASE WHEN PO_PER_18 = 1 THEN VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 ELSE 0 END AS VENTA_PO_PER_18
                ,CASE WHEN PO_PER_19 = 1 THEN VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 ELSE 0 END AS VENTA_PO_PER_19
                ,CASE WHEN PO_PER_20 = 1 THEN VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 ELSE 0 END AS VENTA_PO_PER_20
                ,CASE WHEN PO_PER_21 = 1 THEN VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 ELSE 0 END AS VENTA_PO_PER_21
                ,CASE WHEN PO_PER_22 = 1 THEN VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 ELSE 0 END AS VENTA_PO_PER_22
                ,CASE WHEN PO_PER_23 = 1 THEN VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_PO_PER_23
                ,CASE WHEN PO_PER_24 = 1 THEN VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_PO_PER_24
                
                --VENTA PO REC - VENTA DE DORMIDOS Y PERDIDOS
                ,CASE WHEN PO_REC_13 = 1 THEN VENTA_PO_DOR_13 + VENTA_PO_PER_13 ELSE 0 END AS VENTA_PO_REC_13
                ,CASE WHEN PO_REC_14 = 1 THEN VENTA_PO_DOR_14 + VENTA_PO_PER_14 ELSE 0 END AS VENTA_PO_REC_14
                ,CASE WHEN PO_REC_15 = 1 THEN VENTA_PO_DOR_15 + VENTA_PO_PER_15 ELSE 0 END AS VENTA_PO_REC_15
                ,CASE WHEN PO_REC_16 = 1 THEN VENTA_PO_DOR_16 + VENTA_PO_PER_16 ELSE 0 END AS VENTA_PO_REC_16
                ,CASE WHEN PO_REC_17 = 1 THEN VENTA_PO_DOR_17 + VENTA_PO_PER_17 ELSE 0 END AS VENTA_PO_REC_17
                ,CASE WHEN PO_REC_18 = 1 THEN VENTA_PO_DOR_18 + VENTA_PO_PER_18 ELSE 0 END AS VENTA_PO_REC_18
                ,CASE WHEN PO_REC_19 = 1 THEN VENTA_PO_DOR_19 + VENTA_PO_PER_19 ELSE 0 END AS VENTA_PO_REC_19
                ,CASE WHEN PO_REC_20 = 1 THEN VENTA_PO_DOR_20 + VENTA_PO_PER_20 ELSE 0 END AS VENTA_PO_REC_20
                ,CASE WHEN PO_REC_21 = 1 THEN VENTA_PO_DOR_21 + VENTA_PO_PER_21 ELSE 0 END AS VENTA_PO_REC_21
                ,CASE WHEN PO_REC_22 = 1 THEN VENTA_PO_DOR_22 + VENTA_PO_PER_22 ELSE 0 END AS VENTA_PO_REC_22
                ,CASE WHEN PO_REC_23 = 1 THEN VENTA_PO_DOR_23 + VENTA_PO_PER_23 ELSE 0 END AS VENTA_PO_REC_23
                ,CASE WHEN PO_REC_24 = 1 THEN VENTA_PO_DOR_24 + VENTA_PO_PER_24 ELSE 0 END AS VENTA_PO_REC_24

                --VENTA PO NUEVOS - VENTA GENERADA EN 12 MESES DE LA COMPETENCIA
                ,CASE WHEN PO_NUEVOS_13 = 1 THEN CAT_VENTA_1 + CAT_VENTA_2 + CAT_VENTA_3 + CAT_VENTA_4 + CAT_VENTA_5 + CAT_VENTA_6 + CAT_VENTA_7 + CAT_VENTA_8 + CAT_VENTA_9 + CAT_VENTA_10 + CAT_VENTA_11 + CAT_VENTA_12 ELSE 0 END AS VENTA_PO_NUEVOS_13
                ,CASE WHEN PO_NUEVOS_14 = 1 THEN CAT_VENTA_2 + CAT_VENTA_3 + CAT_VENTA_4 + CAT_VENTA_5 + CAT_VENTA_6 + CAT_VENTA_7 + CAT_VENTA_8 + CAT_VENTA_9 + CAT_VENTA_10 + CAT_VENTA_11 + CAT_VENTA_12 + CAT_VENTA_13 ELSE 0 END AS VENTA_PO_NUEVOS_14
                ,CASE WHEN PO_NUEVOS_15 = 1 THEN CAT_VENTA_3 + CAT_VENTA_4 + CAT_VENTA_5 + CAT_VENTA_6 + CAT_VENTA_7 + CAT_VENTA_8 + CAT_VENTA_9 + CAT_VENTA_10 + CAT_VENTA_11 + CAT_VENTA_12 + CAT_VENTA_13 + CAT_VENTA_14 ELSE 0 END AS VENTA_PO_NUEVOS_15
                ,CASE WHEN PO_NUEVOS_16 = 1 THEN CAT_VENTA_4 + CAT_VENTA_5 + CAT_VENTA_6 + CAT_VENTA_7 + CAT_VENTA_8 + CAT_VENTA_9 + CAT_VENTA_10 + CAT_VENTA_11 + CAT_VENTA_12 + CAT_VENTA_13 + CAT_VENTA_14 + CAT_VENTA_15 ELSE 0 END AS VENTA_PO_NUEVOS_16
                ,CASE WHEN PO_NUEVOS_17 = 1 THEN CAT_VENTA_5 + CAT_VENTA_6 + CAT_VENTA_7 + CAT_VENTA_8 + CAT_VENTA_9 + CAT_VENTA_10 + CAT_VENTA_11 + CAT_VENTA_12 + CAT_VENTA_13 + CAT_VENTA_14 + CAT_VENTA_15 + CAT_VENTA_16 ELSE 0 END AS VENTA_PO_NUEVOS_17
                ,CASE WHEN PO_NUEVOS_18 = 1 THEN CAT_VENTA_6 + CAT_VENTA_7 + CAT_VENTA_8 + CAT_VENTA_9 + CAT_VENTA_10 + CAT_VENTA_11 + CAT_VENTA_12 + CAT_VENTA_13 + CAT_VENTA_14 + CAT_VENTA_15 + CAT_VENTA_16 + CAT_VENTA_17 ELSE 0 END AS VENTA_PO_NUEVOS_18
                ,CASE WHEN PO_NUEVOS_19 = 1 THEN CAT_VENTA_7 + CAT_VENTA_8 + CAT_VENTA_9 + CAT_VENTA_10 + CAT_VENTA_11 + CAT_VENTA_12 + CAT_VENTA_13 + CAT_VENTA_14 + CAT_VENTA_15 + CAT_VENTA_16 + CAT_VENTA_17 + CAT_VENTA_18 ELSE 0 END AS VENTA_PO_NUEVOS_19
                ,CASE WHEN PO_NUEVOS_20 = 1 THEN CAT_VENTA_8 + CAT_VENTA_9 + CAT_VENTA_10 + CAT_VENTA_11 + CAT_VENTA_12 + CAT_VENTA_13 + CAT_VENTA_14 + CAT_VENTA_15 + CAT_VENTA_16 + CAT_VENTA_17 + CAT_VENTA_18 + CAT_VENTA_19 ELSE 0 END AS VENTA_PO_NUEVOS_20
                ,CASE WHEN PO_NUEVOS_21 = 1 THEN CAT_VENTA_9 + CAT_VENTA_10 + CAT_VENTA_11 + CAT_VENTA_12 + CAT_VENTA_13 + CAT_VENTA_14 + CAT_VENTA_15 + CAT_VENTA_16 + CAT_VENTA_17 + CAT_VENTA_18 + CAT_VENTA_19 + CAT_VENTA_20 ELSE 0 END AS VENTA_PO_NUEVOS_21
                ,CASE WHEN PO_NUEVOS_22 = 1 THEN CAT_VENTA_10 + CAT_VENTA_11 + CAT_VENTA_12 + CAT_VENTA_13 + CAT_VENTA_14 + CAT_VENTA_15 + CAT_VENTA_16 + CAT_VENTA_17 + CAT_VENTA_18 + CAT_VENTA_19 + CAT_VENTA_20 + CAT_VENTA_21 ELSE 0 END AS VENTA_PO_NUEVOS_22
                ,CASE WHEN PO_NUEVOS_23 = 1 THEN CAT_VENTA_11 + CAT_VENTA_12 + CAT_VENTA_13 + CAT_VENTA_14 + CAT_VENTA_15 + CAT_VENTA_16 + CAT_VENTA_17 + CAT_VENTA_18 + CAT_VENTA_19 + CAT_VENTA_20 + CAT_VENTA_21 + CAT_VENTA_22 ELSE 0 END AS VENTA_PO_NUEVOS_23
                ,CASE WHEN PO_NUEVOS_24 = 1 THEN CAT_VENTA_12 + CAT_VENTA_13 + CAT_VENTA_14 + CAT_VENTA_15 + CAT_VENTA_16 + CAT_VENTA_17 + CAT_VENTA_18 + CAT_VENTA_19 + CAT_VENTA_20 + CAT_VENTA_21 + CAT_VENTA_22 + CAT_VENTA_23 ELSE 0 END AS VENTA_PO_NUEVOS_24
                
                --VENTA PO REPETIDORES - VENTA GENERADA EN 12 MESES
                ,CASE WHEN PO_REPETIDORES_13 = 1 THEN VENTA_1 + VENTA_2 + VENTA_3 + VENTA_4 + VENTA_5 + VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 ELSE 0 END AS VENTA_PO_REPETIDORES_13
                ,CASE WHEN PO_REPETIDORES_14 = 1 THEN VENTA_2 + VENTA_3 + VENTA_4 + VENTA_5 + VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 ELSE 0 END AS VENTA_PO_REPETIDORES_14
                ,CASE WHEN PO_REPETIDORES_15 = 1 THEN VENTA_3 + VENTA_4 + VENTA_5 + VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 ELSE 0 END AS VENTA_PO_REPETIDORES_15
                ,CASE WHEN PO_REPETIDORES_16 = 1 THEN VENTA_4 + VENTA_5 + VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 ELSE 0 END AS VENTA_PO_REPETIDORES_16
                ,CASE WHEN PO_REPETIDORES_17 = 1 THEN VENTA_5 + VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_PO_REPETIDORES_17
                ,CASE WHEN PO_REPETIDORES_18 = 1 THEN VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_PO_REPETIDORES_18
                ,CASE WHEN PO_REPETIDORES_19 = 1 THEN VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_PO_REPETIDORES_19
                ,CASE WHEN PO_REPETIDORES_20 = 1 THEN VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_PO_REPETIDORES_20
                ,CASE WHEN PO_REPETIDORES_21 = 1 THEN VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_PO_REPETIDORES_21
                ,CASE WHEN PO_REPETIDORES_22 = 1 THEN VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_PO_REPETIDORES_22
                ,CASE WHEN PO_REPETIDORES_23 = 1 THEN VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_PO_REPETIDORES_23
                ,CASE WHEN PO_REPETIDORES_24 = 1 THEN VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_PO_REPETIDORES_24
                
                --VENTA PO LEALES
                ,CASE WHEN PO_LEALES_13 = 1 THEN VENTA_1 + VENTA_2 + VENTA_3 + VENTA_4 + VENTA_5 + VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 ELSE 0 END AS VENTA_PO_LEALES_13
                ,CASE WHEN PO_LEALES_14 = 1 THEN VENTA_2 + VENTA_3 + VENTA_4 + VENTA_5 + VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 ELSE 0 END AS VENTA_PO_LEALES_14
                ,CASE WHEN PO_LEALES_15 = 1 THEN VENTA_3 + VENTA_4 + VENTA_5 + VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 ELSE 0 END AS VENTA_PO_LEALES_15
                ,CASE WHEN PO_LEALES_16 = 1 THEN VENTA_4 + VENTA_5 + VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 ELSE 0 END AS VENTA_PO_LEALES_16
                ,CASE WHEN PO_LEALES_17 = 1 THEN VENTA_5 + VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_PO_LEALES_17
                ,CASE WHEN PO_LEALES_18 = 1 THEN VENTA_6 + VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_PO_LEALES_18
                ,CASE WHEN PO_LEALES_19 = 1 THEN VENTA_7 + VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_PO_LEALES_19
                ,CASE WHEN PO_LEALES_20 = 1 THEN VENTA_8 + VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_PO_LEALES_20
                ,CASE WHEN PO_LEALES_21 = 1 THEN VENTA_9 + VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_PO_LEALES_21
                ,CASE WHEN PO_LEALES_22 = 1 THEN VENTA_10 + VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_PO_LEALES_22
                ,CASE WHEN PO_LEALES_23 = 1 THEN VENTA_11 + VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_PO_LEALES_23
                ,CASE WHEN PO_LEALES_24 = 1 THEN VENTA_12 + VENTA_13 + VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_PO_LEALES_24

                    --VENTAS POS CON COMPRA
                --VENTA RECOMPRA - VENTA DE CLIENTES QUE COMPRARON EL MES ANTERIOR Y TAMBIÉN EL MES ACTUAL
                ,CASE WHEN RECOMPRA_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_RECOMPRA_13
                ,CASE WHEN RECOMPRA_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_RECOMPRA_14
                ,CASE WHEN RECOMPRA_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_RECOMPRA_15
                ,CASE WHEN RECOMPRA_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_RECOMPRA_16
                ,CASE WHEN RECOMPRA_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_RECOMPRA_17
                ,CASE WHEN RECOMPRA_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_RECOMPRA_18
                ,CASE WHEN RECOMPRA_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_RECOMPRA_19
                ,CASE WHEN RECOMPRA_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_RECOMPRA_20
                ,CASE WHEN RECOMPRA_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_RECOMPRA_21
                ,CASE WHEN RECOMPRA_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_RECOMPRA_22
                ,CASE WHEN RECOMPRA_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_RECOMPRA_23
                ,CASE WHEN RECOMPRA_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_RECOMPRA_24
                
                --VENTA FID - VENTA DE CLIENTES QUE CUMPLEN CON EL PO FID Y COMPRARON EN EL MES CORRESPONDIENTE
                ,CASE WHEN FID_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_FID_13
                ,CASE WHEN FID_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_FID_14
                ,CASE WHEN FID_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_FID_15
                ,CASE WHEN FID_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_FID_16
                ,CASE WHEN FID_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_FID_17
                ,CASE WHEN FID_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_FID_18
                ,CASE WHEN FID_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_FID_19
                ,CASE WHEN FID_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_FID_20
                ,CASE WHEN FID_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_FID_21
                ,CASE WHEN FID_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_FID_22
                ,CASE WHEN FID_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_FID_23
                ,CASE WHEN FID_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_FID_24

                --VENTA DOR
                ,CASE WHEN DOR_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_DOR_13
                ,CASE WHEN DOR_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_DOR_14
                ,CASE WHEN DOR_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_DOR_15
                ,CASE WHEN DOR_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_DOR_16
                ,CASE WHEN DOR_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_DOR_17
                ,CASE WHEN DOR_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_DOR_18
                ,CASE WHEN DOR_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_DOR_19
                ,CASE WHEN DOR_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_DOR_20
                ,CASE WHEN DOR_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_DOR_21
                ,CASE WHEN DOR_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_DOR_22
                ,CASE WHEN DOR_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_DOR_23
                ,CASE WHEN DOR_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_DOR_24

                --VENTA PER
                ,CASE WHEN PER_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_PER_13
                ,CASE WHEN PER_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_PER_14
                ,CASE WHEN PER_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_PER_15
                ,CASE WHEN PER_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_PER_16
                ,CASE WHEN PER_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_PER_17
                ,CASE WHEN PER_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_PER_18
                ,CASE WHEN PER_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_PER_19
                ,CASE WHEN PER_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_PER_20
                ,CASE WHEN PER_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_PER_21
                ,CASE WHEN PER_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_PER_22
                ,CASE WHEN PER_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_PER_23
                ,CASE WHEN PER_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_PER_24

                --VENTA REC
                ,CASE WHEN REC_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_REC_13
                ,CASE WHEN REC_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_REC_14
                ,CASE WHEN REC_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_REC_15
                ,CASE WHEN REC_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_REC_16
                ,CASE WHEN REC_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_REC_17
                ,CASE WHEN REC_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_REC_18
                ,CASE WHEN REC_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_REC_19
                ,CASE WHEN REC_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_REC_20
                ,CASE WHEN REC_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_REC_21
                ,CASE WHEN REC_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_REC_22
                ,CASE WHEN REC_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_REC_23
                ,CASE WHEN REC_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_REC_24

                --VENTA NUEVOS
                ,CASE WHEN NUEVOS_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_NUEVOS_13
                ,CASE WHEN NUEVOS_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_NUEVOS_14
                ,CASE WHEN NUEVOS_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_NUEVOS_15
                ,CASE WHEN NUEVOS_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_NUEVOS_16
                ,CASE WHEN NUEVOS_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_NUEVOS_17
                ,CASE WHEN NUEVOS_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_NUEVOS_18
                ,CASE WHEN NUEVOS_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_NUEVOS_19
                ,CASE WHEN NUEVOS_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_NUEVOS_20
                ,CASE WHEN NUEVOS_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_NUEVOS_21
                ,CASE WHEN NUEVOS_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_NUEVOS_22
                ,CASE WHEN NUEVOS_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_NUEVOS_23
                ,CASE WHEN NUEVOS_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_NUEVOS_24
                
                --VENTA REPETIDORES
                ,CASE WHEN REPETIDORES_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_REPETIDORES_13
                ,CASE WHEN REPETIDORES_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_REPETIDORES_14
                ,CASE WHEN REPETIDORES_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_REPETIDORES_15
                ,CASE WHEN REPETIDORES_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_REPETIDORES_16
                ,CASE WHEN REPETIDORES_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_REPETIDORES_17
                ,CASE WHEN REPETIDORES_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_REPETIDORES_18
                ,CASE WHEN REPETIDORES_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_REPETIDORES_19
                ,CASE WHEN REPETIDORES_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_REPETIDORES_20
                ,CASE WHEN REPETIDORES_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_REPETIDORES_21
                ,CASE WHEN REPETIDORES_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_REPETIDORES_22
                ,CASE WHEN REPETIDORES_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_REPETIDORES_23
                ,CASE WHEN REPETIDORES_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_REPETIDORES_24
                
                --VENTA LEALES
                ,CASE WHEN LEALES_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_LEALES_13
                ,CASE WHEN LEALES_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_LEALES_14
                ,CASE WHEN LEALES_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_LEALES_15
                ,CASE WHEN LEALES_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_LEALES_16
                ,CASE WHEN LEALES_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_LEALES_17
                ,CASE WHEN LEALES_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_LEALES_18
                ,CASE WHEN LEALES_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_LEALES_19
                ,CASE WHEN LEALES_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_LEALES_20
                ,CASE WHEN LEALES_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_LEALES_21
                ,CASE WHEN LEALES_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_LEALES_22
                ,CASE WHEN LEALES_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_LEALES_23
                ,CASE WHEN LEALES_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_LEALES_24

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
                
                --VENTA
                ,SUM(VENTA_13) VENTA_13
                ,SUM(VENTA_14) VENTA_14
                ,SUM(VENTA_15) VENTA_15
                ,SUM(VENTA_16) VENTA_16
                ,SUM(VENTA_17) VENTA_17
                ,SUM(VENTA_18) VENTA_18
                ,SUM(VENTA_19) VENTA_19
                ,SUM(VENTA_20) VENTA_20
                ,SUM(VENTA_21) VENTA_21
                ,SUM(VENTA_22) VENTA_22
                ,SUM(VENTA_23) VENTA_23
                ,SUM(VENTA_24) VENTA_24
                
                --TX
                ,SUM(TX_13) TX_13
                ,SUM(TX_14) TX_14
                ,SUM(TX_15) TX_15
                ,SUM(TX_16) TX_16
                ,SUM(TX_17) TX_17
                ,SUM(TX_18) TX_18
                ,SUM(TX_19) TX_19
                ,SUM(TX_20) TX_20
                ,SUM(TX_21) TX_21
                ,SUM(TX_22) TX_22
                ,SUM(TX_23) TX_23
                ,SUM(TX_24) TX_24
            --       
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
                
                --PO_REPETIDORES
                ,SUM(PO_REPETIDORES_13) PO_REPETIDORES_13
                ,SUM(PO_REPETIDORES_14) PO_REPETIDORES_14
                ,SUM(PO_REPETIDORES_15) PO_REPETIDORES_15
                ,SUM(PO_REPETIDORES_16) PO_REPETIDORES_16
                ,SUM(PO_REPETIDORES_17) PO_REPETIDORES_17
                ,SUM(PO_REPETIDORES_18) PO_REPETIDORES_18
                ,SUM(PO_REPETIDORES_19) PO_REPETIDORES_19
                ,SUM(PO_REPETIDORES_20) PO_REPETIDORES_20
                ,SUM(PO_REPETIDORES_21) PO_REPETIDORES_21
                ,SUM(PO_REPETIDORES_22) PO_REPETIDORES_22
                ,SUM(PO_REPETIDORES_23) PO_REPETIDORES_23
                ,SUM(PO_REPETIDORES_24) PO_REPETIDORES_24

                --PO_REPETIDORES
                ,SUM(PO_LEALES_13) PO_LEALES_13
                ,SUM(PO_LEALES_14) PO_LEALES_14
                ,SUM(PO_LEALES_15) PO_LEALES_15
                ,SUM(PO_LEALES_16) PO_LEALES_16
                ,SUM(PO_LEALES_17) PO_LEALES_17
                ,SUM(PO_LEALES_18) PO_LEALES_18
                ,SUM(PO_LEALES_19) PO_LEALES_19
                ,SUM(PO_LEALES_20) PO_LEALES_20
                ,SUM(PO_LEALES_21) PO_LEALES_21
                ,SUM(PO_LEALES_22) PO_LEALES_22
                ,SUM(PO_LEALES_23) PO_LEALES_23
                ,SUM(PO_LEALES_24) PO_LEALES_24
                
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
                
                --REPETIDORES
                ,SUM(REPETIDORES_13) REPETIDORES_13
                ,SUM(REPETIDORES_14) REPETIDORES_14
                ,SUM(REPETIDORES_15) REPETIDORES_15
                ,SUM(REPETIDORES_16) REPETIDORES_16
                ,SUM(REPETIDORES_17) REPETIDORES_17
                ,SUM(REPETIDORES_18) REPETIDORES_18
                ,SUM(REPETIDORES_19) REPETIDORES_19
                ,SUM(REPETIDORES_20) REPETIDORES_20
                ,SUM(REPETIDORES_21) REPETIDORES_21
                ,SUM(REPETIDORES_22) REPETIDORES_22
                ,SUM(REPETIDORES_23) REPETIDORES_23
                ,SUM(REPETIDORES_24) REPETIDORES_24

                --LEALES
                ,SUM(LEALES_13) LEALES_13
                ,SUM(LEALES_14) LEALES_14
                ,SUM(LEALES_15) LEALES_15
                ,SUM(LEALES_16) LEALES_16
                ,SUM(LEALES_17) LEALES_17
                ,SUM(LEALES_18) LEALES_18
                ,SUM(LEALES_19) LEALES_19
                ,SUM(LEALES_20) LEALES_20
                ,SUM(LEALES_21) LEALES_21
                ,SUM(LEALES_22) LEALES_22
                ,SUM(LEALES_23) LEALES_23
                ,SUM(LEALES_24) LEALES_24

                --VENTA PO_RECOMPRA
                ,SUM(VENTA_PO_RECOMPRA_13) VENTA_PO_RECOMPRA_13
                ,SUM(VENTA_PO_RECOMPRA_14) VENTA_PO_RECOMPRA_14
                ,SUM(VENTA_PO_RECOMPRA_15) VENTA_PO_RECOMPRA_15
                ,SUM(VENTA_PO_RECOMPRA_16) VENTA_PO_RECOMPRA_16
                ,SUM(VENTA_PO_RECOMPRA_17) VENTA_PO_RECOMPRA_17
                ,SUM(VENTA_PO_RECOMPRA_18) VENTA_PO_RECOMPRA_18
                ,SUM(VENTA_PO_RECOMPRA_19) VENTA_PO_RECOMPRA_19
                ,SUM(VENTA_PO_RECOMPRA_20) VENTA_PO_RECOMPRA_20
                ,SUM(VENTA_PO_RECOMPRA_21) VENTA_PO_RECOMPRA_21
                ,SUM(VENTA_PO_RECOMPRA_22) VENTA_PO_RECOMPRA_22
                ,SUM(VENTA_PO_RECOMPRA_23) VENTA_PO_RECOMPRA_23
                ,SUM(VENTA_PO_RECOMPRA_24) VENTA_PO_RECOMPRA_24

                --VENTA PO_FID
                ,SUM(VENTA_PO_FID_13) VENTA_PO_FID_13
                ,SUM(VENTA_PO_FID_14) VENTA_PO_FID_14
                ,SUM(VENTA_PO_FID_15) VENTA_PO_FID_15
                ,SUM(VENTA_PO_FID_16) VENTA_PO_FID_16
                ,SUM(VENTA_PO_FID_17) VENTA_PO_FID_17
                ,SUM(VENTA_PO_FID_18) VENTA_PO_FID_18
                ,SUM(VENTA_PO_FID_19) VENTA_PO_FID_19
                ,SUM(VENTA_PO_FID_20) VENTA_PO_FID_20
                ,SUM(VENTA_PO_FID_21) VENTA_PO_FID_21
                ,SUM(VENTA_PO_FID_22) VENTA_PO_FID_22
                ,SUM(VENTA_PO_FID_23) VENTA_PO_FID_23
                ,SUM(VENTA_PO_FID_24) VENTA_PO_FID_24
                
                --VENTA PO_DOR
                ,SUM(VENTA_PO_DOR_13) VENTA_PO_DOR_13
                ,SUM(VENTA_PO_DOR_14) VENTA_PO_DOR_14
                ,SUM(VENTA_PO_DOR_15) VENTA_PO_DOR_15
                ,SUM(VENTA_PO_DOR_16) VENTA_PO_DOR_16
                ,SUM(VENTA_PO_DOR_17) VENTA_PO_DOR_17
                ,SUM(VENTA_PO_DOR_18) VENTA_PO_DOR_18
                ,SUM(VENTA_PO_DOR_19) VENTA_PO_DOR_19
                ,SUM(VENTA_PO_DOR_20) VENTA_PO_DOR_20
                ,SUM(VENTA_PO_DOR_21) VENTA_PO_DOR_21
                ,SUM(VENTA_PO_DOR_22) VENTA_PO_DOR_22
                ,SUM(VENTA_PO_DOR_23) VENTA_PO_DOR_23
                ,SUM(VENTA_PO_DOR_24) VENTA_PO_DOR_24
                
                --VENTA PO_PER
                ,SUM(VENTA_PO_PER_13) VENTA_PO_PER_13
                ,SUM(VENTA_PO_PER_14) VENTA_PO_PER_14
                ,SUM(VENTA_PO_PER_15) VENTA_PO_PER_15
                ,SUM(VENTA_PO_PER_16) VENTA_PO_PER_16
                ,SUM(VENTA_PO_PER_17) VENTA_PO_PER_17
                ,SUM(VENTA_PO_PER_18) VENTA_PO_PER_18
                ,SUM(VENTA_PO_PER_19) VENTA_PO_PER_19
                ,SUM(VENTA_PO_PER_20) VENTA_PO_PER_20
                ,SUM(VENTA_PO_PER_21) VENTA_PO_PER_21
                ,SUM(VENTA_PO_PER_22) VENTA_PO_PER_22
                ,SUM(VENTA_PO_PER_23) VENTA_PO_PER_23
                ,SUM(VENTA_PO_PER_24) VENTA_PO_PER_24
                
                --VENTA PO_REC
                ,SUM(VENTA_PO_REC_13) VENTA_PO_REC_13
                ,SUM(VENTA_PO_REC_14) VENTA_PO_REC_14
                ,SUM(VENTA_PO_REC_15) VENTA_PO_REC_15
                ,SUM(VENTA_PO_REC_16) VENTA_PO_REC_16
                ,SUM(VENTA_PO_REC_17) VENTA_PO_REC_17
                ,SUM(VENTA_PO_REC_18) VENTA_PO_REC_18
                ,SUM(VENTA_PO_REC_19) VENTA_PO_REC_19
                ,SUM(VENTA_PO_REC_20) VENTA_PO_REC_20
                ,SUM(VENTA_PO_REC_21) VENTA_PO_REC_21
                ,SUM(VENTA_PO_REC_22) VENTA_PO_REC_22
                ,SUM(VENTA_PO_REC_23) VENTA_PO_REC_23
                ,SUM(VENTA_PO_REC_24) VENTA_PO_REC_24
                
                --VENTA PO_NUEVOS
                ,SUM(VENTA_PO_NUEVOS_13) VENTA_PO_NUEVOS_13
                ,SUM(VENTA_PO_NUEVOS_14) VENTA_PO_NUEVOS_14
                ,SUM(VENTA_PO_NUEVOS_15) VENTA_PO_NUEVOS_15
                ,SUM(VENTA_PO_NUEVOS_16) VENTA_PO_NUEVOS_16
                ,SUM(VENTA_PO_NUEVOS_17) VENTA_PO_NUEVOS_17
                ,SUM(VENTA_PO_NUEVOS_18) VENTA_PO_NUEVOS_18
                ,SUM(VENTA_PO_NUEVOS_19) VENTA_PO_NUEVOS_19
                ,SUM(VENTA_PO_NUEVOS_20) VENTA_PO_NUEVOS_20
                ,SUM(VENTA_PO_NUEVOS_21) VENTA_PO_NUEVOS_21
                ,SUM(VENTA_PO_NUEVOS_22) VENTA_PO_NUEVOS_22
                ,SUM(VENTA_PO_NUEVOS_23) VENTA_PO_NUEVOS_23
                ,SUM(VENTA_PO_NUEVOS_24) VENTA_PO_NUEVOS_24

                --VENTA PO_REPETIDORES
                ,SUM(VENTA_PO_REPETIDORES_13) VENTA_PO_REPETIDORES_13
                ,SUM(VENTA_PO_REPETIDORES_14) VENTA_PO_REPETIDORES_14
                ,SUM(VENTA_PO_REPETIDORES_15) VENTA_PO_REPETIDORES_15
                ,SUM(VENTA_PO_REPETIDORES_16) VENTA_PO_REPETIDORES_16
                ,SUM(VENTA_PO_REPETIDORES_17) VENTA_PO_REPETIDORES_17
                ,SUM(VENTA_PO_REPETIDORES_18) VENTA_PO_REPETIDORES_18
                ,SUM(VENTA_PO_REPETIDORES_19) VENTA_PO_REPETIDORES_19
                ,SUM(VENTA_PO_REPETIDORES_20) VENTA_PO_REPETIDORES_20
                ,SUM(VENTA_PO_REPETIDORES_21) VENTA_PO_REPETIDORES_21
                ,SUM(VENTA_PO_REPETIDORES_22) VENTA_PO_REPETIDORES_22
                ,SUM(VENTA_PO_REPETIDORES_23) VENTA_PO_REPETIDORES_23
                ,SUM(VENTA_PO_REPETIDORES_24) VENTA_PO_REPETIDORES_24

                --VENTA PO_LEALES
                ,SUM(VENTA_PO_LEALES_13) VENTA_PO_LEALES_13
                ,SUM(VENTA_PO_LEALES_14) VENTA_PO_LEALES_14
                ,SUM(VENTA_PO_LEALES_15) VENTA_PO_LEALES_15
                ,SUM(VENTA_PO_LEALES_16) VENTA_PO_LEALES_16
                ,SUM(VENTA_PO_LEALES_17) VENTA_PO_LEALES_17
                ,SUM(VENTA_PO_LEALES_18) VENTA_PO_LEALES_18
                ,SUM(VENTA_PO_LEALES_19) VENTA_PO_LEALES_19
                ,SUM(VENTA_PO_LEALES_20) VENTA_PO_LEALES_20
                ,SUM(VENTA_PO_LEALES_21) VENTA_PO_LEALES_21
                ,SUM(VENTA_PO_LEALES_22) VENTA_PO_LEALES_22
                ,SUM(VENTA_PO_LEALES_23) VENTA_PO_LEALES_23
                ,SUM(VENTA_PO_LEALES_24) VENTA_PO_LEALES_24

                --VENTA RECOMPRA
                ,SUM(VENTA_RECOMPRA_13) VENTA_RECOMPRA_13
                ,SUM(VENTA_RECOMPRA_14) VENTA_RECOMPRA_14
                ,SUM(VENTA_RECOMPRA_15) VENTA_RECOMPRA_15
                ,SUM(VENTA_RECOMPRA_16) VENTA_RECOMPRA_16
                ,SUM(VENTA_RECOMPRA_17) VENTA_RECOMPRA_17
                ,SUM(VENTA_RECOMPRA_18) VENTA_RECOMPRA_18
                ,SUM(VENTA_RECOMPRA_19) VENTA_RECOMPRA_19
                ,SUM(VENTA_RECOMPRA_20) VENTA_RECOMPRA_20
                ,SUM(VENTA_RECOMPRA_21) VENTA_RECOMPRA_21
                ,SUM(VENTA_RECOMPRA_22) VENTA_RECOMPRA_22
                ,SUM(VENTA_RECOMPRA_23) VENTA_RECOMPRA_23
                ,SUM(VENTA_RECOMPRA_24) VENTA_RECOMPRA_24

                --VENTA FID
                ,SUM(VENTA_FID_13) VENTA_FID_13
                ,SUM(VENTA_FID_14) VENTA_FID_14
                ,SUM(VENTA_FID_15) VENTA_FID_15
                ,SUM(VENTA_FID_16) VENTA_FID_16
                ,SUM(VENTA_FID_17) VENTA_FID_17
                ,SUM(VENTA_FID_18) VENTA_FID_18
                ,SUM(VENTA_FID_19) VENTA_FID_19
                ,SUM(VENTA_FID_20) VENTA_FID_20
                ,SUM(VENTA_FID_21) VENTA_FID_21
                ,SUM(VENTA_FID_22) VENTA_FID_22
                ,SUM(VENTA_FID_23) VENTA_FID_23
                ,SUM(VENTA_FID_24) VENTA_FID_24

                --VENTA DOR
                ,SUM(VENTA_DOR_13) VENTA_DOR_13
                ,SUM(VENTA_DOR_14) VENTA_DOR_14
                ,SUM(VENTA_DOR_15) VENTA_DOR_15
                ,SUM(VENTA_DOR_16) VENTA_DOR_16
                ,SUM(VENTA_DOR_17) VENTA_DOR_17
                ,SUM(VENTA_DOR_18) VENTA_DOR_18
                ,SUM(VENTA_DOR_19) VENTA_DOR_19
                ,SUM(VENTA_DOR_20) VENTA_DOR_20
                ,SUM(VENTA_DOR_21) VENTA_DOR_21
                ,SUM(VENTA_DOR_22) VENTA_DOR_22
                ,SUM(VENTA_DOR_23) VENTA_DOR_23
                ,SUM(VENTA_DOR_24) VENTA_DOR_24
                
                --VENTA_PER
                ,SUM(VENTA_PER_13) VENTA_PER_13
                ,SUM(VENTA_PER_14) VENTA_PER_14
                ,SUM(VENTA_PER_15) VENTA_PER_15
                ,SUM(VENTA_PER_16) VENTA_PER_16
                ,SUM(VENTA_PER_17) VENTA_PER_17
                ,SUM(VENTA_PER_18) VENTA_PER_18
                ,SUM(VENTA_PER_19) VENTA_PER_19
                ,SUM(VENTA_PER_20) VENTA_PER_20
                ,SUM(VENTA_PER_21) VENTA_PER_21
                ,SUM(VENTA_PER_22) VENTA_PER_22
                ,SUM(VENTA_PER_23) VENTA_PER_23
                ,SUM(VENTA_PER_24) VENTA_PER_24
                
                --VENTA_REC
                ,SUM(VENTA_REC_13) VENTA_REC_13
                ,SUM(VENTA_REC_14) VENTA_REC_14
                ,SUM(VENTA_REC_15) VENTA_REC_15
                ,SUM(VENTA_REC_16) VENTA_REC_16
                ,SUM(VENTA_REC_17) VENTA_REC_17
                ,SUM(VENTA_REC_18) VENTA_REC_18
                ,SUM(VENTA_REC_19) VENTA_REC_19
                ,SUM(VENTA_REC_20) VENTA_REC_20
                ,SUM(VENTA_REC_21) VENTA_REC_21
                ,SUM(VENTA_REC_22) VENTA_REC_22
                ,SUM(VENTA_REC_23) VENTA_REC_23
                ,SUM(VENTA_REC_24) VENTA_REC_24
                
                --VENTA NUEVOS
                ,SUM(VENTA_NUEVOS_13) VENTA_NUEVOS_13
                ,SUM(VENTA_NUEVOS_14) VENTA_NUEVOS_14
                ,SUM(VENTA_NUEVOS_15) VENTA_NUEVOS_15
                ,SUM(VENTA_NUEVOS_16) VENTA_NUEVOS_16
                ,SUM(VENTA_NUEVOS_17) VENTA_NUEVOS_17
                ,SUM(VENTA_NUEVOS_18) VENTA_NUEVOS_18
                ,SUM(VENTA_NUEVOS_19) VENTA_NUEVOS_19
                ,SUM(VENTA_NUEVOS_20) VENTA_NUEVOS_20
                ,SUM(VENTA_NUEVOS_21) VENTA_NUEVOS_21
                ,SUM(VENTA_NUEVOS_22) VENTA_NUEVOS_22
                ,SUM(VENTA_NUEVOS_23) VENTA_NUEVOS_23
                ,SUM(VENTA_NUEVOS_24) VENTA_NUEVOS_24

                --VENTA REPETIDORES
                ,SUM(VENTA_REPETIDORES_13) VENTA_REPETIDORES_13
                ,SUM(VENTA_REPETIDORES_14) VENTA_REPETIDORES_14
                ,SUM(VENTA_REPETIDORES_15) VENTA_REPETIDORES_15
                ,SUM(VENTA_REPETIDORES_16) VENTA_REPETIDORES_16
                ,SUM(VENTA_REPETIDORES_17) VENTA_REPETIDORES_17
                ,SUM(VENTA_REPETIDORES_18) VENTA_REPETIDORES_18
                ,SUM(VENTA_REPETIDORES_19) VENTA_REPETIDORES_19
                ,SUM(VENTA_REPETIDORES_20) VENTA_REPETIDORES_20
                ,SUM(VENTA_REPETIDORES_21) VENTA_REPETIDORES_21
                ,SUM(VENTA_REPETIDORES_22) VENTA_REPETIDORES_22
                ,SUM(VENTA_REPETIDORES_23) VENTA_REPETIDORES_23
                ,SUM(VENTA_REPETIDORES_24) VENTA_REPETIDORES_24

                --VENTA LEALES
                ,SUM(VENTA_LEALES_13) VENTA_LEALES_13
                ,SUM(VENTA_LEALES_14) VENTA_LEALES_14
                ,SUM(VENTA_LEALES_15) VENTA_LEALES_15
                ,SUM(VENTA_LEALES_16) VENTA_LEALES_16
                ,SUM(VENTA_LEALES_17) VENTA_LEALES_17
                ,SUM(VENTA_LEALES_18) VENTA_LEALES_18
                ,SUM(VENTA_LEALES_19) VENTA_LEALES_19
                ,SUM(VENTA_LEALES_20) VENTA_LEALES_20
                ,SUM(VENTA_LEALES_21) VENTA_LEALES_21
                ,SUM(VENTA_LEALES_22) VENTA_LEALES_22
                ,SUM(VENTA_LEALES_23) VENTA_LEALES_23
                ,SUM(VENTA_LEALES_24) VENTA_LEALES_24

                FROM __INDICADORES
                GROUP BY 1,2
            )
            ,__RESUMEN_SEGMENTOS AS (
                SELECT PROVEEDOR, MARCA, 13 IND_MES, CLIENTES_13 AS CLIENTES, VENTA_13 AS VENTA, TX_13 AS TX, PO_RECOMPRA_13 AS PO_RECOMPRA, RECOMPRA_13 AS RECOMPRA, PO_FID_13 AS PO_FID, FID_13 AS FID, PO_DOR_13 AS PO_DOR, DOR_13 AS DOR, PO_PER_13 AS PO_PER, PER_13 AS PER, PO_REC_13 AS PO_REC, REC_13 AS REC, PO_NUEVOS_13 AS PO_NUEVOS, NUEVOS_13 AS NUEVOS, PO_REPETIDORES_13 AS PO_REPETIDORES, REPETIDORES_13 AS REPETIDORES, PO_LEALES_13 AS PO_LEALES, LEALES_13 AS LEALES, VENTA_PO_RECOMPRA_13 AS VENTA_PO_RECOMPRA, VENTA_RECOMPRA_13 AS VENTA_RECOMPRA, VENTA_PO_FID_13 AS VENTA_PO_FID, VENTA_FID_13 AS VENTA_FID, VENTA_PO_DOR_13 AS VENTA_PO_DOR, VENTA_DOR_13 AS VENTA_DOR, VENTA_PO_PER_13 AS VENTA_PO_PER, VENTA_PER_13 AS VENTA_PER, VENTA_PO_REC_13 AS VENTA_PO_REC, VENTA_REC_13 AS VENTA_REC, VENTA_PO_NUEVOS_13 AS VENTA_PO_NUEVOS, VENTA_NUEVOS_13 AS VENTA_NUEVOS, VENTA_PO_REPETIDORES_13 AS VENTA_PO_REPETIDORES, VENTA_REPETIDORES_13 AS VENTA_REPETIDORES, VENTA_PO_LEALES_13 AS VENTA_PO_LEALES, VENTA_LEALES_13 AS VENTA_LEALES FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 14 IND_MES, CLIENTES_14 AS CLIENTES, VENTA_14 AS VENTA, TX_14 AS TX, PO_RECOMPRA_14 AS PO_RECOMPRA, RECOMPRA_14 AS RECOMPRA, PO_FID_14 AS PO_FID, FID_14 AS FID, PO_DOR_14 AS PO_DOR, DOR_14 AS DOR, PO_PER_14 AS PO_PER, PER_14 AS PER, PO_REC_14 AS PO_REC, REC_14 AS REC, PO_NUEVOS_14 AS PO_NUEVOS, NUEVOS_14 AS NUEVOS, PO_REPETIDORES_14 AS PO_REPETIDORES, REPETIDORES_14 AS REPETIDORES, PO_LEALES_14 AS PO_LEALES, LEALES_14 AS LEALES, VENTA_PO_RECOMPRA_14 AS VENTA_PO_RECOMPRA, VENTA_RECOMPRA_14 AS VENTA_RECOMPRA, VENTA_PO_FID_14 AS VENTA_PO_FID, VENTA_FID_14 AS VENTA_FID, VENTA_PO_DOR_14 AS VENTA_PO_DOR, VENTA_DOR_14 AS VENTA_DOR, VENTA_PO_PER_14 AS VENTA_PO_PER, VENTA_PER_14 AS VENTA_PER, VENTA_PO_REC_14 AS VENTA_PO_REC, VENTA_REC_14 AS VENTA_REC, VENTA_PO_NUEVOS_14 AS VENTA_PO_NUEVOS, VENTA_NUEVOS_14 AS VENTA_NUEVOS, VENTA_PO_REPETIDORES_14 AS VENTA_PO_REPETIDORES, VENTA_REPETIDORES_14 AS VENTA_REPETIDORES, VENTA_PO_LEALES_14 AS VENTA_PO_LEALES, VENTA_LEALES_14 AS VENTA_LEALES FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 15 IND_MES, CLIENTES_15 AS CLIENTES, VENTA_15 AS VENTA, TX_15 AS TX, PO_RECOMPRA_15 AS PO_RECOMPRA, RECOMPRA_15 AS RECOMPRA, PO_FID_15 AS PO_FID, FID_15 AS FID, PO_DOR_15 AS PO_DOR, DOR_15 AS DOR, PO_PER_15 AS PO_PER, PER_15 AS PER, PO_REC_15 AS PO_REC, REC_15 AS REC, PO_NUEVOS_15 AS PO_NUEVOS, NUEVOS_15 AS NUEVOS, PO_REPETIDORES_15 AS PO_REPETIDORES, REPETIDORES_15 AS REPETIDORES, PO_LEALES_15 AS PO_LEALES, LEALES_15 AS LEALES, VENTA_PO_RECOMPRA_15 AS VENTA_PO_RECOMPRA, VENTA_RECOMPRA_15 AS VENTA_RECOMPRA, VENTA_PO_FID_15 AS VENTA_PO_FID, VENTA_FID_15 AS VENTA_FID, VENTA_PO_DOR_15 AS VENTA_PO_DOR, VENTA_DOR_15 AS VENTA_DOR, VENTA_PO_PER_15 AS VENTA_PO_PER, VENTA_PER_15 AS VENTA_PER, VENTA_PO_REC_15 AS VENTA_PO_REC, VENTA_REC_15 AS VENTA_REC, VENTA_PO_NUEVOS_15 AS VENTA_PO_NUEVOS, VENTA_NUEVOS_15 AS VENTA_NUEVOS, VENTA_PO_REPETIDORES_15 AS VENTA_PO_REPETIDORES, VENTA_REPETIDORES_15 AS VENTA_REPETIDORES, VENTA_PO_LEALES_15 AS VENTA_PO_LEALES, VENTA_LEALES_15 AS VENTA_LEALES FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 16 IND_MES, CLIENTES_16 AS CLIENTES, VENTA_16 AS VENTA, TX_16 AS TX, PO_RECOMPRA_16 AS PO_RECOMPRA, RECOMPRA_16 AS RECOMPRA, PO_FID_16 AS PO_FID, FID_16 AS FID, PO_DOR_16 AS PO_DOR, DOR_16 AS DOR, PO_PER_16 AS PO_PER, PER_16 AS PER, PO_REC_16 AS PO_REC, REC_16 AS REC, PO_NUEVOS_16 AS PO_NUEVOS, NUEVOS_16 AS NUEVOS, PO_REPETIDORES_16 AS PO_REPETIDORES, REPETIDORES_16 AS REPETIDORES, PO_LEALES_16 AS PO_LEALES, LEALES_16 AS LEALES, VENTA_PO_RECOMPRA_16 AS VENTA_PO_RECOMPRA, VENTA_RECOMPRA_16 AS VENTA_RECOMPRA, VENTA_PO_FID_16 AS VENTA_PO_FID, VENTA_FID_16 AS VENTA_FID, VENTA_PO_DOR_16 AS VENTA_PO_DOR, VENTA_DOR_16 AS VENTA_DOR, VENTA_PO_PER_16 AS VENTA_PO_PER, VENTA_PER_16 AS VENTA_PER, VENTA_PO_REC_16 AS VENTA_PO_REC, VENTA_REC_16 AS VENTA_REC, VENTA_PO_NUEVOS_16 AS VENTA_PO_NUEVOS, VENTA_NUEVOS_16 AS VENTA_NUEVOS, VENTA_PO_REPETIDORES_16 AS VENTA_PO_REPETIDORES, VENTA_REPETIDORES_16 AS VENTA_REPETIDORES, VENTA_PO_LEALES_16 AS VENTA_PO_LEALES, VENTA_LEALES_16 AS VENTA_LEALES FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 17 IND_MES, CLIENTES_17 AS CLIENTES, VENTA_17 AS VENTA, TX_17 AS TX, PO_RECOMPRA_17 AS PO_RECOMPRA, RECOMPRA_17 AS RECOMPRA, PO_FID_17 AS PO_FID, FID_17 AS FID, PO_DOR_17 AS PO_DOR, DOR_17 AS DOR, PO_PER_17 AS PO_PER, PER_17 AS PER, PO_REC_17 AS PO_REC, REC_17 AS REC, PO_NUEVOS_17 AS PO_NUEVOS, NUEVOS_17 AS NUEVOS, PO_REPETIDORES_17 AS PO_REPETIDORES, REPETIDORES_17 AS REPETIDORES, PO_LEALES_17 AS PO_LEALES, LEALES_17 AS LEALES, VENTA_PO_RECOMPRA_17 AS VENTA_PO_RECOMPRA, VENTA_RECOMPRA_17 AS VENTA_RECOMPRA, VENTA_PO_FID_17 AS VENTA_PO_FID, VENTA_FID_17 AS VENTA_FID, VENTA_PO_DOR_17 AS VENTA_PO_DOR, VENTA_DOR_17 AS VENTA_DOR, VENTA_PO_PER_17 AS VENTA_PO_PER, VENTA_PER_17 AS VENTA_PER, VENTA_PO_REC_17 AS VENTA_PO_REC, VENTA_REC_17 AS VENTA_REC, VENTA_PO_NUEVOS_17 AS VENTA_PO_NUEVOS, VENTA_NUEVOS_17 AS VENTA_NUEVOS, VENTA_PO_REPETIDORES_17 AS VENTA_PO_REPETIDORES, VENTA_REPETIDORES_17 AS VENTA_REPETIDORES, VENTA_PO_LEALES_17 AS VENTA_PO_LEALES, VENTA_LEALES_17 AS VENTA_LEALES FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 18 IND_MES, CLIENTES_18 AS CLIENTES, VENTA_18 AS VENTA, TX_18 AS TX, PO_RECOMPRA_18 AS PO_RECOMPRA, RECOMPRA_18 AS RECOMPRA, PO_FID_18 AS PO_FID, FID_18 AS FID, PO_DOR_18 AS PO_DOR, DOR_18 AS DOR, PO_PER_18 AS PO_PER, PER_18 AS PER, PO_REC_18 AS PO_REC, REC_18 AS REC, PO_NUEVOS_18 AS PO_NUEVOS, NUEVOS_18 AS NUEVOS, PO_REPETIDORES_18 AS PO_REPETIDORES, REPETIDORES_18 AS REPETIDORES, PO_LEALES_18 AS PO_LEALES, LEALES_18 AS LEALES, VENTA_PO_RECOMPRA_18 AS VENTA_PO_RECOMPRA, VENTA_RECOMPRA_18 AS VENTA_RECOMPRA, VENTA_PO_FID_18 AS VENTA_PO_FID, VENTA_FID_18 AS VENTA_FID, VENTA_PO_DOR_18 AS VENTA_PO_DOR, VENTA_DOR_18 AS VENTA_DOR, VENTA_PO_PER_18 AS VENTA_PO_PER, VENTA_PER_18 AS VENTA_PER, VENTA_PO_REC_18 AS VENTA_PO_REC, VENTA_REC_18 AS VENTA_REC, VENTA_PO_NUEVOS_18 AS VENTA_PO_NUEVOS, VENTA_NUEVOS_18 AS VENTA_NUEVOS, VENTA_PO_REPETIDORES_18 AS VENTA_PO_REPETIDORES, VENTA_REPETIDORES_18 AS VENTA_REPETIDORES, VENTA_PO_LEALES_18 AS VENTA_PO_LEALES, VENTA_LEALES_18 AS VENTA_LEALES FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 19 IND_MES, CLIENTES_19 AS CLIENTES, VENTA_19 AS VENTA, TX_19 AS TX, PO_RECOMPRA_19 AS PO_RECOMPRA, RECOMPRA_19 AS RECOMPRA, PO_FID_19 AS PO_FID, FID_19 AS FID, PO_DOR_19 AS PO_DOR, DOR_19 AS DOR, PO_PER_19 AS PO_PER, PER_19 AS PER, PO_REC_19 AS PO_REC, REC_19 AS REC, PO_NUEVOS_19 AS PO_NUEVOS, NUEVOS_19 AS NUEVOS, PO_REPETIDORES_19 AS PO_REPETIDORES, REPETIDORES_19 AS REPETIDORES, PO_LEALES_19 AS PO_LEALES, LEALES_19 AS LEALES, VENTA_PO_RECOMPRA_19 AS VENTA_PO_RECOMPRA, VENTA_RECOMPRA_19 AS VENTA_RECOMPRA, VENTA_PO_FID_19 AS VENTA_PO_FID, VENTA_FID_19 AS VENTA_FID, VENTA_PO_DOR_19 AS VENTA_PO_DOR, VENTA_DOR_19 AS VENTA_DOR, VENTA_PO_PER_19 AS VENTA_PO_PER, VENTA_PER_19 AS VENTA_PER, VENTA_PO_REC_19 AS VENTA_PO_REC, VENTA_REC_19 AS VENTA_REC, VENTA_PO_NUEVOS_19 AS VENTA_PO_NUEVOS, VENTA_NUEVOS_19 AS VENTA_NUEVOS, VENTA_PO_REPETIDORES_19 AS VENTA_PO_REPETIDORES, VENTA_REPETIDORES_19 AS VENTA_REPETIDORES, VENTA_PO_LEALES_19 AS VENTA_PO_LEALES, VENTA_LEALES_19 AS VENTA_LEALES FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 20 IND_MES, CLIENTES_20 AS CLIENTES, VENTA_20 AS VENTA, TX_20 AS TX, PO_RECOMPRA_20 AS PO_RECOMPRA, RECOMPRA_20 AS RECOMPRA, PO_FID_20 AS PO_FID, FID_20 AS FID, PO_DOR_20 AS PO_DOR, DOR_20 AS DOR, PO_PER_20 AS PO_PER, PER_20 AS PER, PO_REC_20 AS PO_REC, REC_20 AS REC, PO_NUEVOS_20 AS PO_NUEVOS, NUEVOS_20 AS NUEVOS, PO_REPETIDORES_20 AS PO_REPETIDORES, REPETIDORES_20 AS REPETIDORES, PO_LEALES_20 AS PO_LEALES, LEALES_20 AS LEALES, VENTA_PO_RECOMPRA_20 AS VENTA_PO_RECOMPRA, VENTA_RECOMPRA_20 AS VENTA_RECOMPRA, VENTA_PO_FID_20 AS VENTA_PO_FID, VENTA_FID_20 AS VENTA_FID, VENTA_PO_DOR_20 AS VENTA_PO_DOR, VENTA_DOR_20 AS VENTA_DOR, VENTA_PO_PER_20 AS VENTA_PO_PER, VENTA_PER_20 AS VENTA_PER, VENTA_PO_REC_20 AS VENTA_PO_REC, VENTA_REC_20 AS VENTA_REC, VENTA_PO_NUEVOS_20 AS VENTA_PO_NUEVOS, VENTA_NUEVOS_20 AS VENTA_NUEVOS, VENTA_PO_REPETIDORES_20 AS VENTA_PO_REPETIDORES, VENTA_REPETIDORES_20 AS VENTA_REPETIDORES, VENTA_PO_LEALES_20 AS VENTA_PO_LEALES, VENTA_LEALES_20 AS VENTA_LEALES FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 21 IND_MES, CLIENTES_21 AS CLIENTES, VENTA_21 AS VENTA, TX_21 AS TX, PO_RECOMPRA_21 AS PO_RECOMPRA, RECOMPRA_21 AS RECOMPRA, PO_FID_21 AS PO_FID, FID_21 AS FID, PO_DOR_21 AS PO_DOR, DOR_21 AS DOR, PO_PER_21 AS PO_PER, PER_21 AS PER, PO_REC_21 AS PO_REC, REC_21 AS REC, PO_NUEVOS_21 AS PO_NUEVOS, NUEVOS_21 AS NUEVOS, PO_REPETIDORES_21 AS PO_REPETIDORES, REPETIDORES_21 AS REPETIDORES, PO_LEALES_21 AS PO_LEALES, LEALES_21 AS LEALES, VENTA_PO_RECOMPRA_21 AS VENTA_PO_RECOMPRA, VENTA_RECOMPRA_21 AS VENTA_RECOMPRA, VENTA_PO_FID_21 AS VENTA_PO_FID, VENTA_FID_21 AS VENTA_FID, VENTA_PO_DOR_21 AS VENTA_PO_DOR, VENTA_DOR_21 AS VENTA_DOR, VENTA_PO_PER_21 AS VENTA_PO_PER, VENTA_PER_21 AS VENTA_PER, VENTA_PO_REC_21 AS VENTA_PO_REC, VENTA_REC_21 AS VENTA_REC, VENTA_PO_NUEVOS_21 AS VENTA_PO_NUEVOS, VENTA_NUEVOS_21 AS VENTA_NUEVOS, VENTA_PO_REPETIDORES_21 AS VENTA_PO_REPETIDORES, VENTA_REPETIDORES_21 AS VENTA_REPETIDORES, VENTA_PO_LEALES_21 AS VENTA_PO_LEALES, VENTA_LEALES_21 AS VENTA_LEALES FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 22 IND_MES, CLIENTES_22 AS CLIENTES, VENTA_22 AS VENTA, TX_22 AS TX, PO_RECOMPRA_22 AS PO_RECOMPRA, RECOMPRA_22 AS RECOMPRA, PO_FID_22 AS PO_FID, FID_22 AS FID, PO_DOR_22 AS PO_DOR, DOR_22 AS DOR, PO_PER_22 AS PO_PER, PER_22 AS PER, PO_REC_22 AS PO_REC, REC_22 AS REC, PO_NUEVOS_22 AS PO_NUEVOS, NUEVOS_22 AS NUEVOS, PO_REPETIDORES_22 AS PO_REPETIDORES, REPETIDORES_22 AS REPETIDORES, PO_LEALES_22 AS PO_LEALES, LEALES_22 AS LEALES, VENTA_PO_RECOMPRA_22 AS VENTA_PO_RECOMPRA, VENTA_RECOMPRA_22 AS VENTA_RECOMPRA, VENTA_PO_FID_22 AS VENTA_PO_FID, VENTA_FID_22 AS VENTA_FID, VENTA_PO_DOR_22 AS VENTA_PO_DOR, VENTA_DOR_22 AS VENTA_DOR, VENTA_PO_PER_22 AS VENTA_PO_PER, VENTA_PER_22 AS VENTA_PER, VENTA_PO_REC_22 AS VENTA_PO_REC, VENTA_REC_22 AS VENTA_REC, VENTA_PO_NUEVOS_22 AS VENTA_PO_NUEVOS, VENTA_NUEVOS_22 AS VENTA_NUEVOS, VENTA_PO_REPETIDORES_22 AS VENTA_PO_REPETIDORES, VENTA_REPETIDORES_22 AS VENTA_REPETIDORES, VENTA_PO_LEALES_22 AS VENTA_PO_LEALES, VENTA_LEALES_22 AS VENTA_LEALES FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 23 IND_MES, CLIENTES_23 AS CLIENTES, VENTA_23 AS VENTA, TX_23 AS TX, PO_RECOMPRA_23 AS PO_RECOMPRA, RECOMPRA_23 AS RECOMPRA, PO_FID_23 AS PO_FID, FID_23 AS FID, PO_DOR_23 AS PO_DOR, DOR_23 AS DOR, PO_PER_23 AS PO_PER, PER_23 AS PER, PO_REC_23 AS PO_REC, REC_23 AS REC, PO_NUEVOS_23 AS PO_NUEVOS, NUEVOS_23 AS NUEVOS, PO_REPETIDORES_23 AS PO_REPETIDORES, REPETIDORES_23 AS REPETIDORES, PO_LEALES_23 AS PO_LEALES, LEALES_23 AS LEALES, VENTA_PO_RECOMPRA_23 AS VENTA_PO_RECOMPRA, VENTA_RECOMPRA_23 AS VENTA_RECOMPRA, VENTA_PO_FID_23 AS VENTA_PO_FID, VENTA_FID_23 AS VENTA_FID, VENTA_PO_DOR_23 AS VENTA_PO_DOR, VENTA_DOR_23 AS VENTA_DOR, VENTA_PO_PER_23 AS VENTA_PO_PER, VENTA_PER_23 AS VENTA_PER, VENTA_PO_REC_23 AS VENTA_PO_REC, VENTA_REC_23 AS VENTA_REC, VENTA_PO_NUEVOS_23 AS VENTA_PO_NUEVOS, VENTA_NUEVOS_23 AS VENTA_NUEVOS, VENTA_PO_REPETIDORES_23 AS VENTA_PO_REPETIDORES, VENTA_REPETIDORES_23 AS VENTA_REPETIDORES, VENTA_PO_LEALES_23 AS VENTA_PO_LEALES, VENTA_LEALES_23 AS VENTA_LEALES FROM __CLIENTES
                UNION
                SELECT PROVEEDOR, MARCA, 24 IND_MES, CLIENTES_24 AS CLIENTES, VENTA_24 AS VENTA, TX_24 AS TX, PO_RECOMPRA_24 AS PO_RECOMPRA, RECOMPRA_24 AS RECOMPRA, PO_FID_24 AS PO_FID, FID_24 AS FID, PO_DOR_24 AS PO_DOR, DOR_24 AS DOR, PO_PER_24 AS PO_PER, PER_24 AS PER, PO_REC_24 AS PO_REC, REC_24 AS REC, PO_NUEVOS_24 AS PO_NUEVOS, NUEVOS_24 AS NUEVOS, PO_REPETIDORES_24 AS PO_REPETIDORES, REPETIDORES_24 AS REPETIDORES, PO_LEALES_24 AS PO_LEALES, LEALES_24 AS LEALES, VENTA_PO_RECOMPRA_24 AS VENTA_PO_RECOMPRA, VENTA_RECOMPRA_24 AS VENTA_RECOMPRA, VENTA_PO_FID_24 AS VENTA_PO_FID, VENTA_FID_24 AS VENTA_FID, VENTA_PO_DOR_24 AS VENTA_PO_DOR, VENTA_DOR_24 AS VENTA_DOR, VENTA_PO_PER_24 AS VENTA_PO_PER, VENTA_PER_24 AS VENTA_PER, VENTA_PO_REC_24 AS VENTA_PO_REC, VENTA_REC_24 AS VENTA_REC, VENTA_PO_NUEVOS_24 AS VENTA_PO_NUEVOS, VENTA_NUEVOS_24 AS VENTA_NUEVOS, VENTA_PO_REPETIDORES_24 AS VENTA_PO_REPETIDORES, VENTA_REPETIDORES_24 AS VENTA_REPETIDORES, VENTA_PO_LEALES_24 AS VENTA_PO_LEALES, VENTA_LEALES_24 AS VENTA_LEALES FROM __CLIENTES
            )
            SELECT
                '$[CODIGO_CAMPANA]' CODIGO_CAMPANA
                ,PROVEEDOR
                ,MARCA
                ,MES
                ,CLIENTES
                ,VENTA
                ,TX
                ,PO_RECOMPRA
                ,RECOMPRA
                ,PO_FID
                ,FID
                ,PO_DOR
                ,DOR
                ,PO_PER
                ,PER
                ,PO_REC
                ,REC
                ,PO_NUEVOS
                ,NUEVOS
                ,PO_REPETIDORES
                ,REPETIDORES
                ,PO_LEALES
                ,LEALES
                ,VENTA_PO_RECOMPRA
                ,VENTA_RECOMPRA
                ,VENTA_PO_FID
                ,VENTA_FID
                ,VENTA_PO_DOR
                ,VENTA_DOR
                ,VENTA_PO_PER
                ,VENTA_PER
                ,VENTA_PO_REC
                ,VENTA_REC
                ,VENTA_PO_NUEVOS
                ,VENTA_NUEVOS
                ,VENTA_PO_REPETIDORES
                ,VENTA_REPETIDORES
                ,VENTA_PO_LEALES
                ,VENTA_LEALES
            FROM __RESUMEN_SEGMENTOS A
            LEFT JOIN #MESES B USING(IND_MES)
            );

            --SELECT * FROM #SEGMENTOS ORDER BY MES;
            '''
        
        query_insert = f'''
            DELETE CHEDRAUI.MON_CAMP_EVOLUTION_SEGMENTS WHERE CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}';
            INSERT INTO CHEDRAUI.MON_CAMP_EVOLUTION_SEGMENTS SELECT * FROM #SEGMENTOS;
        '''

        return [query_indicadores, query_segmentos, query_insert]

    def get_queries_campana_retencion(self):
        query_segmentos_retencion = f'''
            --SE REQUIERE TABLA #MESES E #INDICADORES
            --SEGMENTOS
            DROP TABLE IF EXISTS #SEGMENTOS_RETENCION;
            CREATE TABLE #SEGMENTOS_RETENCION AS (
            WITH __INDICADORES AS (
                SELECT
                    *
                    --POS
                --PO RECOMPRA
                ,IND_12 AS PO_RECOMPRA_13
                ,IND_13 AS PO_RECOMPRA_14
                ,IND_14 AS PO_RECOMPRA_15
                ,IND_15 AS PO_RECOMPRA_16
                ,IND_16 AS PO_RECOMPRA_17
                ,IND_17 AS PO_RECOMPRA_18
                ,IND_18 AS PO_RECOMPRA_19
                ,IND_19 AS PO_RECOMPRA_20
                ,IND_20 AS PO_RECOMPRA_21
                ,IND_21 AS PO_RECOMPRA_22
                ,IND_22 AS PO_RECOMPRA_23
                ,IND_23 AS PO_RECOMPRA_24
                
                --PO FID
                ,CASE WHEN IND_10 = 1 AND IND_11 = 1 AND IND_12 = 1 THEN 1 ELSE 0 END AS PO_FID_13
                ,CASE WHEN IND_11 = 1 AND IND_12 = 1 AND IND_13 = 1 THEN 1 ELSE 0 END AS PO_FID_14
                ,CASE WHEN IND_12 = 1 AND IND_13 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END AS PO_FID_15
                ,CASE WHEN IND_13 = 1 AND IND_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS PO_FID_16
                ,CASE WHEN IND_14 = 1 AND IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS PO_FID_17
                ,CASE WHEN IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS PO_FID_18
                ,CASE WHEN IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS PO_FID_19
                ,CASE WHEN IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS PO_FID_20
                ,CASE WHEN IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS PO_FID_21
                ,CASE WHEN IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS PO_FID_22
                ,CASE WHEN IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS PO_FID_23
                ,CASE WHEN IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS PO_FID_24

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
                
                --PO REPETIDORES (COMPRAN MÁS DE UN TX)
                ,CASE WHEN TX_1 + TX_2 + TX_3 + TX_4 + TX_5 + TX_6 + TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_13 --      ,CASE WHEN TX_7 > 1 AND TX_8 > 1 AND TX_9 > 1 AND TX_10 > 1 AND TX_11 > 1 AND TX_12 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_13
                ,CASE WHEN TX_2 + TX_3 + TX_4 + TX_5 + TX_6 + TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_14
                ,CASE WHEN TX_3 + TX_4 + TX_5 + TX_6 + TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_15
                ,CASE WHEN TX_4 + TX_5 + TX_6 + TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_16
                ,CASE WHEN TX_5 + TX_6 + TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_17
                ,CASE WHEN TX_6 + TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_18
                ,CASE WHEN TX_7 + TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 + TX_18 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_19
                ,CASE WHEN TX_8 + TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 + TX_18 + TX_19 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_20
                ,CASE WHEN TX_9 + TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 + TX_18 + TX_19 + TX_20 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_21
                ,CASE WHEN TX_10 + TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 + TX_18 + TX_19 + TX_20 + TX_21 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_22
                ,CASE WHEN TX_11 + TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 + TX_18 + TX_19 + TX_20 + TX_21 + TX_22 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_23
                ,CASE WHEN TX_12 + TX_13 + TX_14 + TX_15 + TX_16 + TX_17 + TX_18 + TX_19 + TX_20 + TX_21 + TX_22 + TX_23 > 1 THEN 1 ELSE 0 END PO_REPETIDORES_24
                
                --PO LEALES (COMPRAN MÁS DE UN TX SOLO DE LA MARCA)
                ,CASE WHEN PO_REPETIDORES_13 = 1 AND COMP_TX_1 + COMP_TX_2 + COMP_TX_3 + COMP_TX_4 + COMP_TX_5 + COMP_TX_6 + COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 = 0 THEN 1 ELSE 0 END PO_LEALES_13
                ,CASE WHEN PO_REPETIDORES_14 = 1 AND COMP_TX_2 + COMP_TX_3 + COMP_TX_4 + COMP_TX_5 + COMP_TX_6 + COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 = 0 THEN 1 ELSE 0 END PO_LEALES_14
                ,CASE WHEN PO_REPETIDORES_15 = 1 AND COMP_TX_3 + COMP_TX_4 + COMP_TX_5 + COMP_TX_6 + COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 = 0 THEN 1 ELSE 0 END PO_LEALES_15
                ,CASE WHEN PO_REPETIDORES_16 = 1 AND COMP_TX_4 + COMP_TX_5 + COMP_TX_6 + COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 = 0 THEN 1 ELSE 0 END PO_LEALES_16
                ,CASE WHEN PO_REPETIDORES_17 = 1 AND COMP_TX_5 + COMP_TX_6 + COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 = 0 THEN 1 ELSE 0 END PO_LEALES_17
                ,CASE WHEN PO_REPETIDORES_18 = 1 AND COMP_TX_6 + COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 = 0 THEN 1 ELSE 0 END PO_LEALES_18
                ,CASE WHEN PO_REPETIDORES_19 = 1 AND COMP_TX_7 + COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 + COMP_TX_18 = 0 THEN 1 ELSE 0 END PO_LEALES_19
                ,CASE WHEN PO_REPETIDORES_20 = 1 AND COMP_TX_8 + COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 + COMP_TX_18 + COMP_TX_19 = 0 THEN 1 ELSE 0 END PO_LEALES_20
                ,CASE WHEN PO_REPETIDORES_21 = 1 AND COMP_TX_9 + COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 + COMP_TX_18 + COMP_TX_19 + COMP_TX_20 = 0 THEN 1 ELSE 0 END PO_LEALES_21
                ,CASE WHEN PO_REPETIDORES_22 = 1 AND COMP_TX_10 + COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 + COMP_TX_18 + COMP_TX_19 + COMP_TX_20 + COMP_TX_21 = 0 THEN 1 ELSE 0 END PO_LEALES_22
                ,CASE WHEN PO_REPETIDORES_23 = 1 AND COMP_TX_11 + COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 + COMP_TX_18 + COMP_TX_19 + COMP_TX_20 + COMP_TX_21 + COMP_TX_22 = 0 THEN 1 ELSE 0 END PO_LEALES_23
                ,CASE WHEN PO_REPETIDORES_24 = 1 AND COMP_TX_12 + COMP_TX_13 + COMP_TX_14 + COMP_TX_15 + COMP_TX_16 + COMP_TX_17 + COMP_TX_18 + COMP_TX_19 + COMP_TX_20 + COMP_TX_21 + COMP_TX_22 + COMP_TX_23 = 0 THEN 1 ELSE 0 END PO_LEALES_24
                
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

                --REPETIDORES
                ,CASE WHEN PO_REPETIDORES_13 = 1 AND TX_13 > 0 THEN 1 ELSE 0 END REPETIDORES_13
                ,CASE WHEN PO_REPETIDORES_14 = 1 AND TX_14 > 0 THEN 1 ELSE 0 END REPETIDORES_14
                ,CASE WHEN PO_REPETIDORES_15 = 1 AND TX_15 > 0 THEN 1 ELSE 0 END REPETIDORES_15
                ,CASE WHEN PO_REPETIDORES_16 = 1 AND TX_16 > 0 THEN 1 ELSE 0 END REPETIDORES_16
                ,CASE WHEN PO_REPETIDORES_17 = 1 AND TX_17 > 0 THEN 1 ELSE 0 END REPETIDORES_17
                ,CASE WHEN PO_REPETIDORES_18 = 1 AND TX_18 > 0 THEN 1 ELSE 0 END REPETIDORES_18
                ,CASE WHEN PO_REPETIDORES_19 = 1 AND TX_19 > 0 THEN 1 ELSE 0 END REPETIDORES_19
                ,CASE WHEN PO_REPETIDORES_20 = 1 AND TX_20 > 0 THEN 1 ELSE 0 END REPETIDORES_20
                ,CASE WHEN PO_REPETIDORES_21 = 1 AND TX_21 > 0 THEN 1 ELSE 0 END REPETIDORES_21
                ,CASE WHEN PO_REPETIDORES_22 = 1 AND TX_22 > 0 THEN 1 ELSE 0 END REPETIDORES_22
                ,CASE WHEN PO_REPETIDORES_23 = 1 AND TX_23 > 0 THEN 1 ELSE 0 END REPETIDORES_23
                ,CASE WHEN PO_REPETIDORES_24 = 1 AND TX_24 > 0 THEN 1 ELSE 0 END REPETIDORES_24

                --LEALES
                ,CASE WHEN PO_LEALES_13 = 1 AND TX_13 > 0 AND COMP_TX_13 = 0 THEN 1 ELSE 0 END LEALES_13
                ,CASE WHEN PO_LEALES_14 = 1 AND TX_14 > 0 AND COMP_TX_14 = 0 THEN 1 ELSE 0 END LEALES_14
                ,CASE WHEN PO_LEALES_15 = 1 AND TX_15 > 0 AND COMP_TX_15 = 0 THEN 1 ELSE 0 END LEALES_15
                ,CASE WHEN PO_LEALES_16 = 1 AND TX_16 > 0 AND COMP_TX_16 = 0 THEN 1 ELSE 0 END LEALES_16
                ,CASE WHEN PO_LEALES_17 = 1 AND TX_17 > 0 AND COMP_TX_17 = 0 THEN 1 ELSE 0 END LEALES_17
                ,CASE WHEN PO_LEALES_18 = 1 AND TX_18 > 0 AND COMP_TX_18 = 0 THEN 1 ELSE 0 END LEALES_18
                ,CASE WHEN PO_LEALES_19 = 1 AND TX_19 > 0 AND COMP_TX_19 = 0 THEN 1 ELSE 0 END LEALES_19
                ,CASE WHEN PO_LEALES_20 = 1 AND TX_20 > 0 AND COMP_TX_20 = 0 THEN 1 ELSE 0 END LEALES_20
                ,CASE WHEN PO_LEALES_21 = 1 AND TX_21 > 0 AND COMP_TX_21 = 0 THEN 1 ELSE 0 END LEALES_21
                ,CASE WHEN PO_LEALES_22 = 1 AND TX_22 > 0 AND COMP_TX_22 = 0 THEN 1 ELSE 0 END LEALES_22
                ,CASE WHEN PO_LEALES_23 = 1 AND TX_23 > 0 AND COMP_TX_23 = 0 THEN 1 ELSE 0 END LEALES_23
                ,CASE WHEN PO_LEALES_24 = 1 AND TX_24 > 0 AND COMP_TX_24 = 0 THEN 1 ELSE 0 END LEALES_24
                FROM #INDICADORES
            )
            ,__RETENCION AS (
                SELECT
                *
                --RECOMPRA
                --COMPRA EN EL SIGUIENTE MES
                ,CASE WHEN RECOMPRA_13 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_1M_13
                ,CASE WHEN RECOMPRA_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_1M_14
                ,CASE WHEN RECOMPRA_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_1M_15
                ,CASE WHEN RECOMPRA_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_1M_16
                ,CASE WHEN RECOMPRA_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_1M_17
                ,CASE WHEN RECOMPRA_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_1M_18
                ,CASE WHEN RECOMPRA_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_1M_19
                ,CASE WHEN RECOMPRA_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_1M_20
                ,CASE WHEN RECOMPRA_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_1M_21
                ,CASE WHEN RECOMPRA_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_1M_22
                ,CASE WHEN RECOMPRA_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_1M_23
                ,0 AS RECOMPRA_RETENCION_1M_24
                        
                --COMPRA EN LOS SIGUIENTES 2 MESES
                ,CASE WHEN RECOMPRA_13 = 1 AND IND_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_2M_13
                ,CASE WHEN RECOMPRA_14 = 1 AND IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_2M_14
                ,CASE WHEN RECOMPRA_15 = 1 AND IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_2M_15
                ,CASE WHEN RECOMPRA_16 = 1 AND IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_2M_16
                ,CASE WHEN RECOMPRA_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_2M_17
                ,CASE WHEN RECOMPRA_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_2M_18
                ,CASE WHEN RECOMPRA_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_2M_19
                ,CASE WHEN RECOMPRA_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_2M_20
                ,CASE WHEN RECOMPRA_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_2M_21
                ,CASE WHEN RECOMPRA_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_2M_22
                ,0 AS RECOMPRA_RETENCION_2M_23
                ,0 AS RECOMPRA_RETENCION_2M_24
                
                --COMPRA EN LOS SIGUIENTES 3 MESES
                ,CASE WHEN RECOMPRA_13 = 1 AND IND_14 = 1 AND IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_3M_13
                ,CASE WHEN RECOMPRA_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_3M_14
                ,CASE WHEN RECOMPRA_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_3M_15
                ,CASE WHEN RECOMPRA_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_3M_16
                ,CASE WHEN RECOMPRA_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_3M_17
                ,CASE WHEN RECOMPRA_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_3M_18
                ,CASE WHEN RECOMPRA_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_3M_19
                ,CASE WHEN RECOMPRA_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_3M_20
                ,CASE WHEN RECOMPRA_21 = 1 AND IND_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_3M_21
                ,0 AS RECOMPRA_RETENCION_3M_22
                ,0 AS RECOMPRA_RETENCION_3M_23
                ,0 AS RECOMPRA_RETENCION_3M_24

                --COMPRA EN LOS SIGUIENTES 6 MESES
                ,CASE WHEN RECOMPRA_13 = 1 AND IND_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_6M_13
                ,CASE WHEN RECOMPRA_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_6M_14
                ,CASE WHEN RECOMPRA_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_6M_15
                ,CASE WHEN RECOMPRA_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_6M_16
                ,CASE WHEN RECOMPRA_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_6M_17
                ,CASE WHEN RECOMPRA_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS RECOMPRA_RETENCION_6M_18
                ,0 AS RECOMPRA_RETENCION_6M_19
                ,0 AS RECOMPRA_RETENCION_6M_20
                ,0 AS RECOMPRA_RETENCION_6M_21
                ,0 AS RECOMPRA_RETENCION_6M_22
                ,0 AS RECOMPRA_RETENCION_6M_23
                ,0 AS RECOMPRA_RETENCION_6M_24

                --FID
                --COMPRA EN EL SIGUIENTE MES
                ,CASE WHEN FID_13 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_1M_13
                ,CASE WHEN FID_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_1M_14
                ,CASE WHEN FID_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_1M_15
                ,CASE WHEN FID_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_1M_16
                ,CASE WHEN FID_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_1M_17
                ,CASE WHEN FID_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_1M_18
                ,CASE WHEN FID_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_1M_19
                ,CASE WHEN FID_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_1M_20
                ,CASE WHEN FID_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_1M_21
                ,CASE WHEN FID_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_1M_22
                ,CASE WHEN FID_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_1M_23
                ,0 AS FID_RETENCION_1M_24
                        
                --COMPRA EN LOS SIGUIENTES 2 MESES
                ,CASE WHEN FID_13 = 1 AND IND_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_2M_13
                ,CASE WHEN FID_14 = 1 AND IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_2M_14
                ,CASE WHEN FID_15 = 1 AND IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_2M_15
                ,CASE WHEN FID_16 = 1 AND IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_2M_16
                ,CASE WHEN FID_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_2M_17
                ,CASE WHEN FID_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_2M_18
                ,CASE WHEN FID_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_2M_19
                ,CASE WHEN FID_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_2M_20
                ,CASE WHEN FID_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_2M_21
                ,CASE WHEN FID_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_2M_22
                ,0 AS FID_RETENCION_2M_23
                ,0 AS FID_RETENCION_2M_24
                
                --COMPRA EN LOS SIGUIENTES 3 MESES
                ,CASE WHEN FID_13 = 1 AND IND_14 = 1 AND IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_3M_13
                ,CASE WHEN FID_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_3M_14
                ,CASE WHEN FID_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_3M_15
                ,CASE WHEN FID_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_3M_16
                ,CASE WHEN FID_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_3M_17
                ,CASE WHEN FID_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_3M_18
                ,CASE WHEN FID_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_3M_19
                ,CASE WHEN FID_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_3M_20
                ,CASE WHEN FID_21 = 1 AND IND_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_3M_21
                ,0 AS FID_RETENCION_3M_22
                ,0 AS FID_RETENCION_3M_23
                ,0 AS FID_RETENCION_3M_24

                --COMPRA EN LOS SIGUIENTES 6 MESES
                ,CASE WHEN FID_13 = 1 AND IND_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_6M_13
                ,CASE WHEN FID_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_6M_14
                ,CASE WHEN FID_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_6M_15
                ,CASE WHEN FID_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_6M_16
                ,CASE WHEN FID_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_6M_17
                ,CASE WHEN FID_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS FID_RETENCION_6M_18
                ,0 AS FID_RETENCION_6M_19
                ,0 AS FID_RETENCION_6M_20
                ,0 AS FID_RETENCION_6M_21
                ,0 AS FID_RETENCION_6M_22
                ,0 AS FID_RETENCION_6M_23
                ,0 AS FID_RETENCION_6M_24

                --REC
                --COMPRA EN EL SIGUIENTE MES
                ,CASE WHEN REC_13 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_1M_13
                ,CASE WHEN REC_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_1M_14
                ,CASE WHEN REC_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_1M_15
                ,CASE WHEN REC_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_1M_16
                ,CASE WHEN REC_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_1M_17
                ,CASE WHEN REC_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_1M_18
                ,CASE WHEN REC_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_1M_19
                ,CASE WHEN REC_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_1M_20
                ,CASE WHEN REC_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_1M_21
                ,CASE WHEN REC_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_1M_22
                ,CASE WHEN REC_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_1M_23
                ,0 AS REC_RETENCION_1M_24

                --COMPRA EN LOS SIGUIENTES 2 MESES
                ,CASE WHEN REC_13 = 1 AND IND_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_2M_13
                ,CASE WHEN REC_14 = 1 AND IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_2M_14
                ,CASE WHEN REC_15 = 1 AND IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_2M_15
                ,CASE WHEN REC_16 = 1 AND IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_2M_16
                ,CASE WHEN REC_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_2M_17
                ,CASE WHEN REC_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_2M_18
                ,CASE WHEN REC_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_2M_19
                ,CASE WHEN REC_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_2M_20
                ,CASE WHEN REC_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_2M_21
                ,CASE WHEN REC_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_2M_22
                ,0 AS REC_RETENCION_2M_23
                ,0 AS REC_RETENCION_2M_24

                --COMPRA EN LOS SIGUIENTES 3 MESES
                ,CASE WHEN REC_13 = 1 AND IND_14 = 1 AND IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_3M_13
                ,CASE WHEN REC_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_3M_14
                ,CASE WHEN REC_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_3M_15
                ,CASE WHEN REC_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_3M_16
                ,CASE WHEN REC_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_3M_17
                ,CASE WHEN REC_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_3M_18
                ,CASE WHEN REC_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_3M_19
                ,CASE WHEN REC_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_3M_20
                ,CASE WHEN REC_21 = 1 AND IND_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_3M_21
                ,0 AS REC_RETENCION_3M_22
                ,0 AS REC_RETENCION_3M_23
                ,0 AS REC_RETENCION_3M_24

                --COMPRA EN LOS SIGUIENTES 6 MESES
                ,CASE WHEN REC_13 = 1 AND IND_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_6M_13
                ,CASE WHEN REC_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_6M_14
                ,CASE WHEN REC_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_6M_15
                ,CASE WHEN REC_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_6M_16
                ,CASE WHEN REC_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_6M_17
                ,CASE WHEN REC_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS REC_RETENCION_6M_18
                ,0 AS REC_RETENCION_6M_19
                ,0 AS REC_RETENCION_6M_20
                ,0 AS REC_RETENCION_6M_21
                ,0 AS REC_RETENCION_6M_22
                ,0 AS REC_RETENCION_6M_23
                ,0 AS REC_RETENCION_6M_24

                --NUEVOS
                --COMPRA EN EL SIGUIENTE MES
                ,CASE WHEN NUEVOS_13 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_1M_13
                ,CASE WHEN NUEVOS_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_1M_14
                ,CASE WHEN NUEVOS_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_1M_15
                ,CASE WHEN NUEVOS_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_1M_16
                ,CASE WHEN NUEVOS_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_1M_17
                ,CASE WHEN NUEVOS_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_1M_18
                ,CASE WHEN NUEVOS_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_1M_19
                ,CASE WHEN NUEVOS_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_1M_20
                ,CASE WHEN NUEVOS_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_1M_21
                ,CASE WHEN NUEVOS_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_1M_22
                ,CASE WHEN NUEVOS_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_1M_23
                ,0 AS NUEVOS_RETENCION_1M_24

                --COMPRA EN LOS SIGUIENTES 2 MESES
                ,CASE WHEN NUEVOS_13 = 1 AND IND_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_2M_13
                ,CASE WHEN NUEVOS_14 = 1 AND IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_2M_14
                ,CASE WHEN NUEVOS_15 = 1 AND IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_2M_15
                ,CASE WHEN NUEVOS_16 = 1 AND IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_2M_16
                ,CASE WHEN NUEVOS_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_2M_17
                ,CASE WHEN NUEVOS_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_2M_18
                ,CASE WHEN NUEVOS_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_2M_19
                ,CASE WHEN NUEVOS_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_2M_20
                ,CASE WHEN NUEVOS_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_2M_21
                ,CASE WHEN NUEVOS_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_2M_22
                ,0 AS NUEVOS_RETENCION_2M_23
                ,0 AS NUEVOS_RETENCION_2M_24

                --COMPRA EN LOS SIGUIENTES 3 MESES
                ,CASE WHEN NUEVOS_13 = 1 AND IND_14 = 1 AND IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_3M_13
                ,CASE WHEN NUEVOS_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_3M_14
                ,CASE WHEN NUEVOS_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_3M_15
                ,CASE WHEN NUEVOS_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_3M_16
                ,CASE WHEN NUEVOS_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_3M_17
                ,CASE WHEN NUEVOS_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_3M_18
                ,CASE WHEN NUEVOS_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_3M_19
                ,CASE WHEN NUEVOS_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_3M_20
                ,CASE WHEN NUEVOS_21 = 1 AND IND_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_3M_21
                ,0 AS NUEVOS_RETENCION_3M_22
                ,0 AS NUEVOS_RETENCION_3M_23
                ,0 AS NUEVOS_RETENCION_3M_24

                --COMPRA EN LOS SIGUIENTES 6 MESES
                ,CASE WHEN NUEVOS_13 = 1 AND IND_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_6M_13
                ,CASE WHEN NUEVOS_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_6M_14
                ,CASE WHEN NUEVOS_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_6M_15
                ,CASE WHEN NUEVOS_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_6M_16
                ,CASE WHEN NUEVOS_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_6M_17
                ,CASE WHEN NUEVOS_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS NUEVOS_RETENCION_6M_18
                ,0 AS NUEVOS_RETENCION_6M_19
                ,0 AS NUEVOS_RETENCION_6M_20
                ,0 AS NUEVOS_RETENCION_6M_21
                ,0 AS NUEVOS_RETENCION_6M_22
                ,0 AS NUEVOS_RETENCION_6M_23
                ,0 AS NUEVOS_RETENCION_6M_24

                --REPETIDORES
                --COMPRA EN EL SIGUIENTE MES
                ,CASE WHEN REPETIDORES_13 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_1M_13
                ,CASE WHEN REPETIDORES_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_1M_14
                ,CASE WHEN REPETIDORES_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_1M_15
                ,CASE WHEN REPETIDORES_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_1M_16
                ,CASE WHEN REPETIDORES_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_1M_17
                ,CASE WHEN REPETIDORES_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_1M_18
                ,CASE WHEN REPETIDORES_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_1M_19
                ,CASE WHEN REPETIDORES_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_1M_20
                ,CASE WHEN REPETIDORES_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_1M_21
                ,CASE WHEN REPETIDORES_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_1M_22
                ,CASE WHEN REPETIDORES_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_1M_23
                ,0 AS REPETIDORES_RETENCION_1M_24

                --COMPRA EN LOS SIGUIENTES 2 MESES
                ,CASE WHEN REPETIDORES_13 = 1 AND IND_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_2M_13
                ,CASE WHEN REPETIDORES_14 = 1 AND IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_2M_14
                ,CASE WHEN REPETIDORES_15 = 1 AND IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_2M_15
                ,CASE WHEN REPETIDORES_16 = 1 AND IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_2M_16
                ,CASE WHEN REPETIDORES_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_2M_17
                ,CASE WHEN REPETIDORES_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_2M_18
                ,CASE WHEN REPETIDORES_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_2M_19
                ,CASE WHEN REPETIDORES_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_2M_20
                ,CASE WHEN REPETIDORES_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_2M_21
                ,CASE WHEN REPETIDORES_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_2M_22
                ,0 AS REPETIDORES_RETENCION_2M_23
                ,0 AS REPETIDORES_RETENCION_2M_24

                --COMPRA EN LOS SIGUIENTES 3 MESES
                ,CASE WHEN REPETIDORES_13 = 1 AND IND_14 = 1 AND IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_3M_13
                ,CASE WHEN REPETIDORES_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_3M_14
                ,CASE WHEN REPETIDORES_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_3M_15
                ,CASE WHEN REPETIDORES_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_3M_16
                ,CASE WHEN REPETIDORES_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_3M_17
                ,CASE WHEN REPETIDORES_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_3M_18
                ,CASE WHEN REPETIDORES_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_3M_19
                ,CASE WHEN REPETIDORES_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_3M_20
                ,CASE WHEN REPETIDORES_21 = 1 AND IND_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_3M_21
                ,0 AS REPETIDORES_RETENCION_3M_22
                ,0 AS REPETIDORES_RETENCION_3M_23
                ,0 AS REPETIDORES_RETENCION_3M_24

                --COMPRA EN LOS SIGUIENTES 6 MESES
                ,CASE WHEN REPETIDORES_13 = 1 AND IND_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_6M_13
                ,CASE WHEN REPETIDORES_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_6M_14
                ,CASE WHEN REPETIDORES_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_6M_15
                ,CASE WHEN REPETIDORES_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_6M_16
                ,CASE WHEN REPETIDORES_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_6M_17
                ,CASE WHEN REPETIDORES_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS REPETIDORES_RETENCION_6M_18
                ,0 AS REPETIDORES_RETENCION_6M_19
                ,0 AS REPETIDORES_RETENCION_6M_20
                ,0 AS REPETIDORES_RETENCION_6M_21
                ,0 AS REPETIDORES_RETENCION_6M_22
                ,0 AS REPETIDORES_RETENCION_6M_23
                ,0 AS REPETIDORES_RETENCION_6M_24

                --LEALES
                --COMPRA EN EL SIGUIENTE MES
                ,CASE WHEN LEALES_13 = 1 AND IND_14 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_1M_13
                ,CASE WHEN LEALES_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_1M_14
                ,CASE WHEN LEALES_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_1M_15
                ,CASE WHEN LEALES_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_1M_16
                ,CASE WHEN LEALES_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_1M_17
                ,CASE WHEN LEALES_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_1M_18
                ,CASE WHEN LEALES_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_1M_19
                ,CASE WHEN LEALES_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_1M_20
                ,CASE WHEN LEALES_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_1M_21
                ,CASE WHEN LEALES_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_1M_22
                ,CASE WHEN LEALES_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_1M_23
                ,0 AS LEALES_RETENCION_1M_24

                --COMPRA EN LOS SIGUIENTES 2 MESES
                ,CASE WHEN LEALES_13 = 1 AND IND_14 = 1 AND IND_15 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_2M_13
                ,CASE WHEN LEALES_14 = 1 AND IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_2M_14
                ,CASE WHEN LEALES_15 = 1 AND IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_2M_15
                ,CASE WHEN LEALES_16 = 1 AND IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_2M_16
                ,CASE WHEN LEALES_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_2M_17
                ,CASE WHEN LEALES_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_2M_18
                ,CASE WHEN LEALES_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_2M_19
                ,CASE WHEN LEALES_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_2M_20
                ,CASE WHEN LEALES_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_2M_21
                ,CASE WHEN LEALES_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_2M_22
                ,0 AS LEALES_RETENCION_2M_23
                ,0 AS LEALES_RETENCION_2M_24

                --COMPRA EN LOS SIGUIENTES 3 MESES
                ,CASE WHEN LEALES_13 = 1 AND IND_14 = 1 AND IND_15 = 1 AND IND_16 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_3M_13
                ,CASE WHEN LEALES_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_3M_14
                ,CASE WHEN LEALES_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_3M_15
                ,CASE WHEN LEALES_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_3M_16
                ,CASE WHEN LEALES_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_3M_17
                ,CASE WHEN LEALES_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_3M_18
                ,CASE WHEN LEALES_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_3M_19
                ,CASE WHEN LEALES_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_3M_20
                ,CASE WHEN LEALES_21 = 1 AND IND_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_3M_21
                ,0 AS LEALES_RETENCION_3M_22
                ,0 AS LEALES_RETENCION_3M_23
                ,0 AS LEALES_RETENCION_3M_24

                --COMPRA EN LOS SIGUIENTES 6 MESES
                ,CASE WHEN LEALES_13 = 1 AND IND_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_6M_13
                ,CASE WHEN LEALES_14 = 1 AND IND_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_6M_14
                ,CASE WHEN LEALES_15 = 1 AND IND_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_6M_15
                ,CASE WHEN LEALES_16 = 1 AND IND_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_6M_16
                ,CASE WHEN LEALES_17 = 1 AND IND_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_6M_17
                ,CASE WHEN LEALES_18 = 1 AND IND_19 = 1 AND IND_20 = 1 AND IND_21 = 1 AND IND_22 = 1 AND IND_23 = 1 AND IND_24 = 1 THEN 1 ELSE 0 END AS LEALES_RETENCION_6M_18
                ,0 AS LEALES_RETENCION_6M_19
                ,0 AS LEALES_RETENCION_6M_20
                ,0 AS LEALES_RETENCION_6M_21
                ,0 AS LEALES_RETENCION_6M_22
                ,0 AS LEALES_RETENCION_6M_23
                ,0 AS LEALES_RETENCION_6M_24

                --VENTAS
                --VENTA RECOMPRA - VENTA DE CLIENTES QUE COMPRARON EL MES ANTERIOR Y TAMBIÉN EL MES ACTUAL
                ,CASE WHEN RECOMPRA_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_RECOMPRA_13
                ,CASE WHEN RECOMPRA_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_RECOMPRA_14
                ,CASE WHEN RECOMPRA_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_RECOMPRA_15
                ,CASE WHEN RECOMPRA_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_RECOMPRA_16
                ,CASE WHEN RECOMPRA_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_RECOMPRA_17
                ,CASE WHEN RECOMPRA_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_RECOMPRA_18
                ,CASE WHEN RECOMPRA_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_RECOMPRA_19
                ,CASE WHEN RECOMPRA_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_RECOMPRA_20
                ,CASE WHEN RECOMPRA_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_RECOMPRA_21
                ,CASE WHEN RECOMPRA_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_RECOMPRA_22
                ,CASE WHEN RECOMPRA_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_RECOMPRA_23
                ,CASE WHEN RECOMPRA_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_RECOMPRA_24
                
                --VENTA FID - VENTA DE CLIENTES QUE CUMPLEN CON EL PO FID Y COMPRARON EN EL MES CORRESPONDIENTE
                ,CASE WHEN FID_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_FID_13
                ,CASE WHEN FID_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_FID_14
                ,CASE WHEN FID_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_FID_15
                ,CASE WHEN FID_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_FID_16
                ,CASE WHEN FID_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_FID_17
                ,CASE WHEN FID_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_FID_18
                ,CASE WHEN FID_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_FID_19
                ,CASE WHEN FID_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_FID_20
                ,CASE WHEN FID_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_FID_21
                ,CASE WHEN FID_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_FID_22
                ,CASE WHEN FID_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_FID_23
                ,CASE WHEN FID_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_FID_24

                --VENTA REC
                ,CASE WHEN REC_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_REC_13
                ,CASE WHEN REC_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_REC_14
                ,CASE WHEN REC_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_REC_15
                ,CASE WHEN REC_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_REC_16
                ,CASE WHEN REC_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_REC_17
                ,CASE WHEN REC_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_REC_18
                ,CASE WHEN REC_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_REC_19
                ,CASE WHEN REC_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_REC_20
                ,CASE WHEN REC_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_REC_21
                ,CASE WHEN REC_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_REC_22
                ,CASE WHEN REC_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_REC_23
                ,CASE WHEN REC_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_REC_24

                --VENTA NUEVOS
                ,CASE WHEN NUEVOS_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_NUEVOS_13
                ,CASE WHEN NUEVOS_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_NUEVOS_14
                ,CASE WHEN NUEVOS_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_NUEVOS_15
                ,CASE WHEN NUEVOS_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_NUEVOS_16
                ,CASE WHEN NUEVOS_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_NUEVOS_17
                ,CASE WHEN NUEVOS_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_NUEVOS_18
                ,CASE WHEN NUEVOS_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_NUEVOS_19
                ,CASE WHEN NUEVOS_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_NUEVOS_20
                ,CASE WHEN NUEVOS_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_NUEVOS_21
                ,CASE WHEN NUEVOS_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_NUEVOS_22
                ,CASE WHEN NUEVOS_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_NUEVOS_23
                ,CASE WHEN NUEVOS_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_NUEVOS_24
                
                --VENTA REPETIDORES
                ,CASE WHEN REPETIDORES_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_REPETIDORES_13
                ,CASE WHEN REPETIDORES_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_REPETIDORES_14
                ,CASE WHEN REPETIDORES_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_REPETIDORES_15
                ,CASE WHEN REPETIDORES_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_REPETIDORES_16
                ,CASE WHEN REPETIDORES_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_REPETIDORES_17
                ,CASE WHEN REPETIDORES_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_REPETIDORES_18
                ,CASE WHEN REPETIDORES_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_REPETIDORES_19
                ,CASE WHEN REPETIDORES_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_REPETIDORES_20
                ,CASE WHEN REPETIDORES_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_REPETIDORES_21
                ,CASE WHEN REPETIDORES_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_REPETIDORES_22
                ,CASE WHEN REPETIDORES_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_REPETIDORES_23
                ,CASE WHEN REPETIDORES_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_REPETIDORES_24
                
                --VENTA LEALES
                ,CASE WHEN LEALES_13 = 1 THEN VENTA_13 ELSE 0 END AS VENTA_LEALES_13
                ,CASE WHEN LEALES_14 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_LEALES_14
                ,CASE WHEN LEALES_15 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_LEALES_15
                ,CASE WHEN LEALES_16 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_LEALES_16
                ,CASE WHEN LEALES_17 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_LEALES_17
                ,CASE WHEN LEALES_18 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_LEALES_18
                ,CASE WHEN LEALES_19 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_LEALES_19
                ,CASE WHEN LEALES_20 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_LEALES_20
                ,CASE WHEN LEALES_21 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_LEALES_21
                ,CASE WHEN LEALES_22 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_LEALES_22
                ,CASE WHEN LEALES_23 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_LEALES_23
                ,CASE WHEN LEALES_24 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_LEALES_24

                --RECOMPRA      
                --COMPRA EN EL SIGUIENTE MES
                ,CASE WHEN RECOMPRA_RETENCION_1M_13 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_1M_13
                ,CASE WHEN RECOMPRA_RETENCION_1M_14 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_1M_14
                ,CASE WHEN RECOMPRA_RETENCION_1M_15 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_1M_15
                ,CASE WHEN RECOMPRA_RETENCION_1M_16 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_1M_16
                ,CASE WHEN RECOMPRA_RETENCION_1M_17 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_1M_17
                ,CASE WHEN RECOMPRA_RETENCION_1M_18 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_1M_18
                ,CASE WHEN RECOMPRA_RETENCION_1M_19 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_1M_19
                ,CASE WHEN RECOMPRA_RETENCION_1M_20 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_1M_20
                ,CASE WHEN RECOMPRA_RETENCION_1M_21 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_1M_21
                ,CASE WHEN RECOMPRA_RETENCION_1M_22 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_1M_22
                ,CASE WHEN RECOMPRA_RETENCION_1M_23 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_1M_23
                ,0 AS VENTA_RECOMPRA_RETENCION_1M_24

                --COMPRA EN LOS SIGUIENTES 2 MESES
                ,CASE WHEN RECOMPRA_RETENCION_2M_13 = 1 THEN VENTA_14 + VENTA_15 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_2M_13
                ,CASE WHEN RECOMPRA_RETENCION_2M_14 = 1 THEN VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_2M_14
                ,CASE WHEN RECOMPRA_RETENCION_2M_15 = 1 THEN VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_2M_15
                ,CASE WHEN RECOMPRA_RETENCION_2M_16 = 1 THEN VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_2M_16
                ,CASE WHEN RECOMPRA_RETENCION_2M_17 = 1 THEN VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_2M_17
                ,CASE WHEN RECOMPRA_RETENCION_2M_18 = 1 THEN VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_2M_18
                ,CASE WHEN RECOMPRA_RETENCION_2M_19 = 1 THEN VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_2M_19
                ,CASE WHEN RECOMPRA_RETENCION_2M_20 = 1 THEN VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_2M_20
                ,CASE WHEN RECOMPRA_RETENCION_2M_21 = 1 THEN VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_2M_21
                ,CASE WHEN RECOMPRA_RETENCION_2M_22 = 1 THEN VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_2M_22
                ,0 AS VENTA_RECOMPRA_RETENCION_2M_23
                ,0 AS VENTA_RECOMPRA_RETENCION_2M_24

                --COMPRA EN LOS SIGUIENTES 3 MESES
                ,CASE WHEN RECOMPRA_RETENCION_3M_13 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_3M_13
                ,CASE WHEN RECOMPRA_RETENCION_3M_14 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_3M_14
                ,CASE WHEN RECOMPRA_RETENCION_3M_15 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_3M_15
                ,CASE WHEN RECOMPRA_RETENCION_3M_16 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_3M_16
                ,CASE WHEN RECOMPRA_RETENCION_3M_17 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_3M_17
                ,CASE WHEN RECOMPRA_RETENCION_3M_18 = 1 THEN VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_3M_18
                ,CASE WHEN RECOMPRA_RETENCION_3M_19 = 1 THEN VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_3M_19
                ,CASE WHEN RECOMPRA_RETENCION_3M_20 = 1 THEN VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_3M_20
                ,CASE WHEN RECOMPRA_RETENCION_3M_21 = 1 THEN VENTA_22 + VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_3M_21
                ,0 AS VENTA_RECOMPRA_RETENCION_3M_22
                ,0 AS VENTA_RECOMPRA_RETENCION_3M_23
                ,0 AS VENTA_RECOMPRA_RETENCION_3M_24

                --COMPRA EN LOS SIGUIENTES 6 MESES
                ,CASE WHEN RECOMPRA_RETENCION_6M_13 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_6M_13
                ,CASE WHEN RECOMPRA_RETENCION_6M_14 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_6M_14
                ,CASE WHEN RECOMPRA_RETENCION_6M_15 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_6M_15
                ,CASE WHEN RECOMPRA_RETENCION_6M_16 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_6M_16
                ,CASE WHEN RECOMPRA_RETENCION_6M_17 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_6M_17
                ,CASE WHEN RECOMPRA_RETENCION_6M_18 = 1 THEN VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_RECOMPRA_RETENCION_6M_18
                ,0 AS VENTA_RECOMPRA_RETENCION_6M_19
                ,0 AS VENTA_RECOMPRA_RETENCION_6M_20
                ,0 AS VENTA_RECOMPRA_RETENCION_6M_21
                ,0 AS VENTA_RECOMPRA_RETENCION_6M_22
                ,0 AS VENTA_RECOMPRA_RETENCION_6M_23
                ,0 AS VENTA_RECOMPRA_RETENCION_6M_24
                
                --FID
                --COMPRA EN EL SIGUIENTE MES
                ,CASE WHEN FID_RETENCION_1M_13 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_FID_RETENCION_1M_13
                ,CASE WHEN FID_RETENCION_1M_14 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_FID_RETENCION_1M_14
                ,CASE WHEN FID_RETENCION_1M_15 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_FID_RETENCION_1M_15
                ,CASE WHEN FID_RETENCION_1M_16 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_FID_RETENCION_1M_16
                ,CASE WHEN FID_RETENCION_1M_17 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_FID_RETENCION_1M_17
                ,CASE WHEN FID_RETENCION_1M_18 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_FID_RETENCION_1M_18
                ,CASE WHEN FID_RETENCION_1M_19 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_FID_RETENCION_1M_19
                ,CASE WHEN FID_RETENCION_1M_20 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_FID_RETENCION_1M_20
                ,CASE WHEN FID_RETENCION_1M_21 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_FID_RETENCION_1M_21
                ,CASE WHEN FID_RETENCION_1M_22 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_FID_RETENCION_1M_22
                ,CASE WHEN FID_RETENCION_1M_23 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_FID_RETENCION_1M_23
                ,0 AS VENTA_FID_RETENCION_1M_24

                --COMPRA EN LOS SIGUIENTES 2 MESES
                ,CASE WHEN FID_RETENCION_2M_13 = 1 THEN VENTA_14 + VENTA_15 ELSE 0 END AS VENTA_FID_RETENCION_2M_13
                ,CASE WHEN FID_RETENCION_2M_14 = 1 THEN VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_FID_RETENCION_2M_14
                ,CASE WHEN FID_RETENCION_2M_15 = 1 THEN VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_FID_RETENCION_2M_15
                ,CASE WHEN FID_RETENCION_2M_16 = 1 THEN VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_FID_RETENCION_2M_16
                ,CASE WHEN FID_RETENCION_2M_17 = 1 THEN VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_FID_RETENCION_2M_17
                ,CASE WHEN FID_RETENCION_2M_18 = 1 THEN VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_FID_RETENCION_2M_18
                ,CASE WHEN FID_RETENCION_2M_19 = 1 THEN VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_FID_RETENCION_2M_19
                ,CASE WHEN FID_RETENCION_2M_20 = 1 THEN VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_FID_RETENCION_2M_20
                ,CASE WHEN FID_RETENCION_2M_21 = 1 THEN VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_FID_RETENCION_2M_21
                ,CASE WHEN FID_RETENCION_2M_22 = 1 THEN VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_FID_RETENCION_2M_22
                ,0 AS VENTA_FID_RETENCION_2M_23
                ,0 AS VENTA_FID_RETENCION_2M_24

                --COMPRA EN LOS SIGUIENTES 3 MESES
                ,CASE WHEN FID_RETENCION_3M_13 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_FID_RETENCION_3M_13
                ,CASE WHEN FID_RETENCION_3M_14 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_FID_RETENCION_3M_14
                ,CASE WHEN FID_RETENCION_3M_15 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_FID_RETENCION_3M_15
                ,CASE WHEN FID_RETENCION_3M_16 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_FID_RETENCION_3M_16
                ,CASE WHEN FID_RETENCION_3M_17 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_FID_RETENCION_3M_17
                ,CASE WHEN FID_RETENCION_3M_18 = 1 THEN VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_FID_RETENCION_3M_18
                ,CASE WHEN FID_RETENCION_3M_19 = 1 THEN VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_FID_RETENCION_3M_19
                ,CASE WHEN FID_RETENCION_3M_20 = 1 THEN VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_FID_RETENCION_3M_20
                ,CASE WHEN FID_RETENCION_3M_21 = 1 THEN VENTA_22 + VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_FID_RETENCION_3M_21
                ,0 AS VENTA_FID_RETENCION_3M_22
                ,0 AS VENTA_FID_RETENCION_3M_23
                ,0 AS VENTA_FID_RETENCION_3M_24

                --COMPRA EN LOS SIGUIENTES 6 MESES
                ,CASE WHEN FID_RETENCION_6M_13 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_FID_RETENCION_6M_13
                ,CASE WHEN FID_RETENCION_6M_14 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_FID_RETENCION_6M_14
                ,CASE WHEN FID_RETENCION_6M_15 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_FID_RETENCION_6M_15
                ,CASE WHEN FID_RETENCION_6M_16 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_FID_RETENCION_6M_16
                ,CASE WHEN FID_RETENCION_6M_17 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_FID_RETENCION_6M_17
                ,CASE WHEN FID_RETENCION_6M_18 = 1 THEN VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_FID_RETENCION_6M_18
                ,0 AS VENTA_FID_RETENCION_6M_19
                ,0 AS VENTA_FID_RETENCION_6M_20
                ,0 AS VENTA_FID_RETENCION_6M_21
                ,0 AS VENTA_FID_RETENCION_6M_22
                ,0 AS VENTA_FID_RETENCION_6M_23
                ,0 AS VENTA_FID_RETENCION_6M_24

                --REC
                --COMPRA EN EL SIGUIENTE MES
                ,CASE WHEN REC_RETENCION_1M_13 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_REC_RETENCION_1M_13
                ,CASE WHEN REC_RETENCION_1M_14 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_REC_RETENCION_1M_14
                ,CASE WHEN REC_RETENCION_1M_15 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_REC_RETENCION_1M_15
                ,CASE WHEN REC_RETENCION_1M_16 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_REC_RETENCION_1M_16
                ,CASE WHEN REC_RETENCION_1M_17 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_REC_RETENCION_1M_17
                ,CASE WHEN REC_RETENCION_1M_18 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_REC_RETENCION_1M_18
                ,CASE WHEN REC_RETENCION_1M_19 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_REC_RETENCION_1M_19
                ,CASE WHEN REC_RETENCION_1M_20 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_REC_RETENCION_1M_20
                ,CASE WHEN REC_RETENCION_1M_21 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_REC_RETENCION_1M_21
                ,CASE WHEN REC_RETENCION_1M_22 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_REC_RETENCION_1M_22
                ,CASE WHEN REC_RETENCION_1M_23 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_REC_RETENCION_1M_23
                ,0 AS VENTA_REC_RETENCION_1M_24

                --COMPRA EN LOS SIGUIENTES 2 MESES
                ,CASE WHEN REC_RETENCION_2M_13 = 1 THEN VENTA_14 + VENTA_15 ELSE 0 END AS VENTA_REC_RETENCION_2M_13
                ,CASE WHEN REC_RETENCION_2M_14 = 1 THEN VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_REC_RETENCION_2M_14
                ,CASE WHEN REC_RETENCION_2M_15 = 1 THEN VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_REC_RETENCION_2M_15
                ,CASE WHEN REC_RETENCION_2M_16 = 1 THEN VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_REC_RETENCION_2M_16
                ,CASE WHEN REC_RETENCION_2M_17 = 1 THEN VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_REC_RETENCION_2M_17
                ,CASE WHEN REC_RETENCION_2M_18 = 1 THEN VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_REC_RETENCION_2M_18
                ,CASE WHEN REC_RETENCION_2M_19 = 1 THEN VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_REC_RETENCION_2M_19
                ,CASE WHEN REC_RETENCION_2M_20 = 1 THEN VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_REC_RETENCION_2M_20
                ,CASE WHEN REC_RETENCION_2M_21 = 1 THEN VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_REC_RETENCION_2M_21
                ,CASE WHEN REC_RETENCION_2M_22 = 1 THEN VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_REC_RETENCION_2M_22
                ,0 AS VENTA_REC_RETENCION_2M_23
                ,0 AS VENTA_REC_RETENCION_2M_24

                --COMPRA EN LOS SIGUIENTES 3 MESES
                ,CASE WHEN REC_RETENCION_3M_13 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_REC_RETENCION_3M_13
                ,CASE WHEN REC_RETENCION_3M_14 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_REC_RETENCION_3M_14
                ,CASE WHEN REC_RETENCION_3M_15 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_REC_RETENCION_3M_15
                ,CASE WHEN REC_RETENCION_3M_16 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_REC_RETENCION_3M_16
                ,CASE WHEN REC_RETENCION_3M_17 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_REC_RETENCION_3M_17
                ,CASE WHEN REC_RETENCION_3M_18 = 1 THEN VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_REC_RETENCION_3M_18
                ,CASE WHEN REC_RETENCION_3M_19 = 1 THEN VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_REC_RETENCION_3M_19
                ,CASE WHEN REC_RETENCION_3M_20 = 1 THEN VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_REC_RETENCION_3M_20
                ,CASE WHEN REC_RETENCION_3M_21 = 1 THEN VENTA_22 + VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_REC_RETENCION_3M_21
                ,0 AS VENTA_REC_RETENCION_3M_22
                ,0 AS VENTA_REC_RETENCION_3M_23
                ,0 AS VENTA_REC_RETENCION_3M_24

                --COMPRA EN LOS SIGUIENTES 6 MESES
                ,CASE WHEN REC_RETENCION_6M_13 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_REC_RETENCION_6M_13
                ,CASE WHEN REC_RETENCION_6M_14 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_REC_RETENCION_6M_14
                ,CASE WHEN REC_RETENCION_6M_15 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_REC_RETENCION_6M_15
                ,CASE WHEN REC_RETENCION_6M_16 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_REC_RETENCION_6M_16
                ,CASE WHEN REC_RETENCION_6M_17 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_REC_RETENCION_6M_17
                ,CASE WHEN REC_RETENCION_6M_18 = 1 THEN VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_REC_RETENCION_6M_18
                ,0 AS VENTA_REC_RETENCION_6M_19
                ,0 AS VENTA_REC_RETENCION_6M_20
                ,0 AS VENTA_REC_RETENCION_6M_21
                ,0 AS VENTA_REC_RETENCION_6M_22
                ,0 AS VENTA_REC_RETENCION_6M_23
                ,0 AS VENTA_REC_RETENCION_6M_24

                --NUEVOS
                --COMPRA EN EL SIGUIENTE MES
                ,CASE WHEN NUEVOS_RETENCION_1M_13 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_NUEVOS_RETENCION_1M_13
                ,CASE WHEN NUEVOS_RETENCION_1M_14 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_NUEVOS_RETENCION_1M_14
                ,CASE WHEN NUEVOS_RETENCION_1M_15 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_NUEVOS_RETENCION_1M_15
                ,CASE WHEN NUEVOS_RETENCION_1M_16 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_NUEVOS_RETENCION_1M_16
                ,CASE WHEN NUEVOS_RETENCION_1M_17 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_NUEVOS_RETENCION_1M_17
                ,CASE WHEN NUEVOS_RETENCION_1M_18 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_NUEVOS_RETENCION_1M_18
                ,CASE WHEN NUEVOS_RETENCION_1M_19 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_NUEVOS_RETENCION_1M_19
                ,CASE WHEN NUEVOS_RETENCION_1M_20 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_NUEVOS_RETENCION_1M_20
                ,CASE WHEN NUEVOS_RETENCION_1M_21 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_NUEVOS_RETENCION_1M_21
                ,CASE WHEN NUEVOS_RETENCION_1M_22 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_NUEVOS_RETENCION_1M_22
                ,CASE WHEN NUEVOS_RETENCION_1M_23 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_NUEVOS_RETENCION_1M_23
                ,0 AS VENTA_NUEVOS_RETENCION_1M_24

                --COMPRA EN LOS SIGUIENTES 2 MESES
                ,CASE WHEN NUEVOS_RETENCION_2M_13 = 1 THEN VENTA_14 + VENTA_15 ELSE 0 END AS VENTA_NUEVOS_RETENCION_2M_13
                ,CASE WHEN NUEVOS_RETENCION_2M_14 = 1 THEN VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_NUEVOS_RETENCION_2M_14
                ,CASE WHEN NUEVOS_RETENCION_2M_15 = 1 THEN VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_NUEVOS_RETENCION_2M_15
                ,CASE WHEN NUEVOS_RETENCION_2M_16 = 1 THEN VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_NUEVOS_RETENCION_2M_16
                ,CASE WHEN NUEVOS_RETENCION_2M_17 = 1 THEN VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_NUEVOS_RETENCION_2M_17
                ,CASE WHEN NUEVOS_RETENCION_2M_18 = 1 THEN VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_NUEVOS_RETENCION_2M_18
                ,CASE WHEN NUEVOS_RETENCION_2M_19 = 1 THEN VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_NUEVOS_RETENCION_2M_19
                ,CASE WHEN NUEVOS_RETENCION_2M_20 = 1 THEN VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_NUEVOS_RETENCION_2M_20
                ,CASE WHEN NUEVOS_RETENCION_2M_21 = 1 THEN VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_NUEVOS_RETENCION_2M_21
                ,CASE WHEN NUEVOS_RETENCION_2M_22 = 1 THEN VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_NUEVOS_RETENCION_2M_22
                ,0 AS VENTA_NUEVOS_RETENCION_2M_23
                ,0 AS VENTA_NUEVOS_RETENCION_2M_24

                --COMPRA EN LOS SIGUIENTES 3 MESES
                ,CASE WHEN NUEVOS_RETENCION_3M_13 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_NUEVOS_RETENCION_3M_13
                ,CASE WHEN NUEVOS_RETENCION_3M_14 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_NUEVOS_RETENCION_3M_14
                ,CASE WHEN NUEVOS_RETENCION_3M_15 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_NUEVOS_RETENCION_3M_15
                ,CASE WHEN NUEVOS_RETENCION_3M_16 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_NUEVOS_RETENCION_3M_16
                ,CASE WHEN NUEVOS_RETENCION_3M_17 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_NUEVOS_RETENCION_3M_17
                ,CASE WHEN NUEVOS_RETENCION_3M_18 = 1 THEN VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_NUEVOS_RETENCION_3M_18
                ,CASE WHEN NUEVOS_RETENCION_3M_19 = 1 THEN VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_NUEVOS_RETENCION_3M_19
                ,CASE WHEN NUEVOS_RETENCION_3M_20 = 1 THEN VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_NUEVOS_RETENCION_3M_20
                ,CASE WHEN NUEVOS_RETENCION_3M_21 = 1 THEN VENTA_22 + VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_NUEVOS_RETENCION_3M_21
                ,0 AS VENTA_NUEVOS_RETENCION_3M_22
                ,0 AS VENTA_NUEVOS_RETENCION_3M_23
                ,0 AS VENTA_NUEVOS_RETENCION_3M_24

                --COMPRA EN LOS SIGUIENTES 6 MESES
                ,CASE WHEN NUEVOS_RETENCION_6M_13 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_NUEVOS_RETENCION_6M_13
                ,CASE WHEN NUEVOS_RETENCION_6M_14 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_NUEVOS_RETENCION_6M_14
                ,CASE WHEN NUEVOS_RETENCION_6M_15 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_NUEVOS_RETENCION_6M_15
                ,CASE WHEN NUEVOS_RETENCION_6M_16 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_NUEVOS_RETENCION_6M_16
                ,CASE WHEN NUEVOS_RETENCION_6M_17 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_NUEVOS_RETENCION_6M_17
                ,CASE WHEN NUEVOS_RETENCION_6M_18 = 1 THEN VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_NUEVOS_RETENCION_6M_18
                ,0 AS VENTA_NUEVOS_RETENCION_6M_19
                ,0 AS VENTA_NUEVOS_RETENCION_6M_20
                ,0 AS VENTA_NUEVOS_RETENCION_6M_21
                ,0 AS VENTA_NUEVOS_RETENCION_6M_22
                ,0 AS VENTA_NUEVOS_RETENCION_6M_23
                ,0 AS VENTA_NUEVOS_RETENCION_6M_24

                --REPETIDORES
                --COMPRA EN EL SIGUIENTE MES
                ,CASE WHEN REPETIDORES_RETENCION_1M_13 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_1M_13
                ,CASE WHEN REPETIDORES_RETENCION_1M_14 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_1M_14
                ,CASE WHEN REPETIDORES_RETENCION_1M_15 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_1M_15
                ,CASE WHEN REPETIDORES_RETENCION_1M_16 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_1M_16
                ,CASE WHEN REPETIDORES_RETENCION_1M_17 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_1M_17
                ,CASE WHEN REPETIDORES_RETENCION_1M_18 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_1M_18
                ,CASE WHEN REPETIDORES_RETENCION_1M_19 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_1M_19
                ,CASE WHEN REPETIDORES_RETENCION_1M_20 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_1M_20
                ,CASE WHEN REPETIDORES_RETENCION_1M_21 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_1M_21
                ,CASE WHEN REPETIDORES_RETENCION_1M_22 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_1M_22
                ,CASE WHEN REPETIDORES_RETENCION_1M_23 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_1M_23
                ,0 AS VENTA_REPETIDORES_RETENCION_1M_24

                --COMPRA EN LOS SIGUIENTES 2 MESES
                ,CASE WHEN REPETIDORES_RETENCION_2M_13 = 1 THEN VENTA_14 + VENTA_15 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_2M_13
                ,CASE WHEN REPETIDORES_RETENCION_2M_14 = 1 THEN VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_2M_14
                ,CASE WHEN REPETIDORES_RETENCION_2M_15 = 1 THEN VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_2M_15
                ,CASE WHEN REPETIDORES_RETENCION_2M_16 = 1 THEN VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_2M_16
                ,CASE WHEN REPETIDORES_RETENCION_2M_17 = 1 THEN VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_2M_17
                ,CASE WHEN REPETIDORES_RETENCION_2M_18 = 1 THEN VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_2M_18
                ,CASE WHEN REPETIDORES_RETENCION_2M_19 = 1 THEN VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_2M_19
                ,CASE WHEN REPETIDORES_RETENCION_2M_20 = 1 THEN VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_2M_20
                ,CASE WHEN REPETIDORES_RETENCION_2M_21 = 1 THEN VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_2M_21
                ,CASE WHEN REPETIDORES_RETENCION_2M_22 = 1 THEN VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_2M_22
                ,0 AS VENTA_REPETIDORES_RETENCION_2M_23
                ,0 AS VENTA_REPETIDORES_RETENCION_2M_24
                
                --COMPRA EN LOS SIGUIENTES 3 MESES
                ,CASE WHEN REPETIDORES_RETENCION_3M_13 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_3M_13
                ,CASE WHEN REPETIDORES_RETENCION_3M_14 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_3M_14
                ,CASE WHEN REPETIDORES_RETENCION_3M_15 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_3M_15
                ,CASE WHEN REPETIDORES_RETENCION_3M_16 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_3M_16
                ,CASE WHEN REPETIDORES_RETENCION_3M_17 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_3M_17
                ,CASE WHEN REPETIDORES_RETENCION_3M_18 = 1 THEN VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_3M_18
                ,CASE WHEN REPETIDORES_RETENCION_3M_19 = 1 THEN VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_3M_19
                ,CASE WHEN REPETIDORES_RETENCION_3M_20 = 1 THEN VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_3M_20
                ,CASE WHEN REPETIDORES_RETENCION_3M_21 = 1 THEN VENTA_22 + VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_3M_21
                ,0 AS VENTA_REPETIDORES_RETENCION_3M_22
                ,0 AS VENTA_REPETIDORES_RETENCION_3M_23
                ,0 AS VENTA_REPETIDORES_RETENCION_3M_24

                --COMPRA EN LOS SIGUIENTES 6 MESES
                ,CASE WHEN REPETIDORES_RETENCION_6M_13 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_6M_13
                ,CASE WHEN REPETIDORES_RETENCION_6M_14 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_6M_14
                ,CASE WHEN REPETIDORES_RETENCION_6M_15 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_6M_15
                ,CASE WHEN REPETIDORES_RETENCION_6M_16 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_6M_16
                ,CASE WHEN REPETIDORES_RETENCION_6M_17 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_6M_17
                ,CASE WHEN REPETIDORES_RETENCION_6M_18 = 1 THEN VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_REPETIDORES_RETENCION_6M_18
                ,0 AS VENTA_REPETIDORES_RETENCION_6M_19
                ,0 AS VENTA_REPETIDORES_RETENCION_6M_20
                ,0 AS VENTA_REPETIDORES_RETENCION_6M_21
                ,0 AS VENTA_REPETIDORES_RETENCION_6M_22
                ,0 AS VENTA_REPETIDORES_RETENCION_6M_23
                ,0 AS VENTA_REPETIDORES_RETENCION_6M_24

                --LEALES
                --COMPRA EN EL SIGUIENTE MES
                ,CASE WHEN LEALES_RETENCION_1M_13 = 1 THEN VENTA_14 ELSE 0 END AS VENTA_LEALES_RETENCION_1M_13
                ,CASE WHEN LEALES_RETENCION_1M_14 = 1 THEN VENTA_15 ELSE 0 END AS VENTA_LEALES_RETENCION_1M_14
                ,CASE WHEN LEALES_RETENCION_1M_15 = 1 THEN VENTA_16 ELSE 0 END AS VENTA_LEALES_RETENCION_1M_15
                ,CASE WHEN LEALES_RETENCION_1M_16 = 1 THEN VENTA_17 ELSE 0 END AS VENTA_LEALES_RETENCION_1M_16
                ,CASE WHEN LEALES_RETENCION_1M_17 = 1 THEN VENTA_18 ELSE 0 END AS VENTA_LEALES_RETENCION_1M_17
                ,CASE WHEN LEALES_RETENCION_1M_18 = 1 THEN VENTA_19 ELSE 0 END AS VENTA_LEALES_RETENCION_1M_18
                ,CASE WHEN LEALES_RETENCION_1M_19 = 1 THEN VENTA_20 ELSE 0 END AS VENTA_LEALES_RETENCION_1M_19
                ,CASE WHEN LEALES_RETENCION_1M_20 = 1 THEN VENTA_21 ELSE 0 END AS VENTA_LEALES_RETENCION_1M_20
                ,CASE WHEN LEALES_RETENCION_1M_21 = 1 THEN VENTA_22 ELSE 0 END AS VENTA_LEALES_RETENCION_1M_21
                ,CASE WHEN LEALES_RETENCION_1M_22 = 1 THEN VENTA_23 ELSE 0 END AS VENTA_LEALES_RETENCION_1M_22
                ,CASE WHEN LEALES_RETENCION_1M_23 = 1 THEN VENTA_24 ELSE 0 END AS VENTA_LEALES_RETENCION_1M_23
                ,0 AS VENTA_LEALES_RETENCION_1M_24

                --COMPRA EN LOS SIGUIENTES 2 MESES
                ,CASE WHEN LEALES_RETENCION_2M_13 = 1 THEN VENTA_14 + VENTA_15 ELSE 0 END AS VENTA_LEALES_RETENCION_2M_13
                ,CASE WHEN LEALES_RETENCION_2M_14 = 1 THEN VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_LEALES_RETENCION_2M_14
                ,CASE WHEN LEALES_RETENCION_2M_15 = 1 THEN VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_LEALES_RETENCION_2M_15
                ,CASE WHEN LEALES_RETENCION_2M_16 = 1 THEN VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_LEALES_RETENCION_2M_16
                ,CASE WHEN LEALES_RETENCION_2M_17 = 1 THEN VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_LEALES_RETENCION_2M_17
                ,CASE WHEN LEALES_RETENCION_2M_18 = 1 THEN VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_LEALES_RETENCION_2M_18
                ,CASE WHEN LEALES_RETENCION_2M_19 = 1 THEN VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_LEALES_RETENCION_2M_19
                ,CASE WHEN LEALES_RETENCION_2M_20 = 1 THEN VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_LEALES_RETENCION_2M_20
                ,CASE WHEN LEALES_RETENCION_2M_21 = 1 THEN VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_LEALES_RETENCION_2M_21
                ,CASE WHEN LEALES_RETENCION_2M_22 = 1 THEN VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_LEALES_RETENCION_2M_22
                ,0 AS VENTA_LEALES_RETENCION_2M_23
                ,0 AS VENTA_LEALES_RETENCION_2M_24

                --COMPRA EN LOS SIGUIENTES 3 MESES
                ,CASE WHEN LEALES_RETENCION_3M_13 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 ELSE 0 END AS VENTA_LEALES_RETENCION_3M_13
                ,CASE WHEN LEALES_RETENCION_3M_14 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 ELSE 0 END AS VENTA_LEALES_RETENCION_3M_14
                ,CASE WHEN LEALES_RETENCION_3M_15 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 ELSE 0 END AS VENTA_LEALES_RETENCION_3M_15
                ,CASE WHEN LEALES_RETENCION_3M_16 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_LEALES_RETENCION_3M_16
                ,CASE WHEN LEALES_RETENCION_3M_17 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_LEALES_RETENCION_3M_17
                ,CASE WHEN LEALES_RETENCION_3M_18 = 1 THEN VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_LEALES_RETENCION_3M_18
                ,CASE WHEN LEALES_RETENCION_3M_19 = 1 THEN VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_LEALES_RETENCION_3M_19
                ,CASE WHEN LEALES_RETENCION_3M_20 = 1 THEN VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_LEALES_RETENCION_3M_20
                ,CASE WHEN LEALES_RETENCION_3M_21 = 1 THEN VENTA_22 + VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_LEALES_RETENCION_3M_21
                ,0 AS VENTA_LEALES_RETENCION_3M_22
                ,0 AS VENTA_LEALES_RETENCION_3M_23
                ,0 AS VENTA_LEALES_RETENCION_3M_24

                --COMPRA EN LOS SIGUIENTES 6 MESES
                ,CASE WHEN LEALES_RETENCION_6M_13 = 1 THEN VENTA_14 + VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 ELSE 0 END AS VENTA_LEALES_RETENCION_6M_13
                ,CASE WHEN LEALES_RETENCION_6M_14 = 1 THEN VENTA_15 + VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 ELSE 0 END AS VENTA_LEALES_RETENCION_6M_14
                ,CASE WHEN LEALES_RETENCION_6M_15 = 1 THEN VENTA_16 + VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 ELSE 0 END AS VENTA_LEALES_RETENCION_6M_15
                ,CASE WHEN LEALES_RETENCION_6M_16 = 1 THEN VENTA_17 + VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 ELSE 0 END AS VENTA_LEALES_RETENCION_6M_16
                ,CASE WHEN LEALES_RETENCION_6M_17 = 1 THEN VENTA_18 + VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 ELSE 0 END AS VENTA_LEALES_RETENCION_6M_17
                ,CASE WHEN LEALES_RETENCION_6M_18 = 1 THEN VENTA_19 + VENTA_20 + VENTA_21 + VENTA_22 + VENTA_23 + VENTA_24 ELSE 0 END AS VENTA_LEALES_RETENCION_6M_18
                ,0 AS VENTA_LEALES_RETENCION_6M_19
                ,0 AS VENTA_LEALES_RETENCION_6M_20
                ,0 AS VENTA_LEALES_RETENCION_6M_21
                ,0 AS VENTA_LEALES_RETENCION_6M_22
                ,0 AS VENTA_LEALES_RETENCION_6M_23
                ,0 AS VENTA_LEALES_RETENCION_6M_24  

                FROM __INDICADORES
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
                
                --VENTA
                ,SUM(VENTA_13) VENTA_13
                ,SUM(VENTA_14) VENTA_14
                ,SUM(VENTA_15) VENTA_15
                ,SUM(VENTA_16) VENTA_16
                ,SUM(VENTA_17) VENTA_17
                ,SUM(VENTA_18) VENTA_18
                ,SUM(VENTA_19) VENTA_19
                ,SUM(VENTA_20) VENTA_20
                ,SUM(VENTA_21) VENTA_21
                ,SUM(VENTA_22) VENTA_22
                ,SUM(VENTA_23) VENTA_23
                ,SUM(VENTA_24) VENTA_24
                
                --TX
                ,SUM(TX_13) TX_13
                ,SUM(TX_14) TX_14
                ,SUM(TX_15) TX_15
                ,SUM(TX_16) TX_16
                ,SUM(TX_17) TX_17
                ,SUM(TX_18) TX_18
                ,SUM(TX_19) TX_19
                ,SUM(TX_20) TX_20
                ,SUM(TX_21) TX_21
                ,SUM(TX_22) TX_22
                ,SUM(TX_23) TX_23
                ,SUM(TX_24) TX_24
                
                --SEGMENTOS CON COMPRA
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
                
                --REPETIDORES
                ,SUM(REPETIDORES_13) REPETIDORES_13
                ,SUM(REPETIDORES_14) REPETIDORES_14
                ,SUM(REPETIDORES_15) REPETIDORES_15
                ,SUM(REPETIDORES_16) REPETIDORES_16
                ,SUM(REPETIDORES_17) REPETIDORES_17
                ,SUM(REPETIDORES_18) REPETIDORES_18
                ,SUM(REPETIDORES_19) REPETIDORES_19
                ,SUM(REPETIDORES_20) REPETIDORES_20
                ,SUM(REPETIDORES_21) REPETIDORES_21
                ,SUM(REPETIDORES_22) REPETIDORES_22
                ,SUM(REPETIDORES_23) REPETIDORES_23
                ,SUM(REPETIDORES_24) REPETIDORES_24

                --LEALES
                ,SUM(LEALES_13) LEALES_13
                ,SUM(LEALES_14) LEALES_14
                ,SUM(LEALES_15) LEALES_15
                ,SUM(LEALES_16) LEALES_16
                ,SUM(LEALES_17) LEALES_17
                ,SUM(LEALES_18) LEALES_18
                ,SUM(LEALES_19) LEALES_19
                ,SUM(LEALES_20) LEALES_20
                ,SUM(LEALES_21) LEALES_21
                ,SUM(LEALES_22) LEALES_22
                ,SUM(LEALES_23) LEALES_23
                ,SUM(LEALES_24) LEALES_24

                --RETENCION
                --RECOMPRA
                --1M
                ,SUM(RECOMPRA_RETENCION_1M_13) RECOMPRA_RETENCION_1M_13
                ,SUM(RECOMPRA_RETENCION_1M_14) RECOMPRA_RETENCION_1M_14
                ,SUM(RECOMPRA_RETENCION_1M_15) RECOMPRA_RETENCION_1M_15
                ,SUM(RECOMPRA_RETENCION_1M_16) RECOMPRA_RETENCION_1M_16
                ,SUM(RECOMPRA_RETENCION_1M_17) RECOMPRA_RETENCION_1M_17
                ,SUM(RECOMPRA_RETENCION_1M_18) RECOMPRA_RETENCION_1M_18
                ,SUM(RECOMPRA_RETENCION_1M_19) RECOMPRA_RETENCION_1M_19
                ,SUM(RECOMPRA_RETENCION_1M_20) RECOMPRA_RETENCION_1M_20
                ,SUM(RECOMPRA_RETENCION_1M_21) RECOMPRA_RETENCION_1M_21
                ,SUM(RECOMPRA_RETENCION_1M_22) RECOMPRA_RETENCION_1M_22
                ,SUM(RECOMPRA_RETENCION_1M_23) RECOMPRA_RETENCION_1M_23
                ,SUM(RECOMPRA_RETENCION_1M_24) RECOMPRA_RETENCION_1M_24

                --2M
                ,SUM(RECOMPRA_RETENCION_2M_13) RECOMPRA_RETENCION_2M_13
                ,SUM(RECOMPRA_RETENCION_2M_14) RECOMPRA_RETENCION_2M_14
                ,SUM(RECOMPRA_RETENCION_2M_15) RECOMPRA_RETENCION_2M_15
                ,SUM(RECOMPRA_RETENCION_2M_16) RECOMPRA_RETENCION_2M_16
                ,SUM(RECOMPRA_RETENCION_2M_17) RECOMPRA_RETENCION_2M_17
                ,SUM(RECOMPRA_RETENCION_2M_18) RECOMPRA_RETENCION_2M_18
                ,SUM(RECOMPRA_RETENCION_2M_19) RECOMPRA_RETENCION_2M_19
                ,SUM(RECOMPRA_RETENCION_2M_20) RECOMPRA_RETENCION_2M_20
                ,SUM(RECOMPRA_RETENCION_2M_21) RECOMPRA_RETENCION_2M_21
                ,SUM(RECOMPRA_RETENCION_2M_22) RECOMPRA_RETENCION_2M_22
                ,SUM(RECOMPRA_RETENCION_2M_23) RECOMPRA_RETENCION_2M_23
                ,SUM(RECOMPRA_RETENCION_2M_24) RECOMPRA_RETENCION_2M_24

                --3M
                ,SUM(RECOMPRA_RETENCION_3M_13) RECOMPRA_RETENCION_3M_13
                ,SUM(RECOMPRA_RETENCION_3M_14) RECOMPRA_RETENCION_3M_14
                ,SUM(RECOMPRA_RETENCION_3M_15) RECOMPRA_RETENCION_3M_15
                ,SUM(RECOMPRA_RETENCION_3M_16) RECOMPRA_RETENCION_3M_16
                ,SUM(RECOMPRA_RETENCION_3M_17) RECOMPRA_RETENCION_3M_17
                ,SUM(RECOMPRA_RETENCION_3M_18) RECOMPRA_RETENCION_3M_18
                ,SUM(RECOMPRA_RETENCION_3M_19) RECOMPRA_RETENCION_3M_19
                ,SUM(RECOMPRA_RETENCION_3M_20) RECOMPRA_RETENCION_3M_20
                ,SUM(RECOMPRA_RETENCION_3M_21) RECOMPRA_RETENCION_3M_21
                ,SUM(RECOMPRA_RETENCION_3M_22) RECOMPRA_RETENCION_3M_22
                ,SUM(RECOMPRA_RETENCION_3M_23) RECOMPRA_RETENCION_3M_23
                ,SUM(RECOMPRA_RETENCION_3M_24) RECOMPRA_RETENCION_3M_24

                --6M
                ,SUM(RECOMPRA_RETENCION_6M_13) RECOMPRA_RETENCION_6M_13
                ,SUM(RECOMPRA_RETENCION_6M_14) RECOMPRA_RETENCION_6M_14
                ,SUM(RECOMPRA_RETENCION_6M_15) RECOMPRA_RETENCION_6M_15
                ,SUM(RECOMPRA_RETENCION_6M_16) RECOMPRA_RETENCION_6M_16
                ,SUM(RECOMPRA_RETENCION_6M_17) RECOMPRA_RETENCION_6M_17
                ,SUM(RECOMPRA_RETENCION_6M_18) RECOMPRA_RETENCION_6M_18
                ,SUM(RECOMPRA_RETENCION_6M_19) RECOMPRA_RETENCION_6M_19
                ,SUM(RECOMPRA_RETENCION_6M_20) RECOMPRA_RETENCION_6M_20
                ,SUM(RECOMPRA_RETENCION_6M_21) RECOMPRA_RETENCION_6M_21
                ,SUM(RECOMPRA_RETENCION_6M_22) RECOMPRA_RETENCION_6M_22
                ,SUM(RECOMPRA_RETENCION_6M_23) RECOMPRA_RETENCION_6M_23
                ,SUM(RECOMPRA_RETENCION_6M_24) RECOMPRA_RETENCION_6M_24
                
                --FID
                --1M
                ,SUM(FID_RETENCION_1M_13) FID_RETENCION_1M_13
                ,SUM(FID_RETENCION_1M_14) FID_RETENCION_1M_14
                ,SUM(FID_RETENCION_1M_15) FID_RETENCION_1M_15
                ,SUM(FID_RETENCION_1M_16) FID_RETENCION_1M_16
                ,SUM(FID_RETENCION_1M_17) FID_RETENCION_1M_17
                ,SUM(FID_RETENCION_1M_18) FID_RETENCION_1M_18
                ,SUM(FID_RETENCION_1M_19) FID_RETENCION_1M_19
                ,SUM(FID_RETENCION_1M_20) FID_RETENCION_1M_20
                ,SUM(FID_RETENCION_1M_21) FID_RETENCION_1M_21
                ,SUM(FID_RETENCION_1M_22) FID_RETENCION_1M_22
                ,SUM(FID_RETENCION_1M_23) FID_RETENCION_1M_23
                ,SUM(FID_RETENCION_1M_24) FID_RETENCION_1M_24

                --2M
                ,SUM(FID_RETENCION_2M_13) FID_RETENCION_2M_13
                ,SUM(FID_RETENCION_2M_14) FID_RETENCION_2M_14
                ,SUM(FID_RETENCION_2M_15) FID_RETENCION_2M_15
                ,SUM(FID_RETENCION_2M_16) FID_RETENCION_2M_16
                ,SUM(FID_RETENCION_2M_17) FID_RETENCION_2M_17
                ,SUM(FID_RETENCION_2M_18) FID_RETENCION_2M_18
                ,SUM(FID_RETENCION_2M_19) FID_RETENCION_2M_19
                ,SUM(FID_RETENCION_2M_20) FID_RETENCION_2M_20
                ,SUM(FID_RETENCION_2M_21) FID_RETENCION_2M_21
                ,SUM(FID_RETENCION_2M_22) FID_RETENCION_2M_22
                ,SUM(FID_RETENCION_2M_23) FID_RETENCION_2M_23
                ,SUM(FID_RETENCION_2M_24) FID_RETENCION_2M_24

                --3M
                ,SUM(FID_RETENCION_3M_13) FID_RETENCION_3M_13
                ,SUM(FID_RETENCION_3M_14) FID_RETENCION_3M_14
                ,SUM(FID_RETENCION_3M_15) FID_RETENCION_3M_15
                ,SUM(FID_RETENCION_3M_16) FID_RETENCION_3M_16
                ,SUM(FID_RETENCION_3M_17) FID_RETENCION_3M_17
                ,SUM(FID_RETENCION_3M_18) FID_RETENCION_3M_18
                ,SUM(FID_RETENCION_3M_19) FID_RETENCION_3M_19
                ,SUM(FID_RETENCION_3M_20) FID_RETENCION_3M_20
                ,SUM(FID_RETENCION_3M_21) FID_RETENCION_3M_21
                ,SUM(FID_RETENCION_3M_22) FID_RETENCION_3M_22
                ,SUM(FID_RETENCION_3M_23) FID_RETENCION_3M_23
                ,SUM(FID_RETENCION_3M_24) FID_RETENCION_3M_24

                --6M
                ,SUM(FID_RETENCION_6M_13) FID_RETENCION_6M_13
                ,SUM(FID_RETENCION_6M_14) FID_RETENCION_6M_14
                ,SUM(FID_RETENCION_6M_15) FID_RETENCION_6M_15
                ,SUM(FID_RETENCION_6M_16) FID_RETENCION_6M_16
                ,SUM(FID_RETENCION_6M_17) FID_RETENCION_6M_17
                ,SUM(FID_RETENCION_6M_18) FID_RETENCION_6M_18
                ,SUM(FID_RETENCION_6M_19) FID_RETENCION_6M_19
                ,SUM(FID_RETENCION_6M_20) FID_RETENCION_6M_20
                ,SUM(FID_RETENCION_6M_21) FID_RETENCION_6M_21
                ,SUM(FID_RETENCION_6M_22) FID_RETENCION_6M_22
                ,SUM(FID_RETENCION_6M_23) FID_RETENCION_6M_23
                ,SUM(FID_RETENCION_6M_24) FID_RETENCION_6M_24

                --REC
                --1M
                ,SUM(REC_RETENCION_1M_13) REC_RETENCION_1M_13
                ,SUM(REC_RETENCION_1M_14) REC_RETENCION_1M_14
                ,SUM(REC_RETENCION_1M_15) REC_RETENCION_1M_15
                ,SUM(REC_RETENCION_1M_16) REC_RETENCION_1M_16
                ,SUM(REC_RETENCION_1M_17) REC_RETENCION_1M_17
                ,SUM(REC_RETENCION_1M_18) REC_RETENCION_1M_18
                ,SUM(REC_RETENCION_1M_19) REC_RETENCION_1M_19
                ,SUM(REC_RETENCION_1M_20) REC_RETENCION_1M_20
                ,SUM(REC_RETENCION_1M_21) REC_RETENCION_1M_21
                ,SUM(REC_RETENCION_1M_22) REC_RETENCION_1M_22
                ,SUM(REC_RETENCION_1M_23) REC_RETENCION_1M_23
                ,SUM(REC_RETENCION_1M_24) REC_RETENCION_1M_24

                --2M
                ,SUM(REC_RETENCION_2M_13) REC_RETENCION_2M_13
                ,SUM(REC_RETENCION_2M_14) REC_RETENCION_2M_14
                ,SUM(REC_RETENCION_2M_15) REC_RETENCION_2M_15
                ,SUM(REC_RETENCION_2M_16) REC_RETENCION_2M_16
                ,SUM(REC_RETENCION_2M_17) REC_RETENCION_2M_17
                ,SUM(REC_RETENCION_2M_18) REC_RETENCION_2M_18
                ,SUM(REC_RETENCION_2M_19) REC_RETENCION_2M_19
                ,SUM(REC_RETENCION_2M_20) REC_RETENCION_2M_20
                ,SUM(REC_RETENCION_2M_21) REC_RETENCION_2M_21
                ,SUM(REC_RETENCION_2M_22) REC_RETENCION_2M_22
                ,SUM(REC_RETENCION_2M_23) REC_RETENCION_2M_23
                ,SUM(REC_RETENCION_2M_24) REC_RETENCION_2M_24

                --3M
                ,SUM(REC_RETENCION_3M_13) REC_RETENCION_3M_13
                ,SUM(REC_RETENCION_3M_14) REC_RETENCION_3M_14
                ,SUM(REC_RETENCION_3M_15) REC_RETENCION_3M_15
                ,SUM(REC_RETENCION_3M_16) REC_RETENCION_3M_16
                ,SUM(REC_RETENCION_3M_17) REC_RETENCION_3M_17
                ,SUM(REC_RETENCION_3M_18) REC_RETENCION_3M_18
                ,SUM(REC_RETENCION_3M_19) REC_RETENCION_3M_19
                ,SUM(REC_RETENCION_3M_20) REC_RETENCION_3M_20
                ,SUM(REC_RETENCION_3M_21) REC_RETENCION_3M_21
                ,SUM(REC_RETENCION_3M_22) REC_RETENCION_3M_22
                ,SUM(REC_RETENCION_3M_23) REC_RETENCION_3M_23
                ,SUM(REC_RETENCION_3M_24) REC_RETENCION_3M_24

                --6M
                ,SUM(REC_RETENCION_6M_13) REC_RETENCION_6M_13
                ,SUM(REC_RETENCION_6M_14) REC_RETENCION_6M_14
                ,SUM(REC_RETENCION_6M_15) REC_RETENCION_6M_15
                ,SUM(REC_RETENCION_6M_16) REC_RETENCION_6M_16
                ,SUM(REC_RETENCION_6M_17) REC_RETENCION_6M_17
                ,SUM(REC_RETENCION_6M_18) REC_RETENCION_6M_18
                ,SUM(REC_RETENCION_6M_19) REC_RETENCION_6M_19
                ,SUM(REC_RETENCION_6M_20) REC_RETENCION_6M_20
                ,SUM(REC_RETENCION_6M_21) REC_RETENCION_6M_21
                ,SUM(REC_RETENCION_6M_22) REC_RETENCION_6M_22
                ,SUM(REC_RETENCION_6M_23) REC_RETENCION_6M_23
                ,SUM(REC_RETENCION_6M_24) REC_RETENCION_6M_24

                --NUEVOS
                --1M
                ,SUM(NUEVOS_RETENCION_1M_13) NUEVOS_RETENCION_1M_13
                ,SUM(NUEVOS_RETENCION_1M_14) NUEVOS_RETENCION_1M_14
                ,SUM(NUEVOS_RETENCION_1M_15) NUEVOS_RETENCION_1M_15
                ,SUM(NUEVOS_RETENCION_1M_16) NUEVOS_RETENCION_1M_16
                ,SUM(NUEVOS_RETENCION_1M_17) NUEVOS_RETENCION_1M_17
                ,SUM(NUEVOS_RETENCION_1M_18) NUEVOS_RETENCION_1M_18
                ,SUM(NUEVOS_RETENCION_1M_19) NUEVOS_RETENCION_1M_19
                ,SUM(NUEVOS_RETENCION_1M_20) NUEVOS_RETENCION_1M_20
                ,SUM(NUEVOS_RETENCION_1M_21) NUEVOS_RETENCION_1M_21
                ,SUM(NUEVOS_RETENCION_1M_22) NUEVOS_RETENCION_1M_22
                ,SUM(NUEVOS_RETENCION_1M_23) NUEVOS_RETENCION_1M_23
                ,SUM(NUEVOS_RETENCION_1M_24) NUEVOS_RETENCION_1M_24

                --2M
                ,SUM(NUEVOS_RETENCION_2M_13) NUEVOS_RETENCION_2M_13
                ,SUM(NUEVOS_RETENCION_2M_14) NUEVOS_RETENCION_2M_14
                ,SUM(NUEVOS_RETENCION_2M_15) NUEVOS_RETENCION_2M_15
                ,SUM(NUEVOS_RETENCION_2M_16) NUEVOS_RETENCION_2M_16
                ,SUM(NUEVOS_RETENCION_2M_17) NUEVOS_RETENCION_2M_17
                ,SUM(NUEVOS_RETENCION_2M_18) NUEVOS_RETENCION_2M_18
                ,SUM(NUEVOS_RETENCION_2M_19) NUEVOS_RETENCION_2M_19
                ,SUM(NUEVOS_RETENCION_2M_20) NUEVOS_RETENCION_2M_20
                ,SUM(NUEVOS_RETENCION_2M_21) NUEVOS_RETENCION_2M_21
                ,SUM(NUEVOS_RETENCION_2M_22) NUEVOS_RETENCION_2M_22
                ,SUM(NUEVOS_RETENCION_2M_23) NUEVOS_RETENCION_2M_23
                ,SUM(NUEVOS_RETENCION_2M_24) NUEVOS_RETENCION_2M_24

                --3M
                ,SUM(NUEVOS_RETENCION_3M_13) NUEVOS_RETENCION_3M_13
                ,SUM(NUEVOS_RETENCION_3M_14) NUEVOS_RETENCION_3M_14
                ,SUM(NUEVOS_RETENCION_3M_15) NUEVOS_RETENCION_3M_15
                ,SUM(NUEVOS_RETENCION_3M_16) NUEVOS_RETENCION_3M_16
                ,SUM(NUEVOS_RETENCION_3M_17) NUEVOS_RETENCION_3M_17
                ,SUM(NUEVOS_RETENCION_3M_18) NUEVOS_RETENCION_3M_18
                ,SUM(NUEVOS_RETENCION_3M_19) NUEVOS_RETENCION_3M_19
                ,SUM(NUEVOS_RETENCION_3M_20) NUEVOS_RETENCION_3M_20
                ,SUM(NUEVOS_RETENCION_3M_21) NUEVOS_RETENCION_3M_21
                ,SUM(NUEVOS_RETENCION_3M_22) NUEVOS_RETENCION_3M_22
                ,SUM(NUEVOS_RETENCION_3M_23) NUEVOS_RETENCION_3M_23
                ,SUM(NUEVOS_RETENCION_3M_24) NUEVOS_RETENCION_3M_24

                --6M
                ,SUM(NUEVOS_RETENCION_6M_13) NUEVOS_RETENCION_6M_13
                ,SUM(NUEVOS_RETENCION_6M_14) NUEVOS_RETENCION_6M_14
                ,SUM(NUEVOS_RETENCION_6M_15) NUEVOS_RETENCION_6M_15
                ,SUM(NUEVOS_RETENCION_6M_16) NUEVOS_RETENCION_6M_16
                ,SUM(NUEVOS_RETENCION_6M_17) NUEVOS_RETENCION_6M_17
                ,SUM(NUEVOS_RETENCION_6M_18) NUEVOS_RETENCION_6M_18
                ,SUM(NUEVOS_RETENCION_6M_19) NUEVOS_RETENCION_6M_19
                ,SUM(NUEVOS_RETENCION_6M_20) NUEVOS_RETENCION_6M_20
                ,SUM(NUEVOS_RETENCION_6M_21) NUEVOS_RETENCION_6M_21
                ,SUM(NUEVOS_RETENCION_6M_22) NUEVOS_RETENCION_6M_22
                ,SUM(NUEVOS_RETENCION_6M_23) NUEVOS_RETENCION_6M_23
                ,SUM(NUEVOS_RETENCION_6M_24) NUEVOS_RETENCION_6M_24

                --REPETIDORES
                --1M
                ,SUM(REPETIDORES_RETENCION_1M_13) REPETIDORES_RETENCION_1M_13
                ,SUM(REPETIDORES_RETENCION_1M_14) REPETIDORES_RETENCION_1M_14
                ,SUM(REPETIDORES_RETENCION_1M_15) REPETIDORES_RETENCION_1M_15
                ,SUM(REPETIDORES_RETENCION_1M_16) REPETIDORES_RETENCION_1M_16
                ,SUM(REPETIDORES_RETENCION_1M_17) REPETIDORES_RETENCION_1M_17
                ,SUM(REPETIDORES_RETENCION_1M_18) REPETIDORES_RETENCION_1M_18
                ,SUM(REPETIDORES_RETENCION_1M_19) REPETIDORES_RETENCION_1M_19
                ,SUM(REPETIDORES_RETENCION_1M_20) REPETIDORES_RETENCION_1M_20
                ,SUM(REPETIDORES_RETENCION_1M_21) REPETIDORES_RETENCION_1M_21
                ,SUM(REPETIDORES_RETENCION_1M_22) REPETIDORES_RETENCION_1M_22
                ,SUM(REPETIDORES_RETENCION_1M_23) REPETIDORES_RETENCION_1M_23
                ,SUM(REPETIDORES_RETENCION_1M_24) REPETIDORES_RETENCION_1M_24

                --2M
                ,SUM(REPETIDORES_RETENCION_2M_13) REPETIDORES_RETENCION_2M_13
                ,SUM(REPETIDORES_RETENCION_2M_14) REPETIDORES_RETENCION_2M_14
                ,SUM(REPETIDORES_RETENCION_2M_15) REPETIDORES_RETENCION_2M_15
                ,SUM(REPETIDORES_RETENCION_2M_16) REPETIDORES_RETENCION_2M_16
                ,SUM(REPETIDORES_RETENCION_2M_17) REPETIDORES_RETENCION_2M_17
                ,SUM(REPETIDORES_RETENCION_2M_18) REPETIDORES_RETENCION_2M_18
                ,SUM(REPETIDORES_RETENCION_2M_19) REPETIDORES_RETENCION_2M_19
                ,SUM(REPETIDORES_RETENCION_2M_20) REPETIDORES_RETENCION_2M_20
                ,SUM(REPETIDORES_RETENCION_2M_21) REPETIDORES_RETENCION_2M_21
                ,SUM(REPETIDORES_RETENCION_2M_22) REPETIDORES_RETENCION_2M_22
                ,SUM(REPETIDORES_RETENCION_2M_23) REPETIDORES_RETENCION_2M_23
                ,SUM(REPETIDORES_RETENCION_2M_24) REPETIDORES_RETENCION_2M_24

                --3M
                ,SUM(REPETIDORES_RETENCION_3M_13) REPETIDORES_RETENCION_3M_13
                ,SUM(REPETIDORES_RETENCION_3M_14) REPETIDORES_RETENCION_3M_14
                ,SUM(REPETIDORES_RETENCION_3M_15) REPETIDORES_RETENCION_3M_15
                ,SUM(REPETIDORES_RETENCION_3M_16) REPETIDORES_RETENCION_3M_16
                ,SUM(REPETIDORES_RETENCION_3M_17) REPETIDORES_RETENCION_3M_17
                ,SUM(REPETIDORES_RETENCION_3M_18) REPETIDORES_RETENCION_3M_18
                ,SUM(REPETIDORES_RETENCION_3M_19) REPETIDORES_RETENCION_3M_19
                ,SUM(REPETIDORES_RETENCION_3M_20) REPETIDORES_RETENCION_3M_20
                ,SUM(REPETIDORES_RETENCION_3M_21) REPETIDORES_RETENCION_3M_21
                ,SUM(REPETIDORES_RETENCION_3M_22) REPETIDORES_RETENCION_3M_22
                ,SUM(REPETIDORES_RETENCION_3M_23) REPETIDORES_RETENCION_3M_23
                ,SUM(REPETIDORES_RETENCION_3M_24) REPETIDORES_RETENCION_3M_24

                --6M
                ,SUM(REPETIDORES_RETENCION_6M_13) REPETIDORES_RETENCION_6M_13
                ,SUM(REPETIDORES_RETENCION_6M_14) REPETIDORES_RETENCION_6M_14
                ,SUM(REPETIDORES_RETENCION_6M_15) REPETIDORES_RETENCION_6M_15
                ,SUM(REPETIDORES_RETENCION_6M_16) REPETIDORES_RETENCION_6M_16
                ,SUM(REPETIDORES_RETENCION_6M_17) REPETIDORES_RETENCION_6M_17
                ,SUM(REPETIDORES_RETENCION_6M_18) REPETIDORES_RETENCION_6M_18
                ,SUM(REPETIDORES_RETENCION_6M_19) REPETIDORES_RETENCION_6M_19
                ,SUM(REPETIDORES_RETENCION_6M_20) REPETIDORES_RETENCION_6M_20
                ,SUM(REPETIDORES_RETENCION_6M_21) REPETIDORES_RETENCION_6M_21
                ,SUM(REPETIDORES_RETENCION_6M_22) REPETIDORES_RETENCION_6M_22
                ,SUM(REPETIDORES_RETENCION_6M_23) REPETIDORES_RETENCION_6M_23
                ,SUM(REPETIDORES_RETENCION_6M_24) REPETIDORES_RETENCION_6M_24

                --LEALES
                --1M
                ,SUM(LEALES_RETENCION_1M_13) LEALES_RETENCION_1M_13
                ,SUM(LEALES_RETENCION_1M_14) LEALES_RETENCION_1M_14
                ,SUM(LEALES_RETENCION_1M_15) LEALES_RETENCION_1M_15
                ,SUM(LEALES_RETENCION_1M_16) LEALES_RETENCION_1M_16
                ,SUM(LEALES_RETENCION_1M_17) LEALES_RETENCION_1M_17
                ,SUM(LEALES_RETENCION_1M_18) LEALES_RETENCION_1M_18
                ,SUM(LEALES_RETENCION_1M_19) LEALES_RETENCION_1M_19
                ,SUM(LEALES_RETENCION_1M_20) LEALES_RETENCION_1M_20
                ,SUM(LEALES_RETENCION_1M_21) LEALES_RETENCION_1M_21
                ,SUM(LEALES_RETENCION_1M_22) LEALES_RETENCION_1M_22
                ,SUM(LEALES_RETENCION_1M_23) LEALES_RETENCION_1M_23
                ,SUM(LEALES_RETENCION_1M_24) LEALES_RETENCION_1M_24

                --2M
                ,SUM(LEALES_RETENCION_2M_13) LEALES_RETENCION_2M_13
                ,SUM(LEALES_RETENCION_2M_14) LEALES_RETENCION_2M_14
                ,SUM(LEALES_RETENCION_2M_15) LEALES_RETENCION_2M_15
                ,SUM(LEALES_RETENCION_2M_16) LEALES_RETENCION_2M_16
                ,SUM(LEALES_RETENCION_2M_17) LEALES_RETENCION_2M_17
                ,SUM(LEALES_RETENCION_2M_18) LEALES_RETENCION_2M_18
                ,SUM(LEALES_RETENCION_2M_19) LEALES_RETENCION_2M_19
                ,SUM(LEALES_RETENCION_2M_20) LEALES_RETENCION_2M_20
                ,SUM(LEALES_RETENCION_2M_21) LEALES_RETENCION_2M_21
                ,SUM(LEALES_RETENCION_2M_22) LEALES_RETENCION_2M_22
                ,SUM(LEALES_RETENCION_2M_23) LEALES_RETENCION_2M_23
                ,SUM(LEALES_RETENCION_2M_24) LEALES_RETENCION_2M_24

                --3M
                ,SUM(LEALES_RETENCION_3M_13) LEALES_RETENCION_3M_13
                ,SUM(LEALES_RETENCION_3M_14) LEALES_RETENCION_3M_14
                ,SUM(LEALES_RETENCION_3M_15) LEALES_RETENCION_3M_15
                ,SUM(LEALES_RETENCION_3M_16) LEALES_RETENCION_3M_16
                ,SUM(LEALES_RETENCION_3M_17) LEALES_RETENCION_3M_17
                ,SUM(LEALES_RETENCION_3M_18) LEALES_RETENCION_3M_18
                ,SUM(LEALES_RETENCION_3M_19) LEALES_RETENCION_3M_19
                ,SUM(LEALES_RETENCION_3M_20) LEALES_RETENCION_3M_20
                ,SUM(LEALES_RETENCION_3M_21) LEALES_RETENCION_3M_21
                ,SUM(LEALES_RETENCION_3M_22) LEALES_RETENCION_3M_22
                ,SUM(LEALES_RETENCION_3M_23) LEALES_RETENCION_3M_23
                ,SUM(LEALES_RETENCION_3M_24) LEALES_RETENCION_3M_24

                --6M
                ,SUM(LEALES_RETENCION_6M_13) LEALES_RETENCION_6M_13
                ,SUM(LEALES_RETENCION_6M_14) LEALES_RETENCION_6M_14
                ,SUM(LEALES_RETENCION_6M_15) LEALES_RETENCION_6M_15
                ,SUM(LEALES_RETENCION_6M_16) LEALES_RETENCION_6M_16
                ,SUM(LEALES_RETENCION_6M_17) LEALES_RETENCION_6M_17
                ,SUM(LEALES_RETENCION_6M_18) LEALES_RETENCION_6M_18
                ,SUM(LEALES_RETENCION_6M_19) LEALES_RETENCION_6M_19
                ,SUM(LEALES_RETENCION_6M_20) LEALES_RETENCION_6M_20
                ,SUM(LEALES_RETENCION_6M_21) LEALES_RETENCION_6M_21
                ,SUM(LEALES_RETENCION_6M_22) LEALES_RETENCION_6M_22
                ,SUM(LEALES_RETENCION_6M_23) LEALES_RETENCION_6M_23
                ,SUM(LEALES_RETENCION_6M_24) LEALES_RETENCION_6M_24
                
                --VENTA_RETENCION
                --RECOMPRA
                ,SUM(VENTA_RECOMPRA_13) VENTA_RECOMPRA_13
                ,SUM(VENTA_RECOMPRA_14) VENTA_RECOMPRA_14
                ,SUM(VENTA_RECOMPRA_15) VENTA_RECOMPRA_15
                ,SUM(VENTA_RECOMPRA_16) VENTA_RECOMPRA_16
                ,SUM(VENTA_RECOMPRA_17) VENTA_RECOMPRA_17
                ,SUM(VENTA_RECOMPRA_18) VENTA_RECOMPRA_18
                ,SUM(VENTA_RECOMPRA_19) VENTA_RECOMPRA_19
                ,SUM(VENTA_RECOMPRA_20) VENTA_RECOMPRA_20
                ,SUM(VENTA_RECOMPRA_21) VENTA_RECOMPRA_21
                ,SUM(VENTA_RECOMPRA_22) VENTA_RECOMPRA_22
                ,SUM(VENTA_RECOMPRA_23) VENTA_RECOMPRA_23
                ,SUM(VENTA_RECOMPRA_24) VENTA_RECOMPRA_24
                
                --1M
                ,SUM(VENTA_RECOMPRA_RETENCION_1M_13) VENTA_RECOMPRA_RETENCION_1M_13
                ,SUM(VENTA_RECOMPRA_RETENCION_1M_14) VENTA_RECOMPRA_RETENCION_1M_14
                ,SUM(VENTA_RECOMPRA_RETENCION_1M_15) VENTA_RECOMPRA_RETENCION_1M_15
                ,SUM(VENTA_RECOMPRA_RETENCION_1M_16) VENTA_RECOMPRA_RETENCION_1M_16
                ,SUM(VENTA_RECOMPRA_RETENCION_1M_17) VENTA_RECOMPRA_RETENCION_1M_17
                ,SUM(VENTA_RECOMPRA_RETENCION_1M_18) VENTA_RECOMPRA_RETENCION_1M_18
                ,SUM(VENTA_RECOMPRA_RETENCION_1M_19) VENTA_RECOMPRA_RETENCION_1M_19
                ,SUM(VENTA_RECOMPRA_RETENCION_1M_20) VENTA_RECOMPRA_RETENCION_1M_20
                ,SUM(VENTA_RECOMPRA_RETENCION_1M_21) VENTA_RECOMPRA_RETENCION_1M_21
                ,SUM(VENTA_RECOMPRA_RETENCION_1M_22) VENTA_RECOMPRA_RETENCION_1M_22
                ,SUM(VENTA_RECOMPRA_RETENCION_1M_23) VENTA_RECOMPRA_RETENCION_1M_23
                ,SUM(VENTA_RECOMPRA_RETENCION_1M_24) VENTA_RECOMPRA_RETENCION_1M_24

                --2M
                ,SUM(VENTA_RECOMPRA_RETENCION_2M_13) VENTA_RECOMPRA_RETENCION_2M_13
                ,SUM(VENTA_RECOMPRA_RETENCION_2M_14) VENTA_RECOMPRA_RETENCION_2M_14
                ,SUM(VENTA_RECOMPRA_RETENCION_2M_15) VENTA_RECOMPRA_RETENCION_2M_15
                ,SUM(VENTA_RECOMPRA_RETENCION_2M_16) VENTA_RECOMPRA_RETENCION_2M_16
                ,SUM(VENTA_RECOMPRA_RETENCION_2M_17) VENTA_RECOMPRA_RETENCION_2M_17
                ,SUM(VENTA_RECOMPRA_RETENCION_2M_18) VENTA_RECOMPRA_RETENCION_2M_18
                ,SUM(VENTA_RECOMPRA_RETENCION_2M_19) VENTA_RECOMPRA_RETENCION_2M_19
                ,SUM(VENTA_RECOMPRA_RETENCION_2M_20) VENTA_RECOMPRA_RETENCION_2M_20
                ,SUM(VENTA_RECOMPRA_RETENCION_2M_21) VENTA_RECOMPRA_RETENCION_2M_21
                ,SUM(VENTA_RECOMPRA_RETENCION_2M_22) VENTA_RECOMPRA_RETENCION_2M_22
                ,SUM(VENTA_RECOMPRA_RETENCION_2M_23) VENTA_RECOMPRA_RETENCION_2M_23
                ,SUM(VENTA_RECOMPRA_RETENCION_2M_24) VENTA_RECOMPRA_RETENCION_2M_24

                --3M
                ,SUM(VENTA_RECOMPRA_RETENCION_3M_13) VENTA_RECOMPRA_RETENCION_3M_13
                ,SUM(VENTA_RECOMPRA_RETENCION_3M_14) VENTA_RECOMPRA_RETENCION_3M_14
                ,SUM(VENTA_RECOMPRA_RETENCION_3M_15) VENTA_RECOMPRA_RETENCION_3M_15
                ,SUM(VENTA_RECOMPRA_RETENCION_3M_16) VENTA_RECOMPRA_RETENCION_3M_16
                ,SUM(VENTA_RECOMPRA_RETENCION_3M_17) VENTA_RECOMPRA_RETENCION_3M_17
                ,SUM(VENTA_RECOMPRA_RETENCION_3M_18) VENTA_RECOMPRA_RETENCION_3M_18
                ,SUM(VENTA_RECOMPRA_RETENCION_3M_19) VENTA_RECOMPRA_RETENCION_3M_19
                ,SUM(VENTA_RECOMPRA_RETENCION_3M_20) VENTA_RECOMPRA_RETENCION_3M_20
                ,SUM(VENTA_RECOMPRA_RETENCION_3M_21) VENTA_RECOMPRA_RETENCION_3M_21
                ,SUM(VENTA_RECOMPRA_RETENCION_3M_22) VENTA_RECOMPRA_RETENCION_3M_22
                ,SUM(VENTA_RECOMPRA_RETENCION_3M_23) VENTA_RECOMPRA_RETENCION_3M_23
                ,SUM(VENTA_RECOMPRA_RETENCION_3M_24) VENTA_RECOMPRA_RETENCION_3M_24

                --6M
                ,SUM(VENTA_RECOMPRA_RETENCION_6M_13) VENTA_RECOMPRA_RETENCION_6M_13
                ,SUM(VENTA_RECOMPRA_RETENCION_6M_14) VENTA_RECOMPRA_RETENCION_6M_14
                ,SUM(VENTA_RECOMPRA_RETENCION_6M_15) VENTA_RECOMPRA_RETENCION_6M_15
                ,SUM(VENTA_RECOMPRA_RETENCION_6M_16) VENTA_RECOMPRA_RETENCION_6M_16
                ,SUM(VENTA_RECOMPRA_RETENCION_6M_17) VENTA_RECOMPRA_RETENCION_6M_17
                ,SUM(VENTA_RECOMPRA_RETENCION_6M_18) VENTA_RECOMPRA_RETENCION_6M_18
                ,SUM(VENTA_RECOMPRA_RETENCION_6M_19) VENTA_RECOMPRA_RETENCION_6M_19
                ,SUM(VENTA_RECOMPRA_RETENCION_6M_20) VENTA_RECOMPRA_RETENCION_6M_20
                ,SUM(VENTA_RECOMPRA_RETENCION_6M_21) VENTA_RECOMPRA_RETENCION_6M_21
                ,SUM(VENTA_RECOMPRA_RETENCION_6M_22) VENTA_RECOMPRA_RETENCION_6M_22
                ,SUM(VENTA_RECOMPRA_RETENCION_6M_23) VENTA_RECOMPRA_RETENCION_6M_23
                ,SUM(VENTA_RECOMPRA_RETENCION_6M_24) VENTA_RECOMPRA_RETENCION_6M_24

                --FID
                ,SUM(VENTA_FID_13) VENTA_FID_13
                ,SUM(VENTA_FID_14) VENTA_FID_14
                ,SUM(VENTA_FID_15) VENTA_FID_15
                ,SUM(VENTA_FID_16) VENTA_FID_16
                ,SUM(VENTA_FID_17) VENTA_FID_17
                ,SUM(VENTA_FID_18) VENTA_FID_18
                ,SUM(VENTA_FID_19) VENTA_FID_19
                ,SUM(VENTA_FID_20) VENTA_FID_20
                ,SUM(VENTA_FID_21) VENTA_FID_21
                ,SUM(VENTA_FID_22) VENTA_FID_22
                ,SUM(VENTA_FID_23) VENTA_FID_23
                ,SUM(VENTA_FID_24) VENTA_FID_24

                --1M
                ,SUM(VENTA_FID_RETENCION_1M_13) VENTA_FID_RETENCION_1M_13
                ,SUM(VENTA_FID_RETENCION_1M_14) VENTA_FID_RETENCION_1M_14
                ,SUM(VENTA_FID_RETENCION_1M_15) VENTA_FID_RETENCION_1M_15
                ,SUM(VENTA_FID_RETENCION_1M_16) VENTA_FID_RETENCION_1M_16
                ,SUM(VENTA_FID_RETENCION_1M_17) VENTA_FID_RETENCION_1M_17
                ,SUM(VENTA_FID_RETENCION_1M_18) VENTA_FID_RETENCION_1M_18
                ,SUM(VENTA_FID_RETENCION_1M_19) VENTA_FID_RETENCION_1M_19
                ,SUM(VENTA_FID_RETENCION_1M_20) VENTA_FID_RETENCION_1M_20
                ,SUM(VENTA_FID_RETENCION_1M_21) VENTA_FID_RETENCION_1M_21
                ,SUM(VENTA_FID_RETENCION_1M_22) VENTA_FID_RETENCION_1M_22
                ,SUM(VENTA_FID_RETENCION_1M_23) VENTA_FID_RETENCION_1M_23
                ,SUM(VENTA_FID_RETENCION_1M_24) VENTA_FID_RETENCION_1M_24

                --2M
                ,SUM(VENTA_FID_RETENCION_2M_13) VENTA_FID_RETENCION_2M_13
                ,SUM(VENTA_FID_RETENCION_2M_14) VENTA_FID_RETENCION_2M_14
                ,SUM(VENTA_FID_RETENCION_2M_15) VENTA_FID_RETENCION_2M_15
                ,SUM(VENTA_FID_RETENCION_2M_16) VENTA_FID_RETENCION_2M_16
                ,SUM(VENTA_FID_RETENCION_2M_17) VENTA_FID_RETENCION_2M_17
                ,SUM(VENTA_FID_RETENCION_2M_18) VENTA_FID_RETENCION_2M_18
                ,SUM(VENTA_FID_RETENCION_2M_19) VENTA_FID_RETENCION_2M_19
                ,SUM(VENTA_FID_RETENCION_2M_20) VENTA_FID_RETENCION_2M_20
                ,SUM(VENTA_FID_RETENCION_2M_21) VENTA_FID_RETENCION_2M_21
                ,SUM(VENTA_FID_RETENCION_2M_22) VENTA_FID_RETENCION_2M_22
                ,SUM(VENTA_FID_RETENCION_2M_23) VENTA_FID_RETENCION_2M_23
                ,SUM(VENTA_FID_RETENCION_2M_24) VENTA_FID_RETENCION_2M_24

                --3M
                ,SUM(VENTA_FID_RETENCION_3M_13) VENTA_FID_RETENCION_3M_13
                ,SUM(VENTA_FID_RETENCION_3M_14) VENTA_FID_RETENCION_3M_14
                ,SUM(VENTA_FID_RETENCION_3M_15) VENTA_FID_RETENCION_3M_15
                ,SUM(VENTA_FID_RETENCION_3M_16) VENTA_FID_RETENCION_3M_16
                ,SUM(VENTA_FID_RETENCION_3M_17) VENTA_FID_RETENCION_3M_17
                ,SUM(VENTA_FID_RETENCION_3M_18) VENTA_FID_RETENCION_3M_18
                ,SUM(VENTA_FID_RETENCION_3M_19) VENTA_FID_RETENCION_3M_19
                ,SUM(VENTA_FID_RETENCION_3M_20) VENTA_FID_RETENCION_3M_20
                ,SUM(VENTA_FID_RETENCION_3M_21) VENTA_FID_RETENCION_3M_21
                ,SUM(VENTA_FID_RETENCION_3M_22) VENTA_FID_RETENCION_3M_22
                ,SUM(VENTA_FID_RETENCION_3M_23) VENTA_FID_RETENCION_3M_23
                ,SUM(VENTA_FID_RETENCION_3M_24) VENTA_FID_RETENCION_3M_24

                --6M
                ,SUM(VENTA_FID_RETENCION_6M_13) VENTA_FID_RETENCION_6M_13
                ,SUM(VENTA_FID_RETENCION_6M_14) VENTA_FID_RETENCION_6M_14
                ,SUM(VENTA_FID_RETENCION_6M_15) VENTA_FID_RETENCION_6M_15
                ,SUM(VENTA_FID_RETENCION_6M_16) VENTA_FID_RETENCION_6M_16
                ,SUM(VENTA_FID_RETENCION_6M_17) VENTA_FID_RETENCION_6M_17
                ,SUM(VENTA_FID_RETENCION_6M_18) VENTA_FID_RETENCION_6M_18
                ,SUM(VENTA_FID_RETENCION_6M_19) VENTA_FID_RETENCION_6M_19
                ,SUM(VENTA_FID_RETENCION_6M_20) VENTA_FID_RETENCION_6M_20
                ,SUM(VENTA_FID_RETENCION_6M_21) VENTA_FID_RETENCION_6M_21
                ,SUM(VENTA_FID_RETENCION_6M_22) VENTA_FID_RETENCION_6M_22
                ,SUM(VENTA_FID_RETENCION_6M_23) VENTA_FID_RETENCION_6M_23
                ,SUM(VENTA_FID_RETENCION_6M_24) VENTA_FID_RETENCION_6M_24

                --REC
                ,SUM(VENTA_REC_13) VENTA_REC_13
                ,SUM(VENTA_REC_14) VENTA_REC_14
                ,SUM(VENTA_REC_15) VENTA_REC_15
                ,SUM(VENTA_REC_16) VENTA_REC_16
                ,SUM(VENTA_REC_17) VENTA_REC_17
                ,SUM(VENTA_REC_18) VENTA_REC_18
                ,SUM(VENTA_REC_19) VENTA_REC_19
                ,SUM(VENTA_REC_20) VENTA_REC_20
                ,SUM(VENTA_REC_21) VENTA_REC_21
                ,SUM(VENTA_REC_22) VENTA_REC_22
                ,SUM(VENTA_REC_23) VENTA_REC_23
                ,SUM(VENTA_REC_24) VENTA_REC_24      

                --1M
                ,SUM(VENTA_REC_RETENCION_1M_13) VENTA_REC_RETENCION_1M_13
                ,SUM(VENTA_REC_RETENCION_1M_14) VENTA_REC_RETENCION_1M_14
                ,SUM(VENTA_REC_RETENCION_1M_15) VENTA_REC_RETENCION_1M_15
                ,SUM(VENTA_REC_RETENCION_1M_16) VENTA_REC_RETENCION_1M_16
                ,SUM(VENTA_REC_RETENCION_1M_17) VENTA_REC_RETENCION_1M_17
                ,SUM(VENTA_REC_RETENCION_1M_18) VENTA_REC_RETENCION_1M_18
                ,SUM(VENTA_REC_RETENCION_1M_19) VENTA_REC_RETENCION_1M_19
                ,SUM(VENTA_REC_RETENCION_1M_20) VENTA_REC_RETENCION_1M_20
                ,SUM(VENTA_REC_RETENCION_1M_21) VENTA_REC_RETENCION_1M_21
                ,SUM(VENTA_REC_RETENCION_1M_22) VENTA_REC_RETENCION_1M_22
                ,SUM(VENTA_REC_RETENCION_1M_23) VENTA_REC_RETENCION_1M_23
                ,SUM(VENTA_REC_RETENCION_1M_24) VENTA_REC_RETENCION_1M_24

                --2M
                ,SUM(VENTA_REC_RETENCION_2M_13) VENTA_REC_RETENCION_2M_13
                ,SUM(VENTA_REC_RETENCION_2M_14) VENTA_REC_RETENCION_2M_14
                ,SUM(VENTA_REC_RETENCION_2M_15) VENTA_REC_RETENCION_2M_15
                ,SUM(VENTA_REC_RETENCION_2M_16) VENTA_REC_RETENCION_2M_16
                ,SUM(VENTA_REC_RETENCION_2M_17) VENTA_REC_RETENCION_2M_17
                ,SUM(VENTA_REC_RETENCION_2M_18) VENTA_REC_RETENCION_2M_18
                ,SUM(VENTA_REC_RETENCION_2M_19) VENTA_REC_RETENCION_2M_19
                ,SUM(VENTA_REC_RETENCION_2M_20) VENTA_REC_RETENCION_2M_20
                ,SUM(VENTA_REC_RETENCION_2M_21) VENTA_REC_RETENCION_2M_21
                ,SUM(VENTA_REC_RETENCION_2M_22) VENTA_REC_RETENCION_2M_22
                ,SUM(VENTA_REC_RETENCION_2M_23) VENTA_REC_RETENCION_2M_23
                ,SUM(VENTA_REC_RETENCION_2M_24) VENTA_REC_RETENCION_2M_24

                --3M
                ,SUM(VENTA_REC_RETENCION_3M_13) VENTA_REC_RETENCION_3M_13
                ,SUM(VENTA_REC_RETENCION_3M_14) VENTA_REC_RETENCION_3M_14
                ,SUM(VENTA_REC_RETENCION_3M_15) VENTA_REC_RETENCION_3M_15
                ,SUM(VENTA_REC_RETENCION_3M_16) VENTA_REC_RETENCION_3M_16
                ,SUM(VENTA_REC_RETENCION_3M_17) VENTA_REC_RETENCION_3M_17
                ,SUM(VENTA_REC_RETENCION_3M_18) VENTA_REC_RETENCION_3M_18
                ,SUM(VENTA_REC_RETENCION_3M_19) VENTA_REC_RETENCION_3M_19
                ,SUM(VENTA_REC_RETENCION_3M_20) VENTA_REC_RETENCION_3M_20
                ,SUM(VENTA_REC_RETENCION_3M_21) VENTA_REC_RETENCION_3M_21
                ,SUM(VENTA_REC_RETENCION_3M_22) VENTA_REC_RETENCION_3M_22
                ,SUM(VENTA_REC_RETENCION_3M_23) VENTA_REC_RETENCION_3M_23
                ,SUM(VENTA_REC_RETENCION_3M_24) VENTA_REC_RETENCION_3M_24

                --6M
                ,SUM(VENTA_REC_RETENCION_6M_13) VENTA_REC_RETENCION_6M_13
                ,SUM(VENTA_REC_RETENCION_6M_14) VENTA_REC_RETENCION_6M_14
                ,SUM(VENTA_REC_RETENCION_6M_15) VENTA_REC_RETENCION_6M_15
                ,SUM(VENTA_REC_RETENCION_6M_16) VENTA_REC_RETENCION_6M_16
                ,SUM(VENTA_REC_RETENCION_6M_17) VENTA_REC_RETENCION_6M_17
                ,SUM(VENTA_REC_RETENCION_6M_18) VENTA_REC_RETENCION_6M_18
                ,SUM(VENTA_REC_RETENCION_6M_19) VENTA_REC_RETENCION_6M_19
                ,SUM(VENTA_REC_RETENCION_6M_20) VENTA_REC_RETENCION_6M_20
                ,SUM(VENTA_REC_RETENCION_6M_21) VENTA_REC_RETENCION_6M_21
                ,SUM(VENTA_REC_RETENCION_6M_22) VENTA_REC_RETENCION_6M_22
                ,SUM(VENTA_REC_RETENCION_6M_23) VENTA_REC_RETENCION_6M_23
                ,SUM(VENTA_REC_RETENCION_6M_24) VENTA_REC_RETENCION_6M_24

                --NUEVOS
                ,SUM(VENTA_NUEVOS_13) VENTA_NUEVOS_13
                ,SUM(VENTA_NUEVOS_14) VENTA_NUEVOS_14
                ,SUM(VENTA_NUEVOS_15) VENTA_NUEVOS_15
                ,SUM(VENTA_NUEVOS_16) VENTA_NUEVOS_16
                ,SUM(VENTA_NUEVOS_17) VENTA_NUEVOS_17
                ,SUM(VENTA_NUEVOS_18) VENTA_NUEVOS_18
                ,SUM(VENTA_NUEVOS_19) VENTA_NUEVOS_19
                ,SUM(VENTA_NUEVOS_20) VENTA_NUEVOS_20
                ,SUM(VENTA_NUEVOS_21) VENTA_NUEVOS_21
                ,SUM(VENTA_NUEVOS_22) VENTA_NUEVOS_22
                ,SUM(VENTA_NUEVOS_23) VENTA_NUEVOS_23
                ,SUM(VENTA_NUEVOS_24) VENTA_NUEVOS_24

                --1M
                ,SUM(VENTA_NUEVOS_RETENCION_1M_13) VENTA_NUEVOS_RETENCION_1M_13
                ,SUM(VENTA_NUEVOS_RETENCION_1M_14) VENTA_NUEVOS_RETENCION_1M_14
                ,SUM(VENTA_NUEVOS_RETENCION_1M_15) VENTA_NUEVOS_RETENCION_1M_15
                ,SUM(VENTA_NUEVOS_RETENCION_1M_16) VENTA_NUEVOS_RETENCION_1M_16
                ,SUM(VENTA_NUEVOS_RETENCION_1M_17) VENTA_NUEVOS_RETENCION_1M_17
                ,SUM(VENTA_NUEVOS_RETENCION_1M_18) VENTA_NUEVOS_RETENCION_1M_18
                ,SUM(VENTA_NUEVOS_RETENCION_1M_19) VENTA_NUEVOS_RETENCION_1M_19
                ,SUM(VENTA_NUEVOS_RETENCION_1M_20) VENTA_NUEVOS_RETENCION_1M_20
                ,SUM(VENTA_NUEVOS_RETENCION_1M_21) VENTA_NUEVOS_RETENCION_1M_21
                ,SUM(VENTA_NUEVOS_RETENCION_1M_22) VENTA_NUEVOS_RETENCION_1M_22
                ,SUM(VENTA_NUEVOS_RETENCION_1M_23) VENTA_NUEVOS_RETENCION_1M_23
                ,SUM(VENTA_NUEVOS_RETENCION_1M_24) VENTA_NUEVOS_RETENCION_1M_24

                --2M
                ,SUM(VENTA_NUEVOS_RETENCION_2M_13) VENTA_NUEVOS_RETENCION_2M_13
                ,SUM(VENTA_NUEVOS_RETENCION_2M_14) VENTA_NUEVOS_RETENCION_2M_14
                ,SUM(VENTA_NUEVOS_RETENCION_2M_15) VENTA_NUEVOS_RETENCION_2M_15
                ,SUM(VENTA_NUEVOS_RETENCION_2M_16) VENTA_NUEVOS_RETENCION_2M_16
                ,SUM(VENTA_NUEVOS_RETENCION_2M_17) VENTA_NUEVOS_RETENCION_2M_17
                ,SUM(VENTA_NUEVOS_RETENCION_2M_18) VENTA_NUEVOS_RETENCION_2M_18
                ,SUM(VENTA_NUEVOS_RETENCION_2M_19) VENTA_NUEVOS_RETENCION_2M_19
                ,SUM(VENTA_NUEVOS_RETENCION_2M_20) VENTA_NUEVOS_RETENCION_2M_20
                ,SUM(VENTA_NUEVOS_RETENCION_2M_21) VENTA_NUEVOS_RETENCION_2M_21
                ,SUM(VENTA_NUEVOS_RETENCION_2M_22) VENTA_NUEVOS_RETENCION_2M_22
                ,SUM(VENTA_NUEVOS_RETENCION_2M_23) VENTA_NUEVOS_RETENCION_2M_23
                ,SUM(VENTA_NUEVOS_RETENCION_2M_24) VENTA_NUEVOS_RETENCION_2M_24

                --3M
                ,SUM(VENTA_NUEVOS_RETENCION_3M_13) VENTA_NUEVOS_RETENCION_3M_13
                ,SUM(VENTA_NUEVOS_RETENCION_3M_14) VENTA_NUEVOS_RETENCION_3M_14
                ,SUM(VENTA_NUEVOS_RETENCION_3M_15) VENTA_NUEVOS_RETENCION_3M_15
                ,SUM(VENTA_NUEVOS_RETENCION_3M_16) VENTA_NUEVOS_RETENCION_3M_16
                ,SUM(VENTA_NUEVOS_RETENCION_3M_17) VENTA_NUEVOS_RETENCION_3M_17
                ,SUM(VENTA_NUEVOS_RETENCION_3M_18) VENTA_NUEVOS_RETENCION_3M_18
                ,SUM(VENTA_NUEVOS_RETENCION_3M_19) VENTA_NUEVOS_RETENCION_3M_19
                ,SUM(VENTA_NUEVOS_RETENCION_3M_20) VENTA_NUEVOS_RETENCION_3M_20
                ,SUM(VENTA_NUEVOS_RETENCION_3M_21) VENTA_NUEVOS_RETENCION_3M_21
                ,SUM(VENTA_NUEVOS_RETENCION_3M_22) VENTA_NUEVOS_RETENCION_3M_22
                ,SUM(VENTA_NUEVOS_RETENCION_3M_23) VENTA_NUEVOS_RETENCION_3M_23
                ,SUM(VENTA_NUEVOS_RETENCION_3M_24) VENTA_NUEVOS_RETENCION_3M_24

                --6M
                ,SUM(VENTA_NUEVOS_RETENCION_6M_13) VENTA_NUEVOS_RETENCION_6M_13
                ,SUM(VENTA_NUEVOS_RETENCION_6M_14) VENTA_NUEVOS_RETENCION_6M_14
                ,SUM(VENTA_NUEVOS_RETENCION_6M_15) VENTA_NUEVOS_RETENCION_6M_15
                ,SUM(VENTA_NUEVOS_RETENCION_6M_16) VENTA_NUEVOS_RETENCION_6M_16
                ,SUM(VENTA_NUEVOS_RETENCION_6M_17) VENTA_NUEVOS_RETENCION_6M_17
                ,SUM(VENTA_NUEVOS_RETENCION_6M_18) VENTA_NUEVOS_RETENCION_6M_18
                ,SUM(VENTA_NUEVOS_RETENCION_6M_19) VENTA_NUEVOS_RETENCION_6M_19
                ,SUM(VENTA_NUEVOS_RETENCION_6M_20) VENTA_NUEVOS_RETENCION_6M_20
                ,SUM(VENTA_NUEVOS_RETENCION_6M_21) VENTA_NUEVOS_RETENCION_6M_21
                ,SUM(VENTA_NUEVOS_RETENCION_6M_22) VENTA_NUEVOS_RETENCION_6M_22
                ,SUM(VENTA_NUEVOS_RETENCION_6M_23) VENTA_NUEVOS_RETENCION_6M_23
                ,SUM(VENTA_NUEVOS_RETENCION_6M_24) VENTA_NUEVOS_RETENCION_6M_24

                --REPETIDORES
                ,SUM(VENTA_REPETIDORES_13) VENTA_REPETIDORES_13
                ,SUM(VENTA_REPETIDORES_14) VENTA_REPETIDORES_14
                ,SUM(VENTA_REPETIDORES_15) VENTA_REPETIDORES_15
                ,SUM(VENTA_REPETIDORES_16) VENTA_REPETIDORES_16
                ,SUM(VENTA_REPETIDORES_17) VENTA_REPETIDORES_17
                ,SUM(VENTA_REPETIDORES_18) VENTA_REPETIDORES_18
                ,SUM(VENTA_REPETIDORES_19) VENTA_REPETIDORES_19
                ,SUM(VENTA_REPETIDORES_20) VENTA_REPETIDORES_20
                ,SUM(VENTA_REPETIDORES_21) VENTA_REPETIDORES_21
                ,SUM(VENTA_REPETIDORES_22) VENTA_REPETIDORES_22
                ,SUM(VENTA_REPETIDORES_23) VENTA_REPETIDORES_23
                ,SUM(VENTA_REPETIDORES_24) VENTA_REPETIDORES_24

                --1M
                ,SUM(VENTA_REPETIDORES_RETENCION_1M_13) VENTA_REPETIDORES_RETENCION_1M_13
                ,SUM(VENTA_REPETIDORES_RETENCION_1M_14) VENTA_REPETIDORES_RETENCION_1M_14
                ,SUM(VENTA_REPETIDORES_RETENCION_1M_15) VENTA_REPETIDORES_RETENCION_1M_15
                ,SUM(VENTA_REPETIDORES_RETENCION_1M_16) VENTA_REPETIDORES_RETENCION_1M_16
                ,SUM(VENTA_REPETIDORES_RETENCION_1M_17) VENTA_REPETIDORES_RETENCION_1M_17
                ,SUM(VENTA_REPETIDORES_RETENCION_1M_18) VENTA_REPETIDORES_RETENCION_1M_18
                ,SUM(VENTA_REPETIDORES_RETENCION_1M_19) VENTA_REPETIDORES_RETENCION_1M_19
                ,SUM(VENTA_REPETIDORES_RETENCION_1M_20) VENTA_REPETIDORES_RETENCION_1M_20
                ,SUM(VENTA_REPETIDORES_RETENCION_1M_21) VENTA_REPETIDORES_RETENCION_1M_21
                ,SUM(VENTA_REPETIDORES_RETENCION_1M_22) VENTA_REPETIDORES_RETENCION_1M_22
                ,SUM(VENTA_REPETIDORES_RETENCION_1M_23) VENTA_REPETIDORES_RETENCION_1M_23
                ,SUM(VENTA_REPETIDORES_RETENCION_1M_24) VENTA_REPETIDORES_RETENCION_1M_24

                --2M
                ,SUM(VENTA_REPETIDORES_RETENCION_2M_13) VENTA_REPETIDORES_RETENCION_2M_13
                ,SUM(VENTA_REPETIDORES_RETENCION_2M_14) VENTA_REPETIDORES_RETENCION_2M_14
                ,SUM(VENTA_REPETIDORES_RETENCION_2M_15) VENTA_REPETIDORES_RETENCION_2M_15
                ,SUM(VENTA_REPETIDORES_RETENCION_2M_16) VENTA_REPETIDORES_RETENCION_2M_16
                ,SUM(VENTA_REPETIDORES_RETENCION_2M_17) VENTA_REPETIDORES_RETENCION_2M_17
                ,SUM(VENTA_REPETIDORES_RETENCION_2M_18) VENTA_REPETIDORES_RETENCION_2M_18
                ,SUM(VENTA_REPETIDORES_RETENCION_2M_19) VENTA_REPETIDORES_RETENCION_2M_19
                ,SUM(VENTA_REPETIDORES_RETENCION_2M_20) VENTA_REPETIDORES_RETENCION_2M_20
                ,SUM(VENTA_REPETIDORES_RETENCION_2M_21) VENTA_REPETIDORES_RETENCION_2M_21
                ,SUM(VENTA_REPETIDORES_RETENCION_2M_22) VENTA_REPETIDORES_RETENCION_2M_22
                ,SUM(VENTA_REPETIDORES_RETENCION_2M_23) VENTA_REPETIDORES_RETENCION_2M_23
                ,SUM(VENTA_REPETIDORES_RETENCION_2M_24) VENTA_REPETIDORES_RETENCION_2M_24

                --3M
                ,SUM(VENTA_REPETIDORES_RETENCION_3M_13) VENTA_REPETIDORES_RETENCION_3M_13
                ,SUM(VENTA_REPETIDORES_RETENCION_3M_14) VENTA_REPETIDORES_RETENCION_3M_14
                ,SUM(VENTA_REPETIDORES_RETENCION_3M_15) VENTA_REPETIDORES_RETENCION_3M_15
                ,SUM(VENTA_REPETIDORES_RETENCION_3M_16) VENTA_REPETIDORES_RETENCION_3M_16
                ,SUM(VENTA_REPETIDORES_RETENCION_3M_17) VENTA_REPETIDORES_RETENCION_3M_17
                ,SUM(VENTA_REPETIDORES_RETENCION_3M_18) VENTA_REPETIDORES_RETENCION_3M_18
                ,SUM(VENTA_REPETIDORES_RETENCION_3M_19) VENTA_REPETIDORES_RETENCION_3M_19
                ,SUM(VENTA_REPETIDORES_RETENCION_3M_20) VENTA_REPETIDORES_RETENCION_3M_20
                ,SUM(VENTA_REPETIDORES_RETENCION_3M_21) VENTA_REPETIDORES_RETENCION_3M_21
                ,SUM(VENTA_REPETIDORES_RETENCION_3M_22) VENTA_REPETIDORES_RETENCION_3M_22
                ,SUM(VENTA_REPETIDORES_RETENCION_3M_23) VENTA_REPETIDORES_RETENCION_3M_23
                ,SUM(VENTA_REPETIDORES_RETENCION_3M_24) VENTA_REPETIDORES_RETENCION_3M_24

                --6M
                ,SUM(VENTA_REPETIDORES_RETENCION_6M_13) VENTA_REPETIDORES_RETENCION_6M_13
                ,SUM(VENTA_REPETIDORES_RETENCION_6M_14) VENTA_REPETIDORES_RETENCION_6M_14
                ,SUM(VENTA_REPETIDORES_RETENCION_6M_15) VENTA_REPETIDORES_RETENCION_6M_15
                ,SUM(VENTA_REPETIDORES_RETENCION_6M_16) VENTA_REPETIDORES_RETENCION_6M_16
                ,SUM(VENTA_REPETIDORES_RETENCION_6M_17) VENTA_REPETIDORES_RETENCION_6M_17
                ,SUM(VENTA_REPETIDORES_RETENCION_6M_18) VENTA_REPETIDORES_RETENCION_6M_18
                ,SUM(VENTA_REPETIDORES_RETENCION_6M_19) VENTA_REPETIDORES_RETENCION_6M_19
                ,SUM(VENTA_REPETIDORES_RETENCION_6M_20) VENTA_REPETIDORES_RETENCION_6M_20
                ,SUM(VENTA_REPETIDORES_RETENCION_6M_21) VENTA_REPETIDORES_RETENCION_6M_21
                ,SUM(VENTA_REPETIDORES_RETENCION_6M_22) VENTA_REPETIDORES_RETENCION_6M_22
                ,SUM(VENTA_REPETIDORES_RETENCION_6M_23) VENTA_REPETIDORES_RETENCION_6M_23
                ,SUM(VENTA_REPETIDORES_RETENCION_6M_24) VENTA_REPETIDORES_RETENCION_6M_24

                --LEALES
                ,SUM(VENTA_LEALES_13) VENTA_LEALES_13
                ,SUM(VENTA_LEALES_14) VENTA_LEALES_14
                ,SUM(VENTA_LEALES_15) VENTA_LEALES_15
                ,SUM(VENTA_LEALES_16) VENTA_LEALES_16
                ,SUM(VENTA_LEALES_17) VENTA_LEALES_17
                ,SUM(VENTA_LEALES_18) VENTA_LEALES_18
                ,SUM(VENTA_LEALES_19) VENTA_LEALES_19
                ,SUM(VENTA_LEALES_20) VENTA_LEALES_20
                ,SUM(VENTA_LEALES_21) VENTA_LEALES_21
                ,SUM(VENTA_LEALES_22) VENTA_LEALES_22
                ,SUM(VENTA_LEALES_23) VENTA_LEALES_23
                ,SUM(VENTA_LEALES_24) VENTA_LEALES_24      

                --1M
                ,SUM(VENTA_LEALES_RETENCION_1M_13) VENTA_LEALES_RETENCION_1M_13
                ,SUM(VENTA_LEALES_RETENCION_1M_14) VENTA_LEALES_RETENCION_1M_14
                ,SUM(VENTA_LEALES_RETENCION_1M_15) VENTA_LEALES_RETENCION_1M_15
                ,SUM(VENTA_LEALES_RETENCION_1M_16) VENTA_LEALES_RETENCION_1M_16
                ,SUM(VENTA_LEALES_RETENCION_1M_17) VENTA_LEALES_RETENCION_1M_17
                ,SUM(VENTA_LEALES_RETENCION_1M_18) VENTA_LEALES_RETENCION_1M_18
                ,SUM(VENTA_LEALES_RETENCION_1M_19) VENTA_LEALES_RETENCION_1M_19
                ,SUM(VENTA_LEALES_RETENCION_1M_20) VENTA_LEALES_RETENCION_1M_20
                ,SUM(VENTA_LEALES_RETENCION_1M_21) VENTA_LEALES_RETENCION_1M_21
                ,SUM(VENTA_LEALES_RETENCION_1M_22) VENTA_LEALES_RETENCION_1M_22
                ,SUM(VENTA_LEALES_RETENCION_1M_23) VENTA_LEALES_RETENCION_1M_23
                ,SUM(VENTA_LEALES_RETENCION_1M_24) VENTA_LEALES_RETENCION_1M_24

                --2M
                ,SUM(VENTA_LEALES_RETENCION_2M_13) VENTA_LEALES_RETENCION_2M_13
                ,SUM(VENTA_LEALES_RETENCION_2M_14) VENTA_LEALES_RETENCION_2M_14
                ,SUM(VENTA_LEALES_RETENCION_2M_15) VENTA_LEALES_RETENCION_2M_15
                ,SUM(VENTA_LEALES_RETENCION_2M_16) VENTA_LEALES_RETENCION_2M_16
                ,SUM(VENTA_LEALES_RETENCION_2M_17) VENTA_LEALES_RETENCION_2M_17
                ,SUM(VENTA_LEALES_RETENCION_2M_18) VENTA_LEALES_RETENCION_2M_18
                ,SUM(VENTA_LEALES_RETENCION_2M_19) VENTA_LEALES_RETENCION_2M_19
                ,SUM(VENTA_LEALES_RETENCION_2M_20) VENTA_LEALES_RETENCION_2M_20
                ,SUM(VENTA_LEALES_RETENCION_2M_21) VENTA_LEALES_RETENCION_2M_21
                ,SUM(VENTA_LEALES_RETENCION_2M_22) VENTA_LEALES_RETENCION_2M_22
                ,SUM(VENTA_LEALES_RETENCION_2M_23) VENTA_LEALES_RETENCION_2M_23
                ,SUM(VENTA_LEALES_RETENCION_2M_24) VENTA_LEALES_RETENCION_2M_24

                --3M
                ,SUM(VENTA_LEALES_RETENCION_3M_13) VENTA_LEALES_RETENCION_3M_13
                ,SUM(VENTA_LEALES_RETENCION_3M_14) VENTA_LEALES_RETENCION_3M_14
                ,SUM(VENTA_LEALES_RETENCION_3M_15) VENTA_LEALES_RETENCION_3M_15
                ,SUM(VENTA_LEALES_RETENCION_3M_16) VENTA_LEALES_RETENCION_3M_16
                ,SUM(VENTA_LEALES_RETENCION_3M_17) VENTA_LEALES_RETENCION_3M_17
                ,SUM(VENTA_LEALES_RETENCION_3M_18) VENTA_LEALES_RETENCION_3M_18
                ,SUM(VENTA_LEALES_RETENCION_3M_19) VENTA_LEALES_RETENCION_3M_19
                ,SUM(VENTA_LEALES_RETENCION_3M_20) VENTA_LEALES_RETENCION_3M_20
                ,SUM(VENTA_LEALES_RETENCION_3M_21) VENTA_LEALES_RETENCION_3M_21
                ,SUM(VENTA_LEALES_RETENCION_3M_22) VENTA_LEALES_RETENCION_3M_22
                ,SUM(VENTA_LEALES_RETENCION_3M_23) VENTA_LEALES_RETENCION_3M_23
                ,SUM(VENTA_LEALES_RETENCION_3M_24) VENTA_LEALES_RETENCION_3M_24

                --6M
                ,SUM(VENTA_LEALES_RETENCION_6M_13) VENTA_LEALES_RETENCION_6M_13
                ,SUM(VENTA_LEALES_RETENCION_6M_14) VENTA_LEALES_RETENCION_6M_14
                ,SUM(VENTA_LEALES_RETENCION_6M_15) VENTA_LEALES_RETENCION_6M_15
                ,SUM(VENTA_LEALES_RETENCION_6M_16) VENTA_LEALES_RETENCION_6M_16
                ,SUM(VENTA_LEALES_RETENCION_6M_17) VENTA_LEALES_RETENCION_6M_17
                ,SUM(VENTA_LEALES_RETENCION_6M_18) VENTA_LEALES_RETENCION_6M_18
                ,SUM(VENTA_LEALES_RETENCION_6M_19) VENTA_LEALES_RETENCION_6M_19
                ,SUM(VENTA_LEALES_RETENCION_6M_20) VENTA_LEALES_RETENCION_6M_20
                ,SUM(VENTA_LEALES_RETENCION_6M_21) VENTA_LEALES_RETENCION_6M_21
                ,SUM(VENTA_LEALES_RETENCION_6M_22) VENTA_LEALES_RETENCION_6M_22
                ,SUM(VENTA_LEALES_RETENCION_6M_23) VENTA_LEALES_RETENCION_6M_23
                ,SUM(VENTA_LEALES_RETENCION_6M_24) VENTA_LEALES_RETENCION_6M_24

                FROM __RETENCION
                GROUP BY 1,2
            )
            ,__RESUMEN AS (
                SELECT PROVEEDOR,MARCA,13 AS IND_MES,CLIENTES_13 AS CLIENTES,VENTA_13 AS VENTA,TX_13 AS TX,RECOMPRA_13 AS RECOMPRA,RECOMPRA_RETENCION_1M_13 AS RECOMPRA_RETENCION_1M, RECOMPRA_RETENCION_2M_13 AS RECOMPRA_RETENCION_2M, RECOMPRA_RETENCION_3M_13 AS RECOMPRA_RETENCION_3M, RECOMPRA_RETENCION_6M_13 AS RECOMPRA_RETENCION_6M,FID_13 AS FID,FID_RETENCION_1M_13 AS FID_RETENCION_1M, FID_RETENCION_2M_13 AS FID_RETENCION_2M, FID_RETENCION_3M_13 AS FID_RETENCION_3M, FID_RETENCION_6M_13 AS FID_RETENCION_6M,REC_13 AS REC,REC_RETENCION_1M_13 AS REC_RETENCION_1M, REC_RETENCION_2M_13 AS REC_RETENCION_2M, REC_RETENCION_3M_13 AS REC_RETENCION_3M, REC_RETENCION_6M_13 AS REC_RETENCION_6M,NUEVOS_13 AS NUEVOS,NUEVOS_RETENCION_1M_13 AS NUEVOS_RETENCION_1M, NUEVOS_RETENCION_2M_13 AS NUEVOS_RETENCION_2M, NUEVOS_RETENCION_3M_13 AS NUEVOS_RETENCION_3M, NUEVOS_RETENCION_6M_13 AS NUEVOS_RETENCION_6M,REPETIDORES_13 AS REPETIDORES,REPETIDORES_RETENCION_1M_13 AS REPETIDORES_RETENCION_1M, REPETIDORES_RETENCION_2M_13 AS REPETIDORES_RETENCION_2M, REPETIDORES_RETENCION_3M_13 AS REPETIDORES_RETENCION_3M, REPETIDORES_RETENCION_6M_13 AS REPETIDORES_RETENCION_6M,LEALES_13 AS LEALES,LEALES_RETENCION_1M_13 AS LEALES_RETENCION_1M, LEALES_RETENCION_2M_13 AS LEALES_RETENCION_2M, LEALES_RETENCION_3M_13 AS LEALES_RETENCION_3M, LEALES_RETENCION_6M_13 AS LEALES_RETENCION_6M,VENTA_RECOMPRA_13 AS VENTA_RECOMPRA,VENTA_RECOMPRA_RETENCION_1M_13 AS VENTA_RECOMPRA_RETENCION_1M, VENTA_RECOMPRA_RETENCION_2M_13 AS VENTA_RECOMPRA_RETENCION_2M, VENTA_RECOMPRA_RETENCION_3M_13 AS VENTA_RECOMPRA_RETENCION_3M, VENTA_RECOMPRA_RETENCION_6M_13 AS VENTA_RECOMPRA_RETENCION_6M,VENTA_FID_13 AS VENTA_FID,VENTA_FID_RETENCION_1M_13 AS VENTA_FID_RETENCION_1M, VENTA_FID_RETENCION_2M_13 AS VENTA_FID_RETENCION_2M, VENTA_FID_RETENCION_3M_13 AS VENTA_FID_RETENCION_3M, VENTA_FID_RETENCION_6M_13 AS VENTA_FID_RETENCION_6M,VENTA_REC_13 AS VENTA_REC,VENTA_REC_RETENCION_1M_13 AS VENTA_REC_RETENCION_1M, VENTA_REC_RETENCION_2M_13 AS VENTA_REC_RETENCION_2M, VENTA_REC_RETENCION_3M_13 AS VENTA_REC_RETENCION_3M, VENTA_REC_RETENCION_6M_13 AS VENTA_REC_RETENCION_6M,VENTA_NUEVOS_13 AS VENTA_NUEVOS,VENTA_NUEVOS_RETENCION_1M_13 AS VENTA_NUEVOS_RETENCION_1M, VENTA_NUEVOS_RETENCION_2M_13 AS VENTA_NUEVOS_RETENCION_2M, VENTA_NUEVOS_RETENCION_3M_13 AS VENTA_NUEVOS_RETENCION_3M, VENTA_NUEVOS_RETENCION_6M_13 AS VENTA_NUEVOS_RETENCION_6M,VENTA_REPETIDORES_13 AS VENTA_REPETIDORES,VENTA_REPETIDORES_RETENCION_1M_13 AS VENTA_REPETIDORES_RETENCION_1M, VENTA_REPETIDORES_RETENCION_2M_13 AS VENTA_REPETIDORES_RETENCION_2M, VENTA_REPETIDORES_RETENCION_3M_13 AS VENTA_REPETIDORES_RETENCION_3M, VENTA_REPETIDORES_RETENCION_6M_13 AS VENTA_REPETIDORES_RETENCION_6M,VENTA_LEALES_13 AS VENTA_LEALES,VENTA_LEALES_RETENCION_1M_13 AS VENTA_LEALES_RETENCION_1M, VENTA_LEALES_RETENCION_2M_13 AS VENTA_LEALES_RETENCION_2M, VENTA_LEALES_RETENCION_3M_13 AS VENTA_LEALES_RETENCION_3M, VENTA_LEALES_RETENCION_6M_13 AS VENTA_LEALES_RETENCION_6M FROM __CLIENTES WHERE IND_MES = 13
                UNION
                SELECT PROVEEDOR,MARCA,14 AS IND_MES,CLIENTES_14 AS CLIENTES,VENTA_14 AS VENTA,TX_14 AS TX,RECOMPRA_14 AS RECOMPRA,RECOMPRA_RETENCION_1M_14 AS RECOMPRA_RETENCION_1M, RECOMPRA_RETENCION_2M_14 AS RECOMPRA_RETENCION_2M, RECOMPRA_RETENCION_3M_14 AS RECOMPRA_RETENCION_3M, RECOMPRA_RETENCION_6M_14 AS RECOMPRA_RETENCION_6M,FID_14 AS FID,FID_RETENCION_1M_14 AS FID_RETENCION_1M, FID_RETENCION_2M_14 AS FID_RETENCION_2M, FID_RETENCION_3M_14 AS FID_RETENCION_3M, FID_RETENCION_6M_14 AS FID_RETENCION_6M,REC_14 AS REC,REC_RETENCION_1M_14 AS REC_RETENCION_1M, REC_RETENCION_2M_14 AS REC_RETENCION_2M, REC_RETENCION_3M_14 AS REC_RETENCION_3M, REC_RETENCION_6M_14 AS REC_RETENCION_6M,NUEVOS_14 AS NUEVOS,NUEVOS_RETENCION_1M_14 AS NUEVOS_RETENCION_1M, NUEVOS_RETENCION_2M_14 AS NUEVOS_RETENCION_2M, NUEVOS_RETENCION_3M_14 AS NUEVOS_RETENCION_3M, NUEVOS_RETENCION_6M_14 AS NUEVOS_RETENCION_6M,REPETIDORES_14 AS REPETIDORES,REPETIDORES_RETENCION_1M_14 AS REPETIDORES_RETENCION_1M, REPETIDORES_RETENCION_2M_14 AS REPETIDORES_RETENCION_2M, REPETIDORES_RETENCION_3M_14 AS REPETIDORES_RETENCION_3M, REPETIDORES_RETENCION_6M_14 AS REPETIDORES_RETENCION_6M,LEALES_14 AS LEALES,LEALES_RETENCION_1M_14 AS LEALES_RETENCION_1M, LEALES_RETENCION_2M_14 AS LEALES_RETENCION_2M, LEALES_RETENCION_3M_14 AS LEALES_RETENCION_3M, LEALES_RETENCION_6M_14 AS LEALES_RETENCION_6M,VENTA_RECOMPRA_14 AS VENTA_RECOMPRA,VENTA_RECOMPRA_RETENCION_1M_14 AS VENTA_RECOMPRA_RETENCION_1M, VENTA_RECOMPRA_RETENCION_2M_14 AS VENTA_RECOMPRA_RETENCION_2M, VENTA_RECOMPRA_RETENCION_3M_14 AS VENTA_RECOMPRA_RETENCION_3M, VENTA_RECOMPRA_RETENCION_6M_14 AS VENTA_RECOMPRA_RETENCION_6M,VENTA_FID_14 AS VENTA_FID,VENTA_FID_RETENCION_1M_14 AS VENTA_FID_RETENCION_1M, VENTA_FID_RETENCION_2M_14 AS VENTA_FID_RETENCION_2M, VENTA_FID_RETENCION_3M_14 AS VENTA_FID_RETENCION_3M, VENTA_FID_RETENCION_6M_14 AS VENTA_FID_RETENCION_6M,VENTA_REC_14 AS VENTA_REC,VENTA_REC_RETENCION_1M_14 AS VENTA_REC_RETENCION_1M, VENTA_REC_RETENCION_2M_14 AS VENTA_REC_RETENCION_2M, VENTA_REC_RETENCION_3M_14 AS VENTA_REC_RETENCION_3M, VENTA_REC_RETENCION_6M_14 AS VENTA_REC_RETENCION_6M,VENTA_NUEVOS_14 AS VENTA_NUEVOS,VENTA_NUEVOS_RETENCION_1M_14 AS VENTA_NUEVOS_RETENCION_1M, VENTA_NUEVOS_RETENCION_2M_14 AS VENTA_NUEVOS_RETENCION_2M, VENTA_NUEVOS_RETENCION_3M_14 AS VENTA_NUEVOS_RETENCION_3M, VENTA_NUEVOS_RETENCION_6M_14 AS VENTA_NUEVOS_RETENCION_6M,VENTA_REPETIDORES_14 AS VENTA_REPETIDORES,VENTA_REPETIDORES_RETENCION_1M_14 AS VENTA_REPETIDORES_RETENCION_1M, VENTA_REPETIDORES_RETENCION_2M_14 AS VENTA_REPETIDORES_RETENCION_2M, VENTA_REPETIDORES_RETENCION_3M_14 AS VENTA_REPETIDORES_RETENCION_3M, VENTA_REPETIDORES_RETENCION_6M_14 AS VENTA_REPETIDORES_RETENCION_6M,VENTA_LEALES_14 AS VENTA_LEALES,VENTA_LEALES_RETENCION_1M_14 AS VENTA_LEALES_RETENCION_1M, VENTA_LEALES_RETENCION_2M_14 AS VENTA_LEALES_RETENCION_2M, VENTA_LEALES_RETENCION_3M_14 AS VENTA_LEALES_RETENCION_3M, VENTA_LEALES_RETENCION_6M_14 AS VENTA_LEALES_RETENCION_6M FROM __CLIENTES WHERE IND_MES = 14
                UNION
                SELECT PROVEEDOR,MARCA,15 AS IND_MES,CLIENTES_15 AS CLIENTES,VENTA_15 AS VENTA,TX_15 AS TX,RECOMPRA_15 AS RECOMPRA,RECOMPRA_RETENCION_1M_15 AS RECOMPRA_RETENCION_1M, RECOMPRA_RETENCION_2M_15 AS RECOMPRA_RETENCION_2M, RECOMPRA_RETENCION_3M_15 AS RECOMPRA_RETENCION_3M, RECOMPRA_RETENCION_6M_15 AS RECOMPRA_RETENCION_6M,FID_15 AS FID,FID_RETENCION_1M_15 AS FID_RETENCION_1M, FID_RETENCION_2M_15 AS FID_RETENCION_2M, FID_RETENCION_3M_15 AS FID_RETENCION_3M, FID_RETENCION_6M_15 AS FID_RETENCION_6M,REC_15 AS REC,REC_RETENCION_1M_15 AS REC_RETENCION_1M, REC_RETENCION_2M_15 AS REC_RETENCION_2M, REC_RETENCION_3M_15 AS REC_RETENCION_3M, REC_RETENCION_6M_15 AS REC_RETENCION_6M,NUEVOS_15 AS NUEVOS,NUEVOS_RETENCION_1M_15 AS NUEVOS_RETENCION_1M, NUEVOS_RETENCION_2M_15 AS NUEVOS_RETENCION_2M, NUEVOS_RETENCION_3M_15 AS NUEVOS_RETENCION_3M, NUEVOS_RETENCION_6M_15 AS NUEVOS_RETENCION_6M,REPETIDORES_15 AS REPETIDORES,REPETIDORES_RETENCION_1M_15 AS REPETIDORES_RETENCION_1M, REPETIDORES_RETENCION_2M_15 AS REPETIDORES_RETENCION_2M, REPETIDORES_RETENCION_3M_15 AS REPETIDORES_RETENCION_3M, REPETIDORES_RETENCION_6M_15 AS REPETIDORES_RETENCION_6M,LEALES_15 AS LEALES,LEALES_RETENCION_1M_15 AS LEALES_RETENCION_1M, LEALES_RETENCION_2M_15 AS LEALES_RETENCION_2M, LEALES_RETENCION_3M_15 AS LEALES_RETENCION_3M, LEALES_RETENCION_6M_15 AS LEALES_RETENCION_6M,VENTA_RECOMPRA_15 AS VENTA_RECOMPRA,VENTA_RECOMPRA_RETENCION_1M_15 AS VENTA_RECOMPRA_RETENCION_1M, VENTA_RECOMPRA_RETENCION_2M_15 AS VENTA_RECOMPRA_RETENCION_2M, VENTA_RECOMPRA_RETENCION_3M_15 AS VENTA_RECOMPRA_RETENCION_3M, VENTA_RECOMPRA_RETENCION_6M_15 AS VENTA_RECOMPRA_RETENCION_6M,VENTA_FID_15 AS VENTA_FID,VENTA_FID_RETENCION_1M_15 AS VENTA_FID_RETENCION_1M, VENTA_FID_RETENCION_2M_15 AS VENTA_FID_RETENCION_2M, VENTA_FID_RETENCION_3M_15 AS VENTA_FID_RETENCION_3M, VENTA_FID_RETENCION_6M_15 AS VENTA_FID_RETENCION_6M,VENTA_REC_15 AS VENTA_REC,VENTA_REC_RETENCION_1M_15 AS VENTA_REC_RETENCION_1M, VENTA_REC_RETENCION_2M_15 AS VENTA_REC_RETENCION_2M, VENTA_REC_RETENCION_3M_15 AS VENTA_REC_RETENCION_3M, VENTA_REC_RETENCION_6M_15 AS VENTA_REC_RETENCION_6M,VENTA_NUEVOS_15 AS VENTA_NUEVOS,VENTA_NUEVOS_RETENCION_1M_15 AS VENTA_NUEVOS_RETENCION_1M, VENTA_NUEVOS_RETENCION_2M_15 AS VENTA_NUEVOS_RETENCION_2M, VENTA_NUEVOS_RETENCION_3M_15 AS VENTA_NUEVOS_RETENCION_3M, VENTA_NUEVOS_RETENCION_6M_15 AS VENTA_NUEVOS_RETENCION_6M,VENTA_REPETIDORES_15 AS VENTA_REPETIDORES,VENTA_REPETIDORES_RETENCION_1M_15 AS VENTA_REPETIDORES_RETENCION_1M, VENTA_REPETIDORES_RETENCION_2M_15 AS VENTA_REPETIDORES_RETENCION_2M, VENTA_REPETIDORES_RETENCION_3M_15 AS VENTA_REPETIDORES_RETENCION_3M, VENTA_REPETIDORES_RETENCION_6M_15 AS VENTA_REPETIDORES_RETENCION_6M,VENTA_LEALES_15 AS VENTA_LEALES,VENTA_LEALES_RETENCION_1M_15 AS VENTA_LEALES_RETENCION_1M, VENTA_LEALES_RETENCION_2M_15 AS VENTA_LEALES_RETENCION_2M, VENTA_LEALES_RETENCION_3M_15 AS VENTA_LEALES_RETENCION_3M, VENTA_LEALES_RETENCION_6M_15 AS VENTA_LEALES_RETENCION_6M FROM __CLIENTES WHERE IND_MES = 15
                UNION
                SELECT PROVEEDOR,MARCA,16 AS IND_MES,CLIENTES_16 AS CLIENTES,VENTA_16 AS VENTA,TX_16 AS TX,RECOMPRA_16 AS RECOMPRA,RECOMPRA_RETENCION_1M_16 AS RECOMPRA_RETENCION_1M, RECOMPRA_RETENCION_2M_16 AS RECOMPRA_RETENCION_2M, RECOMPRA_RETENCION_3M_16 AS RECOMPRA_RETENCION_3M, RECOMPRA_RETENCION_6M_16 AS RECOMPRA_RETENCION_6M,FID_16 AS FID,FID_RETENCION_1M_16 AS FID_RETENCION_1M, FID_RETENCION_2M_16 AS FID_RETENCION_2M, FID_RETENCION_3M_16 AS FID_RETENCION_3M, FID_RETENCION_6M_16 AS FID_RETENCION_6M,REC_16 AS REC,REC_RETENCION_1M_16 AS REC_RETENCION_1M, REC_RETENCION_2M_16 AS REC_RETENCION_2M, REC_RETENCION_3M_16 AS REC_RETENCION_3M, REC_RETENCION_6M_16 AS REC_RETENCION_6M,NUEVOS_16 AS NUEVOS,NUEVOS_RETENCION_1M_16 AS NUEVOS_RETENCION_1M, NUEVOS_RETENCION_2M_16 AS NUEVOS_RETENCION_2M, NUEVOS_RETENCION_3M_16 AS NUEVOS_RETENCION_3M, NUEVOS_RETENCION_6M_16 AS NUEVOS_RETENCION_6M,REPETIDORES_16 AS REPETIDORES,REPETIDORES_RETENCION_1M_16 AS REPETIDORES_RETENCION_1M, REPETIDORES_RETENCION_2M_16 AS REPETIDORES_RETENCION_2M, REPETIDORES_RETENCION_3M_16 AS REPETIDORES_RETENCION_3M, REPETIDORES_RETENCION_6M_16 AS REPETIDORES_RETENCION_6M,LEALES_16 AS LEALES,LEALES_RETENCION_1M_16 AS LEALES_RETENCION_1M, LEALES_RETENCION_2M_16 AS LEALES_RETENCION_2M, LEALES_RETENCION_3M_16 AS LEALES_RETENCION_3M, LEALES_RETENCION_6M_16 AS LEALES_RETENCION_6M,VENTA_RECOMPRA_16 AS VENTA_RECOMPRA,VENTA_RECOMPRA_RETENCION_1M_16 AS VENTA_RECOMPRA_RETENCION_1M, VENTA_RECOMPRA_RETENCION_2M_16 AS VENTA_RECOMPRA_RETENCION_2M, VENTA_RECOMPRA_RETENCION_3M_16 AS VENTA_RECOMPRA_RETENCION_3M, VENTA_RECOMPRA_RETENCION_6M_16 AS VENTA_RECOMPRA_RETENCION_6M,VENTA_FID_16 AS VENTA_FID,VENTA_FID_RETENCION_1M_16 AS VENTA_FID_RETENCION_1M, VENTA_FID_RETENCION_2M_16 AS VENTA_FID_RETENCION_2M, VENTA_FID_RETENCION_3M_16 AS VENTA_FID_RETENCION_3M, VENTA_FID_RETENCION_6M_16 AS VENTA_FID_RETENCION_6M,VENTA_REC_16 AS VENTA_REC,VENTA_REC_RETENCION_1M_16 AS VENTA_REC_RETENCION_1M, VENTA_REC_RETENCION_2M_16 AS VENTA_REC_RETENCION_2M, VENTA_REC_RETENCION_3M_16 AS VENTA_REC_RETENCION_3M, VENTA_REC_RETENCION_6M_16 AS VENTA_REC_RETENCION_6M,VENTA_NUEVOS_16 AS VENTA_NUEVOS,VENTA_NUEVOS_RETENCION_1M_16 AS VENTA_NUEVOS_RETENCION_1M, VENTA_NUEVOS_RETENCION_2M_16 AS VENTA_NUEVOS_RETENCION_2M, VENTA_NUEVOS_RETENCION_3M_16 AS VENTA_NUEVOS_RETENCION_3M, VENTA_NUEVOS_RETENCION_6M_16 AS VENTA_NUEVOS_RETENCION_6M,VENTA_REPETIDORES_16 AS VENTA_REPETIDORES,VENTA_REPETIDORES_RETENCION_1M_16 AS VENTA_REPETIDORES_RETENCION_1M, VENTA_REPETIDORES_RETENCION_2M_16 AS VENTA_REPETIDORES_RETENCION_2M, VENTA_REPETIDORES_RETENCION_3M_16 AS VENTA_REPETIDORES_RETENCION_3M, VENTA_REPETIDORES_RETENCION_6M_16 AS VENTA_REPETIDORES_RETENCION_6M,VENTA_LEALES_16 AS VENTA_LEALES,VENTA_LEALES_RETENCION_1M_16 AS VENTA_LEALES_RETENCION_1M, VENTA_LEALES_RETENCION_2M_16 AS VENTA_LEALES_RETENCION_2M, VENTA_LEALES_RETENCION_3M_16 AS VENTA_LEALES_RETENCION_3M, VENTA_LEALES_RETENCION_6M_16 AS VENTA_LEALES_RETENCION_6M FROM __CLIENTES WHERE IND_MES = 16
                UNION
                SELECT PROVEEDOR,MARCA,17 AS IND_MES,CLIENTES_17 AS CLIENTES,VENTA_17 AS VENTA,TX_17 AS TX,RECOMPRA_17 AS RECOMPRA,RECOMPRA_RETENCION_1M_17 AS RECOMPRA_RETENCION_1M, RECOMPRA_RETENCION_2M_17 AS RECOMPRA_RETENCION_2M, RECOMPRA_RETENCION_3M_17 AS RECOMPRA_RETENCION_3M, RECOMPRA_RETENCION_6M_17 AS RECOMPRA_RETENCION_6M,FID_17 AS FID,FID_RETENCION_1M_17 AS FID_RETENCION_1M, FID_RETENCION_2M_17 AS FID_RETENCION_2M, FID_RETENCION_3M_17 AS FID_RETENCION_3M, FID_RETENCION_6M_17 AS FID_RETENCION_6M,REC_17 AS REC,REC_RETENCION_1M_17 AS REC_RETENCION_1M, REC_RETENCION_2M_17 AS REC_RETENCION_2M, REC_RETENCION_3M_17 AS REC_RETENCION_3M, REC_RETENCION_6M_17 AS REC_RETENCION_6M,NUEVOS_17 AS NUEVOS,NUEVOS_RETENCION_1M_17 AS NUEVOS_RETENCION_1M, NUEVOS_RETENCION_2M_17 AS NUEVOS_RETENCION_2M, NUEVOS_RETENCION_3M_17 AS NUEVOS_RETENCION_3M, NUEVOS_RETENCION_6M_17 AS NUEVOS_RETENCION_6M,REPETIDORES_17 AS REPETIDORES,REPETIDORES_RETENCION_1M_17 AS REPETIDORES_RETENCION_1M, REPETIDORES_RETENCION_2M_17 AS REPETIDORES_RETENCION_2M, REPETIDORES_RETENCION_3M_17 AS REPETIDORES_RETENCION_3M, REPETIDORES_RETENCION_6M_17 AS REPETIDORES_RETENCION_6M,LEALES_17 AS LEALES,LEALES_RETENCION_1M_17 AS LEALES_RETENCION_1M, LEALES_RETENCION_2M_17 AS LEALES_RETENCION_2M, LEALES_RETENCION_3M_17 AS LEALES_RETENCION_3M, LEALES_RETENCION_6M_17 AS LEALES_RETENCION_6M,VENTA_RECOMPRA_17 AS VENTA_RECOMPRA,VENTA_RECOMPRA_RETENCION_1M_17 AS VENTA_RECOMPRA_RETENCION_1M, VENTA_RECOMPRA_RETENCION_2M_17 AS VENTA_RECOMPRA_RETENCION_2M, VENTA_RECOMPRA_RETENCION_3M_17 AS VENTA_RECOMPRA_RETENCION_3M, VENTA_RECOMPRA_RETENCION_6M_17 AS VENTA_RECOMPRA_RETENCION_6M,VENTA_FID_17 AS VENTA_FID,VENTA_FID_RETENCION_1M_17 AS VENTA_FID_RETENCION_1M, VENTA_FID_RETENCION_2M_17 AS VENTA_FID_RETENCION_2M, VENTA_FID_RETENCION_3M_17 AS VENTA_FID_RETENCION_3M, VENTA_FID_RETENCION_6M_17 AS VENTA_FID_RETENCION_6M,VENTA_REC_17 AS VENTA_REC,VENTA_REC_RETENCION_1M_17 AS VENTA_REC_RETENCION_1M, VENTA_REC_RETENCION_2M_17 AS VENTA_REC_RETENCION_2M, VENTA_REC_RETENCION_3M_17 AS VENTA_REC_RETENCION_3M, VENTA_REC_RETENCION_6M_17 AS VENTA_REC_RETENCION_6M,VENTA_NUEVOS_17 AS VENTA_NUEVOS,VENTA_NUEVOS_RETENCION_1M_17 AS VENTA_NUEVOS_RETENCION_1M, VENTA_NUEVOS_RETENCION_2M_17 AS VENTA_NUEVOS_RETENCION_2M, VENTA_NUEVOS_RETENCION_3M_17 AS VENTA_NUEVOS_RETENCION_3M, VENTA_NUEVOS_RETENCION_6M_17 AS VENTA_NUEVOS_RETENCION_6M,VENTA_REPETIDORES_17 AS VENTA_REPETIDORES,VENTA_REPETIDORES_RETENCION_1M_17 AS VENTA_REPETIDORES_RETENCION_1M, VENTA_REPETIDORES_RETENCION_2M_17 AS VENTA_REPETIDORES_RETENCION_2M, VENTA_REPETIDORES_RETENCION_3M_17 AS VENTA_REPETIDORES_RETENCION_3M, VENTA_REPETIDORES_RETENCION_6M_17 AS VENTA_REPETIDORES_RETENCION_6M,VENTA_LEALES_17 AS VENTA_LEALES,VENTA_LEALES_RETENCION_1M_17 AS VENTA_LEALES_RETENCION_1M, VENTA_LEALES_RETENCION_2M_17 AS VENTA_LEALES_RETENCION_2M, VENTA_LEALES_RETENCION_3M_17 AS VENTA_LEALES_RETENCION_3M, VENTA_LEALES_RETENCION_6M_17 AS VENTA_LEALES_RETENCION_6M FROM __CLIENTES WHERE IND_MES = 17
                UNION
                SELECT PROVEEDOR,MARCA,18 AS IND_MES,CLIENTES_18 AS CLIENTES,VENTA_18 AS VENTA,TX_18 AS TX,RECOMPRA_18 AS RECOMPRA,RECOMPRA_RETENCION_1M_18 AS RECOMPRA_RETENCION_1M, RECOMPRA_RETENCION_2M_18 AS RECOMPRA_RETENCION_2M, RECOMPRA_RETENCION_3M_18 AS RECOMPRA_RETENCION_3M, RECOMPRA_RETENCION_6M_18 AS RECOMPRA_RETENCION_6M,FID_18 AS FID,FID_RETENCION_1M_18 AS FID_RETENCION_1M, FID_RETENCION_2M_18 AS FID_RETENCION_2M, FID_RETENCION_3M_18 AS FID_RETENCION_3M, FID_RETENCION_6M_18 AS FID_RETENCION_6M,REC_18 AS REC,REC_RETENCION_1M_18 AS REC_RETENCION_1M, REC_RETENCION_2M_18 AS REC_RETENCION_2M, REC_RETENCION_3M_18 AS REC_RETENCION_3M, REC_RETENCION_6M_18 AS REC_RETENCION_6M,NUEVOS_18 AS NUEVOS,NUEVOS_RETENCION_1M_18 AS NUEVOS_RETENCION_1M, NUEVOS_RETENCION_2M_18 AS NUEVOS_RETENCION_2M, NUEVOS_RETENCION_3M_18 AS NUEVOS_RETENCION_3M, NUEVOS_RETENCION_6M_18 AS NUEVOS_RETENCION_6M,REPETIDORES_18 AS REPETIDORES,REPETIDORES_RETENCION_1M_18 AS REPETIDORES_RETENCION_1M, REPETIDORES_RETENCION_2M_18 AS REPETIDORES_RETENCION_2M, REPETIDORES_RETENCION_3M_18 AS REPETIDORES_RETENCION_3M, REPETIDORES_RETENCION_6M_18 AS REPETIDORES_RETENCION_6M,LEALES_18 AS LEALES,LEALES_RETENCION_1M_18 AS LEALES_RETENCION_1M, LEALES_RETENCION_2M_18 AS LEALES_RETENCION_2M, LEALES_RETENCION_3M_18 AS LEALES_RETENCION_3M, LEALES_RETENCION_6M_18 AS LEALES_RETENCION_6M,VENTA_RECOMPRA_18 AS VENTA_RECOMPRA,VENTA_RECOMPRA_RETENCION_1M_18 AS VENTA_RECOMPRA_RETENCION_1M, VENTA_RECOMPRA_RETENCION_2M_18 AS VENTA_RECOMPRA_RETENCION_2M, VENTA_RECOMPRA_RETENCION_3M_18 AS VENTA_RECOMPRA_RETENCION_3M, VENTA_RECOMPRA_RETENCION_6M_18 AS VENTA_RECOMPRA_RETENCION_6M,VENTA_FID_18 AS VENTA_FID,VENTA_FID_RETENCION_1M_18 AS VENTA_FID_RETENCION_1M, VENTA_FID_RETENCION_2M_18 AS VENTA_FID_RETENCION_2M, VENTA_FID_RETENCION_3M_18 AS VENTA_FID_RETENCION_3M, VENTA_FID_RETENCION_6M_18 AS VENTA_FID_RETENCION_6M,VENTA_REC_18 AS VENTA_REC,VENTA_REC_RETENCION_1M_18 AS VENTA_REC_RETENCION_1M, VENTA_REC_RETENCION_2M_18 AS VENTA_REC_RETENCION_2M, VENTA_REC_RETENCION_3M_18 AS VENTA_REC_RETENCION_3M, VENTA_REC_RETENCION_6M_18 AS VENTA_REC_RETENCION_6M,VENTA_NUEVOS_18 AS VENTA_NUEVOS,VENTA_NUEVOS_RETENCION_1M_18 AS VENTA_NUEVOS_RETENCION_1M, VENTA_NUEVOS_RETENCION_2M_18 AS VENTA_NUEVOS_RETENCION_2M, VENTA_NUEVOS_RETENCION_3M_18 AS VENTA_NUEVOS_RETENCION_3M, VENTA_NUEVOS_RETENCION_6M_18 AS VENTA_NUEVOS_RETENCION_6M,VENTA_REPETIDORES_18 AS VENTA_REPETIDORES,VENTA_REPETIDORES_RETENCION_1M_18 AS VENTA_REPETIDORES_RETENCION_1M, VENTA_REPETIDORES_RETENCION_2M_18 AS VENTA_REPETIDORES_RETENCION_2M, VENTA_REPETIDORES_RETENCION_3M_18 AS VENTA_REPETIDORES_RETENCION_3M, VENTA_REPETIDORES_RETENCION_6M_18 AS VENTA_REPETIDORES_RETENCION_6M,VENTA_LEALES_18 AS VENTA_LEALES,VENTA_LEALES_RETENCION_1M_18 AS VENTA_LEALES_RETENCION_1M, VENTA_LEALES_RETENCION_2M_18 AS VENTA_LEALES_RETENCION_2M, VENTA_LEALES_RETENCION_3M_18 AS VENTA_LEALES_RETENCION_3M, VENTA_LEALES_RETENCION_6M_18 AS VENTA_LEALES_RETENCION_6M FROM __CLIENTES WHERE IND_MES = 18
                UNION
                SELECT PROVEEDOR,MARCA,19 AS IND_MES,CLIENTES_19 AS CLIENTES,VENTA_19 AS VENTA,TX_19 AS TX,RECOMPRA_19 AS RECOMPRA,RECOMPRA_RETENCION_1M_19 AS RECOMPRA_RETENCION_1M, RECOMPRA_RETENCION_2M_19 AS RECOMPRA_RETENCION_2M, RECOMPRA_RETENCION_3M_19 AS RECOMPRA_RETENCION_3M, RECOMPRA_RETENCION_6M_19 AS RECOMPRA_RETENCION_6M,FID_19 AS FID,FID_RETENCION_1M_19 AS FID_RETENCION_1M, FID_RETENCION_2M_19 AS FID_RETENCION_2M, FID_RETENCION_3M_19 AS FID_RETENCION_3M, FID_RETENCION_6M_19 AS FID_RETENCION_6M,REC_19 AS REC,REC_RETENCION_1M_19 AS REC_RETENCION_1M, REC_RETENCION_2M_19 AS REC_RETENCION_2M, REC_RETENCION_3M_19 AS REC_RETENCION_3M, REC_RETENCION_6M_19 AS REC_RETENCION_6M,NUEVOS_19 AS NUEVOS,NUEVOS_RETENCION_1M_19 AS NUEVOS_RETENCION_1M, NUEVOS_RETENCION_2M_19 AS NUEVOS_RETENCION_2M, NUEVOS_RETENCION_3M_19 AS NUEVOS_RETENCION_3M, NUEVOS_RETENCION_6M_19 AS NUEVOS_RETENCION_6M,REPETIDORES_19 AS REPETIDORES,REPETIDORES_RETENCION_1M_19 AS REPETIDORES_RETENCION_1M, REPETIDORES_RETENCION_2M_19 AS REPETIDORES_RETENCION_2M, REPETIDORES_RETENCION_3M_19 AS REPETIDORES_RETENCION_3M, REPETIDORES_RETENCION_6M_19 AS REPETIDORES_RETENCION_6M,LEALES_19 AS LEALES,LEALES_RETENCION_1M_19 AS LEALES_RETENCION_1M, LEALES_RETENCION_2M_19 AS LEALES_RETENCION_2M, LEALES_RETENCION_3M_19 AS LEALES_RETENCION_3M, LEALES_RETENCION_6M_19 AS LEALES_RETENCION_6M,VENTA_RECOMPRA_19 AS VENTA_RECOMPRA,VENTA_RECOMPRA_RETENCION_1M_19 AS VENTA_RECOMPRA_RETENCION_1M, VENTA_RECOMPRA_RETENCION_2M_19 AS VENTA_RECOMPRA_RETENCION_2M, VENTA_RECOMPRA_RETENCION_3M_19 AS VENTA_RECOMPRA_RETENCION_3M, VENTA_RECOMPRA_RETENCION_6M_19 AS VENTA_RECOMPRA_RETENCION_6M,VENTA_FID_19 AS VENTA_FID,VENTA_FID_RETENCION_1M_19 AS VENTA_FID_RETENCION_1M, VENTA_FID_RETENCION_2M_19 AS VENTA_FID_RETENCION_2M, VENTA_FID_RETENCION_3M_19 AS VENTA_FID_RETENCION_3M, VENTA_FID_RETENCION_6M_19 AS VENTA_FID_RETENCION_6M,VENTA_REC_19 AS VENTA_REC,VENTA_REC_RETENCION_1M_19 AS VENTA_REC_RETENCION_1M, VENTA_REC_RETENCION_2M_19 AS VENTA_REC_RETENCION_2M, VENTA_REC_RETENCION_3M_19 AS VENTA_REC_RETENCION_3M, VENTA_REC_RETENCION_6M_19 AS VENTA_REC_RETENCION_6M,VENTA_NUEVOS_19 AS VENTA_NUEVOS,VENTA_NUEVOS_RETENCION_1M_19 AS VENTA_NUEVOS_RETENCION_1M, VENTA_NUEVOS_RETENCION_2M_19 AS VENTA_NUEVOS_RETENCION_2M, VENTA_NUEVOS_RETENCION_3M_19 AS VENTA_NUEVOS_RETENCION_3M, VENTA_NUEVOS_RETENCION_6M_19 AS VENTA_NUEVOS_RETENCION_6M,VENTA_REPETIDORES_19 AS VENTA_REPETIDORES,VENTA_REPETIDORES_RETENCION_1M_19 AS VENTA_REPETIDORES_RETENCION_1M, VENTA_REPETIDORES_RETENCION_2M_19 AS VENTA_REPETIDORES_RETENCION_2M, VENTA_REPETIDORES_RETENCION_3M_19 AS VENTA_REPETIDORES_RETENCION_3M, VENTA_REPETIDORES_RETENCION_6M_19 AS VENTA_REPETIDORES_RETENCION_6M,VENTA_LEALES_19 AS VENTA_LEALES,VENTA_LEALES_RETENCION_1M_19 AS VENTA_LEALES_RETENCION_1M, VENTA_LEALES_RETENCION_2M_19 AS VENTA_LEALES_RETENCION_2M, VENTA_LEALES_RETENCION_3M_19 AS VENTA_LEALES_RETENCION_3M, VENTA_LEALES_RETENCION_6M_19 AS VENTA_LEALES_RETENCION_6M FROM __CLIENTES WHERE IND_MES = 19
                UNION
                SELECT PROVEEDOR,MARCA,20 AS IND_MES,CLIENTES_20 AS CLIENTES,VENTA_20 AS VENTA,TX_20 AS TX,RECOMPRA_20 AS RECOMPRA,RECOMPRA_RETENCION_1M_20 AS RECOMPRA_RETENCION_1M, RECOMPRA_RETENCION_2M_20 AS RECOMPRA_RETENCION_2M, RECOMPRA_RETENCION_3M_20 AS RECOMPRA_RETENCION_3M, RECOMPRA_RETENCION_6M_20 AS RECOMPRA_RETENCION_6M,FID_20 AS FID,FID_RETENCION_1M_20 AS FID_RETENCION_1M, FID_RETENCION_2M_20 AS FID_RETENCION_2M, FID_RETENCION_3M_20 AS FID_RETENCION_3M, FID_RETENCION_6M_20 AS FID_RETENCION_6M,REC_20 AS REC,REC_RETENCION_1M_20 AS REC_RETENCION_1M, REC_RETENCION_2M_20 AS REC_RETENCION_2M, REC_RETENCION_3M_20 AS REC_RETENCION_3M, REC_RETENCION_6M_20 AS REC_RETENCION_6M,NUEVOS_20 AS NUEVOS,NUEVOS_RETENCION_1M_20 AS NUEVOS_RETENCION_1M, NUEVOS_RETENCION_2M_20 AS NUEVOS_RETENCION_2M, NUEVOS_RETENCION_3M_20 AS NUEVOS_RETENCION_3M, NUEVOS_RETENCION_6M_20 AS NUEVOS_RETENCION_6M,REPETIDORES_20 AS REPETIDORES,REPETIDORES_RETENCION_1M_20 AS REPETIDORES_RETENCION_1M, REPETIDORES_RETENCION_2M_20 AS REPETIDORES_RETENCION_2M, REPETIDORES_RETENCION_3M_20 AS REPETIDORES_RETENCION_3M, REPETIDORES_RETENCION_6M_20 AS REPETIDORES_RETENCION_6M,LEALES_20 AS LEALES,LEALES_RETENCION_1M_20 AS LEALES_RETENCION_1M, LEALES_RETENCION_2M_20 AS LEALES_RETENCION_2M, LEALES_RETENCION_3M_20 AS LEALES_RETENCION_3M, LEALES_RETENCION_6M_20 AS LEALES_RETENCION_6M,VENTA_RECOMPRA_20 AS VENTA_RECOMPRA,VENTA_RECOMPRA_RETENCION_1M_20 AS VENTA_RECOMPRA_RETENCION_1M, VENTA_RECOMPRA_RETENCION_2M_20 AS VENTA_RECOMPRA_RETENCION_2M, VENTA_RECOMPRA_RETENCION_3M_20 AS VENTA_RECOMPRA_RETENCION_3M, VENTA_RECOMPRA_RETENCION_6M_20 AS VENTA_RECOMPRA_RETENCION_6M,VENTA_FID_20 AS VENTA_FID,VENTA_FID_RETENCION_1M_20 AS VENTA_FID_RETENCION_1M, VENTA_FID_RETENCION_2M_20 AS VENTA_FID_RETENCION_2M, VENTA_FID_RETENCION_3M_20 AS VENTA_FID_RETENCION_3M, VENTA_FID_RETENCION_6M_20 AS VENTA_FID_RETENCION_6M,VENTA_REC_20 AS VENTA_REC,VENTA_REC_RETENCION_1M_20 AS VENTA_REC_RETENCION_1M, VENTA_REC_RETENCION_2M_20 AS VENTA_REC_RETENCION_2M, VENTA_REC_RETENCION_3M_20 AS VENTA_REC_RETENCION_3M, VENTA_REC_RETENCION_6M_20 AS VENTA_REC_RETENCION_6M,VENTA_NUEVOS_20 AS VENTA_NUEVOS,VENTA_NUEVOS_RETENCION_1M_20 AS VENTA_NUEVOS_RETENCION_1M, VENTA_NUEVOS_RETENCION_2M_20 AS VENTA_NUEVOS_RETENCION_2M, VENTA_NUEVOS_RETENCION_3M_20 AS VENTA_NUEVOS_RETENCION_3M, VENTA_NUEVOS_RETENCION_6M_20 AS VENTA_NUEVOS_RETENCION_6M,VENTA_REPETIDORES_20 AS VENTA_REPETIDORES,VENTA_REPETIDORES_RETENCION_1M_20 AS VENTA_REPETIDORES_RETENCION_1M, VENTA_REPETIDORES_RETENCION_2M_20 AS VENTA_REPETIDORES_RETENCION_2M, VENTA_REPETIDORES_RETENCION_3M_20 AS VENTA_REPETIDORES_RETENCION_3M, VENTA_REPETIDORES_RETENCION_6M_20 AS VENTA_REPETIDORES_RETENCION_6M,VENTA_LEALES_20 AS VENTA_LEALES,VENTA_LEALES_RETENCION_1M_20 AS VENTA_LEALES_RETENCION_1M, VENTA_LEALES_RETENCION_2M_20 AS VENTA_LEALES_RETENCION_2M, VENTA_LEALES_RETENCION_3M_20 AS VENTA_LEALES_RETENCION_3M, VENTA_LEALES_RETENCION_6M_20 AS VENTA_LEALES_RETENCION_6M FROM __CLIENTES WHERE IND_MES = 20
                UNION
                SELECT PROVEEDOR,MARCA,21 AS IND_MES,CLIENTES_21 AS CLIENTES,VENTA_21 AS VENTA,TX_21 AS TX,RECOMPRA_21 AS RECOMPRA,RECOMPRA_RETENCION_1M_21 AS RECOMPRA_RETENCION_1M, RECOMPRA_RETENCION_2M_21 AS RECOMPRA_RETENCION_2M, RECOMPRA_RETENCION_3M_21 AS RECOMPRA_RETENCION_3M, RECOMPRA_RETENCION_6M_21 AS RECOMPRA_RETENCION_6M,FID_21 AS FID,FID_RETENCION_1M_21 AS FID_RETENCION_1M, FID_RETENCION_2M_21 AS FID_RETENCION_2M, FID_RETENCION_3M_21 AS FID_RETENCION_3M, FID_RETENCION_6M_21 AS FID_RETENCION_6M,REC_21 AS REC,REC_RETENCION_1M_21 AS REC_RETENCION_1M, REC_RETENCION_2M_21 AS REC_RETENCION_2M, REC_RETENCION_3M_21 AS REC_RETENCION_3M, REC_RETENCION_6M_21 AS REC_RETENCION_6M,NUEVOS_21 AS NUEVOS,NUEVOS_RETENCION_1M_21 AS NUEVOS_RETENCION_1M, NUEVOS_RETENCION_2M_21 AS NUEVOS_RETENCION_2M, NUEVOS_RETENCION_3M_21 AS NUEVOS_RETENCION_3M, NUEVOS_RETENCION_6M_21 AS NUEVOS_RETENCION_6M,REPETIDORES_21 AS REPETIDORES,REPETIDORES_RETENCION_1M_21 AS REPETIDORES_RETENCION_1M, REPETIDORES_RETENCION_2M_21 AS REPETIDORES_RETENCION_2M, REPETIDORES_RETENCION_3M_21 AS REPETIDORES_RETENCION_3M, REPETIDORES_RETENCION_6M_21 AS REPETIDORES_RETENCION_6M,LEALES_21 AS LEALES,LEALES_RETENCION_1M_21 AS LEALES_RETENCION_1M, LEALES_RETENCION_2M_21 AS LEALES_RETENCION_2M, LEALES_RETENCION_3M_21 AS LEALES_RETENCION_3M, LEALES_RETENCION_6M_21 AS LEALES_RETENCION_6M,VENTA_RECOMPRA_21 AS VENTA_RECOMPRA,VENTA_RECOMPRA_RETENCION_1M_21 AS VENTA_RECOMPRA_RETENCION_1M, VENTA_RECOMPRA_RETENCION_2M_21 AS VENTA_RECOMPRA_RETENCION_2M, VENTA_RECOMPRA_RETENCION_3M_21 AS VENTA_RECOMPRA_RETENCION_3M, VENTA_RECOMPRA_RETENCION_6M_21 AS VENTA_RECOMPRA_RETENCION_6M,VENTA_FID_21 AS VENTA_FID,VENTA_FID_RETENCION_1M_21 AS VENTA_FID_RETENCION_1M, VENTA_FID_RETENCION_2M_21 AS VENTA_FID_RETENCION_2M, VENTA_FID_RETENCION_3M_21 AS VENTA_FID_RETENCION_3M, VENTA_FID_RETENCION_6M_21 AS VENTA_FID_RETENCION_6M,VENTA_REC_21 AS VENTA_REC,VENTA_REC_RETENCION_1M_21 AS VENTA_REC_RETENCION_1M, VENTA_REC_RETENCION_2M_21 AS VENTA_REC_RETENCION_2M, VENTA_REC_RETENCION_3M_21 AS VENTA_REC_RETENCION_3M, VENTA_REC_RETENCION_6M_21 AS VENTA_REC_RETENCION_6M,VENTA_NUEVOS_21 AS VENTA_NUEVOS,VENTA_NUEVOS_RETENCION_1M_21 AS VENTA_NUEVOS_RETENCION_1M, VENTA_NUEVOS_RETENCION_2M_21 AS VENTA_NUEVOS_RETENCION_2M, VENTA_NUEVOS_RETENCION_3M_21 AS VENTA_NUEVOS_RETENCION_3M, VENTA_NUEVOS_RETENCION_6M_21 AS VENTA_NUEVOS_RETENCION_6M,VENTA_REPETIDORES_21 AS VENTA_REPETIDORES,VENTA_REPETIDORES_RETENCION_1M_21 AS VENTA_REPETIDORES_RETENCION_1M, VENTA_REPETIDORES_RETENCION_2M_21 AS VENTA_REPETIDORES_RETENCION_2M, VENTA_REPETIDORES_RETENCION_3M_21 AS VENTA_REPETIDORES_RETENCION_3M, VENTA_REPETIDORES_RETENCION_6M_21 AS VENTA_REPETIDORES_RETENCION_6M,VENTA_LEALES_21 AS VENTA_LEALES,VENTA_LEALES_RETENCION_1M_21 AS VENTA_LEALES_RETENCION_1M, VENTA_LEALES_RETENCION_2M_21 AS VENTA_LEALES_RETENCION_2M, VENTA_LEALES_RETENCION_3M_21 AS VENTA_LEALES_RETENCION_3M, VENTA_LEALES_RETENCION_6M_21 AS VENTA_LEALES_RETENCION_6M FROM __CLIENTES WHERE IND_MES = 21
                UNION
                SELECT PROVEEDOR,MARCA,22 AS IND_MES,CLIENTES_22 AS CLIENTES,VENTA_22 AS VENTA,TX_22 AS TX,RECOMPRA_22 AS RECOMPRA,RECOMPRA_RETENCION_1M_22 AS RECOMPRA_RETENCION_1M, RECOMPRA_RETENCION_2M_22 AS RECOMPRA_RETENCION_2M, RECOMPRA_RETENCION_3M_22 AS RECOMPRA_RETENCION_3M, RECOMPRA_RETENCION_6M_22 AS RECOMPRA_RETENCION_6M,FID_22 AS FID,FID_RETENCION_1M_22 AS FID_RETENCION_1M, FID_RETENCION_2M_22 AS FID_RETENCION_2M, FID_RETENCION_3M_22 AS FID_RETENCION_3M, FID_RETENCION_6M_22 AS FID_RETENCION_6M,REC_22 AS REC,REC_RETENCION_1M_22 AS REC_RETENCION_1M, REC_RETENCION_2M_22 AS REC_RETENCION_2M, REC_RETENCION_3M_22 AS REC_RETENCION_3M, REC_RETENCION_6M_22 AS REC_RETENCION_6M,NUEVOS_22 AS NUEVOS,NUEVOS_RETENCION_1M_22 AS NUEVOS_RETENCION_1M, NUEVOS_RETENCION_2M_22 AS NUEVOS_RETENCION_2M, NUEVOS_RETENCION_3M_22 AS NUEVOS_RETENCION_3M, NUEVOS_RETENCION_6M_22 AS NUEVOS_RETENCION_6M,REPETIDORES_22 AS REPETIDORES,REPETIDORES_RETENCION_1M_22 AS REPETIDORES_RETENCION_1M, REPETIDORES_RETENCION_2M_22 AS REPETIDORES_RETENCION_2M, REPETIDORES_RETENCION_3M_22 AS REPETIDORES_RETENCION_3M, REPETIDORES_RETENCION_6M_22 AS REPETIDORES_RETENCION_6M,LEALES_22 AS LEALES,LEALES_RETENCION_1M_22 AS LEALES_RETENCION_1M, LEALES_RETENCION_2M_22 AS LEALES_RETENCION_2M, LEALES_RETENCION_3M_22 AS LEALES_RETENCION_3M, LEALES_RETENCION_6M_22 AS LEALES_RETENCION_6M,VENTA_RECOMPRA_22 AS VENTA_RECOMPRA,VENTA_RECOMPRA_RETENCION_1M_22 AS VENTA_RECOMPRA_RETENCION_1M, VENTA_RECOMPRA_RETENCION_2M_22 AS VENTA_RECOMPRA_RETENCION_2M, VENTA_RECOMPRA_RETENCION_3M_22 AS VENTA_RECOMPRA_RETENCION_3M, VENTA_RECOMPRA_RETENCION_6M_22 AS VENTA_RECOMPRA_RETENCION_6M,VENTA_FID_22 AS VENTA_FID,VENTA_FID_RETENCION_1M_22 AS VENTA_FID_RETENCION_1M, VENTA_FID_RETENCION_2M_22 AS VENTA_FID_RETENCION_2M, VENTA_FID_RETENCION_3M_22 AS VENTA_FID_RETENCION_3M, VENTA_FID_RETENCION_6M_22 AS VENTA_FID_RETENCION_6M,VENTA_REC_22 AS VENTA_REC,VENTA_REC_RETENCION_1M_22 AS VENTA_REC_RETENCION_1M, VENTA_REC_RETENCION_2M_22 AS VENTA_REC_RETENCION_2M, VENTA_REC_RETENCION_3M_22 AS VENTA_REC_RETENCION_3M, VENTA_REC_RETENCION_6M_22 AS VENTA_REC_RETENCION_6M,VENTA_NUEVOS_22 AS VENTA_NUEVOS,VENTA_NUEVOS_RETENCION_1M_22 AS VENTA_NUEVOS_RETENCION_1M, VENTA_NUEVOS_RETENCION_2M_22 AS VENTA_NUEVOS_RETENCION_2M, VENTA_NUEVOS_RETENCION_3M_22 AS VENTA_NUEVOS_RETENCION_3M, VENTA_NUEVOS_RETENCION_6M_22 AS VENTA_NUEVOS_RETENCION_6M,VENTA_REPETIDORES_22 AS VENTA_REPETIDORES,VENTA_REPETIDORES_RETENCION_1M_22 AS VENTA_REPETIDORES_RETENCION_1M, VENTA_REPETIDORES_RETENCION_2M_22 AS VENTA_REPETIDORES_RETENCION_2M, VENTA_REPETIDORES_RETENCION_3M_22 AS VENTA_REPETIDORES_RETENCION_3M, VENTA_REPETIDORES_RETENCION_6M_22 AS VENTA_REPETIDORES_RETENCION_6M,VENTA_LEALES_22 AS VENTA_LEALES,VENTA_LEALES_RETENCION_1M_22 AS VENTA_LEALES_RETENCION_1M, VENTA_LEALES_RETENCION_2M_22 AS VENTA_LEALES_RETENCION_2M, VENTA_LEALES_RETENCION_3M_22 AS VENTA_LEALES_RETENCION_3M, VENTA_LEALES_RETENCION_6M_22 AS VENTA_LEALES_RETENCION_6M FROM __CLIENTES WHERE IND_MES = 22
                UNION
                SELECT PROVEEDOR,MARCA,23 AS IND_MES,CLIENTES_23 AS CLIENTES,VENTA_23 AS VENTA,TX_23 AS TX,RECOMPRA_23 AS RECOMPRA,RECOMPRA_RETENCION_1M_23 AS RECOMPRA_RETENCION_1M, RECOMPRA_RETENCION_2M_23 AS RECOMPRA_RETENCION_2M, RECOMPRA_RETENCION_3M_23 AS RECOMPRA_RETENCION_3M, RECOMPRA_RETENCION_6M_23 AS RECOMPRA_RETENCION_6M,FID_23 AS FID,FID_RETENCION_1M_23 AS FID_RETENCION_1M, FID_RETENCION_2M_23 AS FID_RETENCION_2M, FID_RETENCION_3M_23 AS FID_RETENCION_3M, FID_RETENCION_6M_23 AS FID_RETENCION_6M,REC_23 AS REC,REC_RETENCION_1M_23 AS REC_RETENCION_1M, REC_RETENCION_2M_23 AS REC_RETENCION_2M, REC_RETENCION_3M_23 AS REC_RETENCION_3M, REC_RETENCION_6M_23 AS REC_RETENCION_6M,NUEVOS_23 AS NUEVOS,NUEVOS_RETENCION_1M_23 AS NUEVOS_RETENCION_1M, NUEVOS_RETENCION_2M_23 AS NUEVOS_RETENCION_2M, NUEVOS_RETENCION_3M_23 AS NUEVOS_RETENCION_3M, NUEVOS_RETENCION_6M_23 AS NUEVOS_RETENCION_6M,REPETIDORES_23 AS REPETIDORES,REPETIDORES_RETENCION_1M_23 AS REPETIDORES_RETENCION_1M, REPETIDORES_RETENCION_2M_23 AS REPETIDORES_RETENCION_2M, REPETIDORES_RETENCION_3M_23 AS REPETIDORES_RETENCION_3M, REPETIDORES_RETENCION_6M_23 AS REPETIDORES_RETENCION_6M,LEALES_23 AS LEALES,LEALES_RETENCION_1M_23 AS LEALES_RETENCION_1M, LEALES_RETENCION_2M_23 AS LEALES_RETENCION_2M, LEALES_RETENCION_3M_23 AS LEALES_RETENCION_3M, LEALES_RETENCION_6M_23 AS LEALES_RETENCION_6M,VENTA_RECOMPRA_23 AS VENTA_RECOMPRA,VENTA_RECOMPRA_RETENCION_1M_23 AS VENTA_RECOMPRA_RETENCION_1M, VENTA_RECOMPRA_RETENCION_2M_23 AS VENTA_RECOMPRA_RETENCION_2M, VENTA_RECOMPRA_RETENCION_3M_23 AS VENTA_RECOMPRA_RETENCION_3M, VENTA_RECOMPRA_RETENCION_6M_23 AS VENTA_RECOMPRA_RETENCION_6M,VENTA_FID_23 AS VENTA_FID,VENTA_FID_RETENCION_1M_23 AS VENTA_FID_RETENCION_1M, VENTA_FID_RETENCION_2M_23 AS VENTA_FID_RETENCION_2M, VENTA_FID_RETENCION_3M_23 AS VENTA_FID_RETENCION_3M, VENTA_FID_RETENCION_6M_23 AS VENTA_FID_RETENCION_6M,VENTA_REC_23 AS VENTA_REC,VENTA_REC_RETENCION_1M_23 AS VENTA_REC_RETENCION_1M, VENTA_REC_RETENCION_2M_23 AS VENTA_REC_RETENCION_2M, VENTA_REC_RETENCION_3M_23 AS VENTA_REC_RETENCION_3M, VENTA_REC_RETENCION_6M_23 AS VENTA_REC_RETENCION_6M,VENTA_NUEVOS_23 AS VENTA_NUEVOS,VENTA_NUEVOS_RETENCION_1M_23 AS VENTA_NUEVOS_RETENCION_1M, VENTA_NUEVOS_RETENCION_2M_23 AS VENTA_NUEVOS_RETENCION_2M, VENTA_NUEVOS_RETENCION_3M_23 AS VENTA_NUEVOS_RETENCION_3M, VENTA_NUEVOS_RETENCION_6M_23 AS VENTA_NUEVOS_RETENCION_6M,VENTA_REPETIDORES_23 AS VENTA_REPETIDORES,VENTA_REPETIDORES_RETENCION_1M_23 AS VENTA_REPETIDORES_RETENCION_1M, VENTA_REPETIDORES_RETENCION_2M_23 AS VENTA_REPETIDORES_RETENCION_2M, VENTA_REPETIDORES_RETENCION_3M_23 AS VENTA_REPETIDORES_RETENCION_3M, VENTA_REPETIDORES_RETENCION_6M_23 AS VENTA_REPETIDORES_RETENCION_6M,VENTA_LEALES_23 AS VENTA_LEALES,VENTA_LEALES_RETENCION_1M_23 AS VENTA_LEALES_RETENCION_1M, VENTA_LEALES_RETENCION_2M_23 AS VENTA_LEALES_RETENCION_2M, VENTA_LEALES_RETENCION_3M_23 AS VENTA_LEALES_RETENCION_3M, VENTA_LEALES_RETENCION_6M_23 AS VENTA_LEALES_RETENCION_6M FROM __CLIENTES WHERE IND_MES = 23
                UNION
                SELECT PROVEEDOR,MARCA,24 AS IND_MES,CLIENTES_24 AS CLIENTES,VENTA_24 AS VENTA,TX_24 AS TX,RECOMPRA_24 AS RECOMPRA,RECOMPRA_RETENCION_1M_24 AS RECOMPRA_RETENCION_1M, RECOMPRA_RETENCION_2M_24 AS RECOMPRA_RETENCION_2M, RECOMPRA_RETENCION_3M_24 AS RECOMPRA_RETENCION_3M, RECOMPRA_RETENCION_6M_24 AS RECOMPRA_RETENCION_6M,FID_24 AS FID,FID_RETENCION_1M_24 AS FID_RETENCION_1M, FID_RETENCION_2M_24 AS FID_RETENCION_2M, FID_RETENCION_3M_24 AS FID_RETENCION_3M, FID_RETENCION_6M_24 AS FID_RETENCION_6M,REC_24 AS REC,REC_RETENCION_1M_24 AS REC_RETENCION_1M, REC_RETENCION_2M_24 AS REC_RETENCION_2M, REC_RETENCION_3M_24 AS REC_RETENCION_3M, REC_RETENCION_6M_24 AS REC_RETENCION_6M,NUEVOS_24 AS NUEVOS,NUEVOS_RETENCION_1M_24 AS NUEVOS_RETENCION_1M, NUEVOS_RETENCION_2M_24 AS NUEVOS_RETENCION_2M, NUEVOS_RETENCION_3M_24 AS NUEVOS_RETENCION_3M, NUEVOS_RETENCION_6M_24 AS NUEVOS_RETENCION_6M,REPETIDORES_24 AS REPETIDORES,REPETIDORES_RETENCION_1M_24 AS REPETIDORES_RETENCION_1M, REPETIDORES_RETENCION_2M_24 AS REPETIDORES_RETENCION_2M, REPETIDORES_RETENCION_3M_24 AS REPETIDORES_RETENCION_3M, REPETIDORES_RETENCION_6M_24 AS REPETIDORES_RETENCION_6M,LEALES_24 AS LEALES,LEALES_RETENCION_1M_24 AS LEALES_RETENCION_1M, LEALES_RETENCION_2M_24 AS LEALES_RETENCION_2M, LEALES_RETENCION_3M_24 AS LEALES_RETENCION_3M, LEALES_RETENCION_6M_24 AS LEALES_RETENCION_6M,VENTA_RECOMPRA_24 AS VENTA_RECOMPRA,VENTA_RECOMPRA_RETENCION_1M_24 AS VENTA_RECOMPRA_RETENCION_1M, VENTA_RECOMPRA_RETENCION_2M_24 AS VENTA_RECOMPRA_RETENCION_2M, VENTA_RECOMPRA_RETENCION_3M_24 AS VENTA_RECOMPRA_RETENCION_3M, VENTA_RECOMPRA_RETENCION_6M_24 AS VENTA_RECOMPRA_RETENCION_6M,VENTA_FID_24 AS VENTA_FID,VENTA_FID_RETENCION_1M_24 AS VENTA_FID_RETENCION_1M, VENTA_FID_RETENCION_2M_24 AS VENTA_FID_RETENCION_2M, VENTA_FID_RETENCION_3M_24 AS VENTA_FID_RETENCION_3M, VENTA_FID_RETENCION_6M_24 AS VENTA_FID_RETENCION_6M,VENTA_REC_24 AS VENTA_REC,VENTA_REC_RETENCION_1M_24 AS VENTA_REC_RETENCION_1M, VENTA_REC_RETENCION_2M_24 AS VENTA_REC_RETENCION_2M, VENTA_REC_RETENCION_3M_24 AS VENTA_REC_RETENCION_3M, VENTA_REC_RETENCION_6M_24 AS VENTA_REC_RETENCION_6M,VENTA_NUEVOS_24 AS VENTA_NUEVOS,VENTA_NUEVOS_RETENCION_1M_24 AS VENTA_NUEVOS_RETENCION_1M, VENTA_NUEVOS_RETENCION_2M_24 AS VENTA_NUEVOS_RETENCION_2M, VENTA_NUEVOS_RETENCION_3M_24 AS VENTA_NUEVOS_RETENCION_3M, VENTA_NUEVOS_RETENCION_6M_24 AS VENTA_NUEVOS_RETENCION_6M,VENTA_REPETIDORES_24 AS VENTA_REPETIDORES,VENTA_REPETIDORES_RETENCION_1M_24 AS VENTA_REPETIDORES_RETENCION_1M, VENTA_REPETIDORES_RETENCION_2M_24 AS VENTA_REPETIDORES_RETENCION_2M, VENTA_REPETIDORES_RETENCION_3M_24 AS VENTA_REPETIDORES_RETENCION_3M, VENTA_REPETIDORES_RETENCION_6M_24 AS VENTA_REPETIDORES_RETENCION_6M,VENTA_LEALES_24 AS VENTA_LEALES,VENTA_LEALES_RETENCION_1M_24 AS VENTA_LEALES_RETENCION_1M, VENTA_LEALES_RETENCION_2M_24 AS VENTA_LEALES_RETENCION_2M, VENTA_LEALES_RETENCION_3M_24 AS VENTA_LEALES_RETENCION_3M, VENTA_LEALES_RETENCION_6M_24 AS VENTA_LEALES_RETENCION_6M FROM __CLIENTES WHERE IND_MES = 24
            )
            SELECT
                '{self.campana_variables['codigo_campana']}' CODIGO_CAMPANA
                ,PROVEEDOR
                ,MARCA
                ,MES
                ,CLIENTES
                ,VENTA
                ,TX
                ,RECOMPRA
                ,RECOMPRA_RETENCION_1M
                ,RECOMPRA_RETENCION_2M
                ,RECOMPRA_RETENCION_3M
                ,RECOMPRA_RETENCION_6M
                ,FID
                ,FID_RETENCION_1M
                ,FID_RETENCION_2M
                ,FID_RETENCION_3M
                ,FID_RETENCION_6M
                ,REC
                ,REC_RETENCION_1M
                ,REC_RETENCION_2M
                ,REC_RETENCION_3M
                ,REC_RETENCION_6M
                ,NUEVOS
                ,NUEVOS_RETENCION_1M
                ,NUEVOS_RETENCION_2M
                ,NUEVOS_RETENCION_3M
                ,NUEVOS_RETENCION_6M
                ,REPETIDORES
                ,REPETIDORES_RETENCION_1M
                ,REPETIDORES_RETENCION_2M
                ,REPETIDORES_RETENCION_3M
                ,REPETIDORES_RETENCION_6M
                ,LEALES
                ,LEALES_RETENCION_1M
                ,LEALES_RETENCION_2M
                ,LEALES_RETENCION_3M
                ,LEALES_RETENCION_6M
                ,VENTA_RECOMPRA
                ,VENTA_RECOMPRA_RETENCION_1M
                ,VENTA_RECOMPRA_RETENCION_2M
                ,VENTA_RECOMPRA_RETENCION_3M
                ,VENTA_RECOMPRA_RETENCION_6M
                ,VENTA_FID
                ,VENTA_FID_RETENCION_1M
                ,VENTA_FID_RETENCION_2M
                ,VENTA_FID_RETENCION_3M
                ,VENTA_FID_RETENCION_6M
                ,VENTA_REC
                ,VENTA_REC_RETENCION_1M
                ,VENTA_REC_RETENCION_2M
                ,VENTA_REC_RETENCION_3M
                ,VENTA_REC_RETENCION_6M
                ,VENTA_NUEVOS
                ,VENTA_NUEVOS_RETENCION_1M
                ,VENTA_NUEVOS_RETENCION_2M
                ,VENTA_NUEVOS_RETENCION_3M
                ,VENTA_NUEVOS_RETENCION_6M
                ,VENTA_REPETIDORES
                ,VENTA_REPETIDORES_RETENCION_1M
                ,VENTA_REPETIDORES_RETENCION_2M
                ,VENTA_REPETIDORES_RETENCION_3M
                ,VENTA_REPETIDORES_RETENCION_6M
                ,VENTA_LEALES
                ,VENTA_LEALES_RETENCION_1M
                ,VENTA_LEALES_RETENCION_2M
                ,VENTA_LEALES_RETENCION_3M
                ,VENTA_LEALES_RETENCION_6M
            FROM __RESUMEN A
            LEFT JOIN #MESES B USING(IND_MES)
            );

            --SELECT * FROM #SEGMENTOS_RETENCION ORDER BY MES;
            '''
        
        query_insert = f'''
            DELETE CHEDRAUI.MON_CAMP_EVOLUTION_RETENTION WHERE CODIGO_CAMPANA = '{self.campana_variables['codigo_campana']}';
            INSERT INTO CHEDRAUI.MON_CAMP_EVOLUTION_RETENTION SELECT * FROM #SEGMENTOS_RETENCION;
        '''
        return [query_segmentos_retencion, query_insert]