import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta

# Create a Class to handle the Radiografia data
class Radiografia():
    def __init__(self):
        self.set_rad_variables()
        self.set_dict_tablas_radiografia_completa()
    
    def set_rad_variables(self, inicio='', termino='', nombre='', online=0):
        self.dict_fechas = {}
        self.nombre = nombre
        self.ind_online = online
        self.__get_fechas_campana(inicio, termino)

    def set_rad_corta_variables(self, nombre, fec_ini_campana, fec_fin_campana, fec_ini_analisis, fec_fin_analisis, condicion, online):
        # Seleccionar las fechas y offsets

        ini_campana = fec_ini_campana[:7]
        fin_campana = fec_fin_campana[:7]
        ini_analisis = fec_ini_analisis[:7]
        fin_analisis = fec_fin_analisis[:7]
        fin_analisis_aa = (pd.to_datetime(fec_fin_analisis) - pd.DateOffset(months=12)).strftime('%Y-%m-%d')[:7]
        ini_analisis_aa = (pd.to_datetime(fec_ini_analisis) - pd.DateOffset(months=12)).strftime('%Y-%m-%d')[:7]

        self.dict_rad_corta_var = {
            'nombre': nombre
            ,'ind_online': online
            ,'mes_ini_campana': ini_campana
            ,'mes_fin_campana': fin_campana
            ,'mes_fin_analisis': fin_analisis
            ,'mes_ini_analisis': ini_analisis
            ,'mes_fin_analisis_aa': fin_analisis_aa
            ,'mes_ini_analisis_aa': ini_analisis_aa
            ,'date_dash_campana': f"'{ini_campana}' AND '{fin_campana}'"
            ,'date_dash': f"'{ini_analisis}' AND '{fin_analisis}'"
            ,'date_dash_aa': f"'{ini_analisis_aa}' AND '{fin_analisis_aa}'"
            ,'condicion': condicion if condicion else 0
            ,'condicion_50': 50
            ,'condicion_75': 75
            ,'condicion_100': 100
            ,'condicion_150': 150
            ,'condicion_200': 200
        }

    def set_dict_tablas_radiografia_completa(self):
        # Crear tablas de Radiografía
        self.dict_tablas_radiografia_completa = {
            'Venta': None,
            'Lista de Productos': None,
            'Categorias': None,
            'Productos': None,
            'Marcas': None,
            'Funnel Clientes': None,
            'Evolucion': None,
            'Segmentos': None
            }

    def __get_fechas_campana(self, inicio, termino):
        # Crear diccionario de fechas
        if inicio and termino:
            # Obtener las fechas de inicio y fin un año antes
            inicio_a = (pd.to_datetime(inicio) - pd.DateOffset(years=1)).strftime('%Y-%m-%d')
            termino_a = (pd.to_datetime(termino) - pd.DateOffset(years=1)).strftime('%Y-%m-%d')

            # Obtener las fechas para dormidos y perdidos
            fecha_fin = datetime.strptime(termino, '%Y-%m-%d')
            
            self.dict_fechas = {
                'fecha_ini': inicio,
                'fecha_fin': termino,
                'fecha_ini_a': inicio_a,
                'fecha_fin_a': termino_a,
                'fecha_ini_d1': (pd.to_datetime(fecha_fin) - pd.DateOffset(months=6)).strftime('%Y-%m-%d'),
                'fecha_fin_d1': (pd.to_datetime(fecha_fin) - pd.DateOffset(months=4)).strftime('%Y-%m-%d'),
                'fecha_ini_d2': (pd.to_datetime(fecha_fin) - pd.DateOffset(months=3)).strftime('%Y-%m-%d'),
                'fecha_fin_d2': (pd.to_datetime(fecha_fin) - pd.DateOffset(months=1)).strftime('%Y-%m-%d'),
                'fecha_ini_p1': (pd.to_datetime(fecha_fin) - pd.DateOffset(years=1)).strftime('%Y-%m-%d'),
                'fecha_fin_p1': (pd.to_datetime(fecha_fin) - pd.DateOffset(months=7)).strftime('%Y-%m-%d'),
                'fecha_ini_p2': (pd.to_datetime(fecha_fin) - pd.DateOffset(months=6)).strftime('%Y-%m-%d'),
                'fecha_fin_p2': (pd.to_datetime(fecha_fin) - pd.DateOffset(months=1)).strftime('%Y-%m-%d')
            }

    def get_table_names_radiografia_completa(self):
        return list(self.dict_tablas_radiografia_completa.keys())

    def get_queries_rad_categorias(self, id):
        query_actual_marca_mc_total = f'''
            --TOTAL ---------------------------------------------------------
            --ACTUAL -------------------------------------------------------
            --MARCA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_MC_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_MC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_MC
                ,SUM(VENTA) VENTA_MARCA_MC
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_MC
                ,SUM(UNIDADES) UNIDADES_MARCA_MC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_marca_nmc_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_NMC_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_NMC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_NMC
                ,SUM(VENTA) VENTA_MARCA_NMC
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_NMC
                ,SUM(UNIDADES) UNIDADES_MARCA_NMC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_marca_total_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_TOTAL_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_TOTAL_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA
                ,SUM(VENTA) VENTA_MARCA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA
                ,SUM(UNIDADES) UNIDADES_MARCA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_marca_online_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_ONLINE_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_ONLINE_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_ONLINE
                ,SUM(VENTA) VENTA_MARCA_ONLINE
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_ONLINE
                ,SUM(UNIDADES) UNIDADES_MARCA_ONLINE

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_MARCA_MC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_MARCA_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_MARCA_MC_ONLINE
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_MARCA_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_MARCA_NMC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_MARCA_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_MARCA_NMC_ONLINE

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_mc_total = f'''
            --TOTAL ---------------------------------------------------------
            --ACTUAL -------------------------------------------------------
            --CATEGORIA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_MC_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_MC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_MC
                ,SUM(VENTA) VENTA_CAT_MC
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_MC
                ,SUM(UNIDADES) UNIDADES_CAT_MC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_nmc_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_NMC_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_NMC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_NMC
                ,SUM(VENTA) VENTA_CAT_NMC
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_NMC
                ,SUM(UNIDADES) UNIDADES_CAT_NMC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_total_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_TOTAL_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_TOTAL_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT
                ,SUM(VENTA) VENTA_CAT
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT
                ,SUM(UNIDADES) UNIDADES_CAT

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_online_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_ONLINE_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_ONLINE_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_ONLINE
                ,SUM(VENTA) VENTA_CAT_ONLINE
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_ONLINE
                ,SUM(UNIDADES) UNIDADES_CAT_ONLINE

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_CAT_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_CAT_MC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_CAT_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_CAT_MC_ONLINE
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_CAT_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_CAT_NMC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_CAT_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_CAT_NMC_ONLINE

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_total = f'''
            --JOIN ACTUAL_TOTAL --------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_TOTAL AS (
                SELECT
                    ID_RADIOGRAFIA
                    ,CLASS_DESC
                    ,SUBCLASS_DESC
                    ,PROD_TYPE_DESC
                    ,CLIENTES_MARCA
                    ,VENTA_MARCA
                    ,VENTA_MARCA_MC
                    ,VENTA_MARCA_NMC
                    ,TX_MARCA
                    ,TX_MARCA_MC
                    ,TX_MARCA_NMC
                    ,UNIDADES_MARCA
                    ,UNIDADES_MARCA_MC
                    ,UNIDADES_MARCA_NMC
                    ,VENTA_MARCA_ONLINE
                    ,VENTA_MARCA_MC_ONLINE
                    ,VENTA_MARCA_NMC_ONLINE
                    ,CLIENTES_CAT
                    ,VENTA_CAT
                    ,VENTA_CAT_MC
                    ,VENTA_CAT_NMC
                    ,TX_CAT
                    ,TX_CAT_MC
                    ,TX_CAT_NMC
                    ,UNIDADES_CAT
                    ,UNIDADES_CAT_MC
                    ,UNIDADES_CAT_NMC
                    ,VENTA_CAT_ONLINE
                    ,VENTA_CAT_MC_ONLINE
                    ,VENTA_CAT_NMC_ONLINE
                FROM #MON_RAD_ACTUAL_MARCA_TOTAL_TOTAL A
                FULL JOIN #MON_RAD_ACTUAL_MARCA_MC_TOTAL B USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_MARCA_NMC_TOTAL C USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_MARCA_ONLINE_TOTAL D USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_TOTAL_TOTAL E USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_MC_TOTAL F USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_NMC_TOTAL G USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_ONLINE_TOTAL H USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
            );

            -- SELECT * FROM #MON_RAD_ACTUAL_TOTAL;

            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_TOTAL_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_MC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_NMC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_ONLINE_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_TOTAL_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_MC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_NMC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_ONLINE_TOTAL;
        '''

        query_aa_marca_mc_total = f'''
            --TOTAL ---------------------------------------------------------
            --AA -------------------------------------------------------
            --MARCA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_MC_TOTAL;
            CREATE TABLE #MON_RAD_AA_MARCA_MC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_MC_AA
                ,SUM(VENTA) VENTA_MARCA_MC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_MC_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_MC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_marca_nmc_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_NMC_TOTAL;
            CREATE TABLE #MON_RAD_AA_MARCA_NMC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_NMC_AA
                ,SUM(VENTA) VENTA_MARCA_NMC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_NMC_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_NMC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_marca_total_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_TOTAL_TOTAL;
            CREATE TABLE #MON_RAD_AA_MARCA_TOTAL_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_AA
                ,SUM(VENTA) VENTA_MARCA_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_marca_online_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_ONLINE_TOTAL;
            CREATE TABLE #MON_RAD_AA_MARCA_ONLINE_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_ONLINE_AA
                ,SUM(VENTA) VENTA_MARCA_ONLINE_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_ONLINE_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_ONLINE_AA

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_MARCA_MC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_MARCA_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_MARCA_MC_ONLINE_AA
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_MARCA_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_MARCA_NMC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_MARCA_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_MARCA_NMC_ONLINE_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_mc_total = f'''
            --TOTAL ---------------------------------------------------------
            --AA -------------------------------------------------------
            --CATEGORIA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_MC_TOTAL;
            CREATE TABLE #MON_RAD_AA_CAT_MC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_MC_AA
                ,SUM(VENTA) VENTA_CAT_MC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_MC_AA
                ,SUM(UNIDADES) UNIDADES_CAT_MC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_nmc_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_NMC_TOTAL;
            CREATE TABLE #MON_RAD_AA_CAT_NMC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_NMC_AA
                ,SUM(VENTA) VENTA_CAT_NMC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_NMC_AA
                ,SUM(UNIDADES) UNIDADES_CAT_NMC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_total_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_TOTAL_TOTAL;
            CREATE TABLE #MON_RAD_AA_CAT_TOTAL_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_AA
                ,SUM(VENTA) VENTA_CAT_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_AA
                ,SUM(UNIDADES) UNIDADES_CAT_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_online_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_ONLINE_TOTAL;
            CREATE TABLE #MON_RAD_AA_CAT_ONLINE_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_ONLINE_AA
                ,SUM(VENTA) VENTA_CAT_ONLINE_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_ONLINE_AA
                ,SUM(UNIDADES) UNIDADES_CAT_ONLINE_AA

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_CAT_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_CAT_MC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_CAT_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_CAT_MC_ONLINE_AA
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_CAT_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_CAT_NMC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_CAT_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_CAT_NMC_ONLINE_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_total = f'''
            --JOIN AA_TOTAL --------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_TOTAL;
            CREATE TABLE #MON_RAD_AA_TOTAL AS (
                SELECT
                    ID_RADIOGRAFIA
                    ,CLASS_DESC
                    ,SUBCLASS_DESC
                    ,PROD_TYPE_DESC
                    ,CLIENTES_MARCA_AA
                    ,VENTA_MARCA_AA
                    ,VENTA_MARCA_MC_AA
                    ,VENTA_MARCA_NMC_AA
                    ,TX_MARCA_AA
                    ,TX_MARCA_MC_AA
                    ,TX_MARCA_NMC_AA
                    ,UNIDADES_MARCA_AA
                    ,UNIDADES_MARCA_MC_AA
                    ,UNIDADES_MARCA_NMC_AA
                    ,VENTA_MARCA_ONLINE_AA
                    ,VENTA_MARCA_MC_ONLINE_AA
                    ,VENTA_MARCA_NMC_ONLINE_AA
                    ,CLIENTES_CAT_AA
                    ,VENTA_CAT_AA
                    ,VENTA_CAT_MC_AA
                    ,VENTA_CAT_NMC_AA
                    ,TX_CAT_AA
                    ,TX_CAT_MC_AA
                    ,TX_CAT_NMC_AA
                    ,UNIDADES_CAT_AA
                    ,UNIDADES_CAT_MC_AA
                    ,UNIDADES_CAT_NMC_AA
                    ,VENTA_CAT_ONLINE_AA
                    ,VENTA_CAT_MC_ONLINE_AA
                    ,VENTA_CAT_NMC_ONLINE_AA
                FROM #MON_RAD_AA_MARCA_TOTAL_TOTAL A
                FULL JOIN #MON_RAD_AA_MARCA_MC_TOTAL B USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_MARCA_NMC_TOTAL C USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_MARCA_ONLINE_TOTAL D USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_TOTAL_TOTAL E USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_MC_TOTAL F USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_NMC_TOTAL G USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_ONLINE_TOTAL H USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
            );

            -- SELECT * FROM #MON_RAD_AA_TOTAL;

            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_TOTAL_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_MC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_NMC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_ONLINE_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_TOTAL_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_MC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_NMC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_ONLINE_TOTAL;
        '''

        query_total = f'''
            --JOIN TOTAL----------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_CAT_TOTAL;
            CREATE TABLE #MON_RAD_CAT_TOTAL AS (
                SELECT
                    *
                FROM #MON_RAD_ACTUAL_TOTAL
                LEFT JOIN #MON_RAD_AA_TOTAL USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
            );

            --SELECT * FROM #MON_RAD_CAT_TOTAL;

            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_TOTAL;

            --GUARDAR TOTAL EN DB----------------------------------------------------------
            DELETE CHEDRAUI.MON_RAD_CAT WHERE ID_RADIOGRAFIA = '{id}' AND CLASS_DESC = 'TOTAL' AND SUBCLASS_DESC = 'TOTAL' AND PROD_TYPE_DESC = 'TOTAL';
            INSERT INTO CHEDRAUI.MON_RAD_CAT SELECT * FROM #MON_RAD_CAT_TOTAL;        
        '''

        query_actual_marca_mc_class = f'''
            --CLASS ---------------------------------------------------------
            --ACTUAL -------------------------------------------------------
            --MARCA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_MC_CLASS;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_MC_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_MC
                ,SUM(VENTA) VENTA_MARCA_MC
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_MC
                ,SUM(UNIDADES) UNIDADES_MARCA_MC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_marca_nmc_class = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_NMC_CLASS;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_NMC_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_NMC
                ,SUM(VENTA) VENTA_MARCA_NMC
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_NMC
                ,SUM(UNIDADES) UNIDADES_MARCA_NMC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_marca_total_class = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_TOTAL_CLASS;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_TOTAL_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA
                ,SUM(VENTA) VENTA_MARCA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA
                ,SUM(UNIDADES) UNIDADES_MARCA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_marca_online_class = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_ONLINE_CLASS;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_ONLINE_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_ONLINE
                ,SUM(VENTA) VENTA_MARCA_ONLINE
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_ONLINE
                ,SUM(UNIDADES) UNIDADES_MARCA_ONLINE

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_MARCA_MC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_MARCA_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_MARCA_MC_ONLINE
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_MARCA_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_MARCA_NMC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_MARCA_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_MARCA_NMC_ONLINE

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_mc_class = f'''
            --CLASS ---------------------------------------------------------
            --ACTUAL -------------------------------------------------------
            --CATEGORIA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_MC_CLASS;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_MC_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_MC
                ,SUM(VENTA) VENTA_CAT_MC
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_MC
                ,SUM(UNIDADES) UNIDADES_CAT_MC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_nmc_class = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_NMC_CLASS;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_NMC_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_NMC
                ,SUM(VENTA) VENTA_CAT_NMC
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_NMC
                ,SUM(UNIDADES) UNIDADES_CAT_NMC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_total_class = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_TOTAL_CLASS;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_TOTAL_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT
                ,SUM(VENTA) VENTA_CAT
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT
                ,SUM(UNIDADES) UNIDADES_CAT

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_online_class = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_ONLINE_CLASS;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_ONLINE_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_ONLINE
                ,SUM(VENTA) VENTA_CAT_ONLINE
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_ONLINE
                ,SUM(UNIDADES) UNIDADES_CAT_ONLINE

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_CAT_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_CAT_MC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_CAT_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_CAT_MC_ONLINE
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_CAT_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_CAT_NMC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_CAT_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_CAT_NMC_ONLINE

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_class = f'''
            --JOIN ACTUAL_CLASS --------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CLASS;
            CREATE TABLE #MON_RAD_ACTUAL_CLASS AS (
                SELECT
                    ID_RADIOGRAFIA
                    ,CLASS_DESC
                    ,SUBCLASS_DESC
                    ,PROD_TYPE_DESC
                    ,CLIENTES_MARCA
                    ,VENTA_MARCA
                    ,VENTA_MARCA_MC
                    ,VENTA_MARCA_NMC
                    ,TX_MARCA
                    ,TX_MARCA_MC
                    ,TX_MARCA_NMC
                    ,UNIDADES_MARCA
                    ,UNIDADES_MARCA_MC
                    ,UNIDADES_MARCA_NMC
                    ,VENTA_MARCA_ONLINE
                    ,VENTA_MARCA_MC_ONLINE
                    ,VENTA_MARCA_NMC_ONLINE
                    ,CLIENTES_CAT
                    ,VENTA_CAT
                    ,VENTA_CAT_MC
                    ,VENTA_CAT_NMC
                    ,TX_CAT
                    ,TX_CAT_MC
                    ,TX_CAT_NMC
                    ,UNIDADES_CAT
                    ,UNIDADES_CAT_MC
                    ,UNIDADES_CAT_NMC
                    ,VENTA_CAT_ONLINE
                    ,VENTA_CAT_MC_ONLINE
                    ,VENTA_CAT_NMC_ONLINE
                FROM #MON_RAD_ACTUAL_MARCA_TOTAL_CLASS A
                FULL JOIN #MON_RAD_ACTUAL_MARCA_MC_CLASS B USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_MARCA_NMC_CLASS C USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_MARCA_ONLINE_CLASS D USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_TOTAL_CLASS E USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_MC_CLASS F USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_NMC_CLASS G USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_ONLINE_CLASS H USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
            );

            -- SELECT * FROM #MON_RAD_ACTUAL_CLASS;

            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_TOTAL_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_MC_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_NMC_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_ONLINE_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_TOTAL_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_MC_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_NMC_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_ONLINE_CLASS;
        '''

        query_aa_marca_mc_class = f'''
            --CLASS ---------------------------------------------------------
            --AA -------------------------------------------------------
            --MARCA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_MC_CLASS;
            CREATE TABLE #MON_RAD_AA_MARCA_MC_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_MC_AA
                ,SUM(VENTA) VENTA_MARCA_MC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_MC_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_MC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_marca_nmc_class = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_NMC_CLASS;
            CREATE TABLE #MON_RAD_AA_MARCA_NMC_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_NMC_AA
                ,SUM(VENTA) VENTA_MARCA_NMC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_NMC_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_NMC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_marca_total_class = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_TOTAL_CLASS;
            CREATE TABLE #MON_RAD_AA_MARCA_TOTAL_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_AA
                ,SUM(VENTA) VENTA_MARCA_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_marca_online_class = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_ONLINE_CLASS;
            CREATE TABLE #MON_RAD_AA_MARCA_ONLINE_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_ONLINE_AA
                ,SUM(VENTA) VENTA_MARCA_ONLINE_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_ONLINE_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_ONLINE_AA

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_MARCA_MC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_MARCA_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_MARCA_MC_ONLINE_AA
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_MARCA_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_MARCA_NMC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_MARCA_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_MARCA_NMC_ONLINE_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_mc_class = f'''
            --CLASS ---------------------------------------------------------
            --AA -------------------------------------------------------
            --CATEGORIA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_MC_CLASS;
            CREATE TABLE #MON_RAD_AA_CAT_MC_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_MC_AA
                ,SUM(VENTA) VENTA_CAT_MC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_MC_AA
                ,SUM(UNIDADES) UNIDADES_CAT_MC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_nmc_class = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_NMC_CLASS;
            CREATE TABLE #MON_RAD_AA_CAT_NMC_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_NMC_AA
                ,SUM(VENTA) VENTA_CAT_NMC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_NMC_AA
                ,SUM(UNIDADES) UNIDADES_CAT_NMC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_total_class = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_TOTAL_CLASS;
            CREATE TABLE #MON_RAD_AA_CAT_TOTAL_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_AA
                ,SUM(VENTA) VENTA_CAT_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_AA
                ,SUM(UNIDADES) UNIDADES_CAT_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_online_class = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_ONLINE_CLASS;
            CREATE TABLE #MON_RAD_AA_CAT_ONLINE_CLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_ONLINE_AA
                ,SUM(VENTA) VENTA_CAT_ONLINE_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_ONLINE_AA
                ,SUM(UNIDADES) UNIDADES_CAT_ONLINE_AA

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_CAT_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_CAT_MC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_CAT_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_CAT_MC_ONLINE_AA
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_CAT_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_CAT_NMC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_CAT_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_CAT_NMC_ONLINE_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''
        
        query_aa_class = f'''
            --JOIN AA_CLASS --------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_CLASS;
            CREATE TABLE #MON_RAD_AA_CLASS AS (
                SELECT
                    ID_RADIOGRAFIA
                    ,CLASS_DESC
                    ,SUBCLASS_DESC
                    ,PROD_TYPE_DESC
                    ,CLIENTES_MARCA_AA
                    ,VENTA_MARCA_AA
                    ,VENTA_MARCA_MC_AA
                    ,VENTA_MARCA_NMC_AA
                    ,TX_MARCA_AA
                    ,TX_MARCA_MC_AA
                    ,TX_MARCA_NMC_AA
                    ,UNIDADES_MARCA_AA
                    ,UNIDADES_MARCA_MC_AA
                    ,UNIDADES_MARCA_NMC_AA
                    ,VENTA_MARCA_ONLINE_AA
                    ,VENTA_MARCA_MC_ONLINE_AA
                    ,VENTA_MARCA_NMC_ONLINE_AA
                    ,CLIENTES_CAT_AA
                    ,VENTA_CAT_AA
                    ,VENTA_CAT_MC_AA
                    ,VENTA_CAT_NMC_AA
                    ,TX_CAT_AA
                    ,TX_CAT_MC_AA
                    ,TX_CAT_NMC_AA
                    ,UNIDADES_CAT_AA
                    ,UNIDADES_CAT_MC_AA
                    ,UNIDADES_CAT_NMC_AA
                    ,VENTA_CAT_ONLINE_AA
                    ,VENTA_CAT_MC_ONLINE_AA
                    ,VENTA_CAT_NMC_ONLINE_AA
                FROM #MON_RAD_AA_MARCA_TOTAL_CLASS A
                FULL JOIN #MON_RAD_AA_MARCA_MC_CLASS B USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_MARCA_NMC_CLASS C USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_MARCA_ONLINE_CLASS D USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_TOTAL_CLASS E USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_MC_CLASS F USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_NMC_CLASS G USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_ONLINE_CLASS H USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
            );

            -- SELECT * FROM #MON_RAD_AA_CLASS;

            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_TOTAL_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_MC_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_NMC_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_ONLINE_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_TOTAL_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_MC_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_NMC_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_ONLINE_CLASS;
        '''

        query_class = f'''
            --JOIN CLASS----------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_CAT_CLASS;
            CREATE TABLE #MON_RAD_CAT_CLASS AS (
                SELECT
                    *
                FROM #MON_RAD_ACTUAL_CLASS
                LEFT JOIN #MON_RAD_AA_CLASS USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
            );

            --SELECT * FROM #MON_RAD_CAT_CLASS;

            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_CLASS;

            --GUARDAR TOTAL EN DB----------------------------------------------------------
            DELETE CHEDRAUI.MON_RAD_CAT WHERE ID_RADIOGRAFIA = '{id}' AND CLASS_DESC <> 'TOTAL' AND SUBCLASS_DESC = 'TOTAL' AND PROD_TYPE_DESC = 'TOTAL';
            INSERT INTO CHEDRAUI.MON_RAD_CAT SELECT * FROM #MON_RAD_CAT_CLASS;
        '''

        query_actual_marca_mc_subclass = f'''
            --SUBCLASS ---------------------------------------------------------
            --ACTUAL -------------------------------------------------------
            --MARCA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_MC_SUBCLASS;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_MC_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_MC
                ,SUM(VENTA) VENTA_MARCA_MC
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_MC
                ,SUM(UNIDADES) UNIDADES_MARCA_MC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_marca_nmc_subclass = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_NMC_SUBCLASS;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_NMC_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_NMC
                ,SUM(VENTA) VENTA_MARCA_NMC
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_NMC
                ,SUM(UNIDADES) UNIDADES_MARCA_NMC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_marca_total_subclass = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_TOTAL_SUBCLASS;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_TOTAL_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA
                ,SUM(VENTA) VENTA_MARCA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA
                ,SUM(UNIDADES) UNIDADES_MARCA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_marca_online_subclass = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_ONLINE_SUBCLASS;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_ONLINE_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_ONLINE
                ,SUM(VENTA) VENTA_MARCA_ONLINE
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_ONLINE
                ,SUM(UNIDADES) UNIDADES_MARCA_ONLINE

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_MARCA_MC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_MARCA_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_MARCA_MC_ONLINE
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_MARCA_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_MARCA_NMC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_MARCA_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_MARCA_NMC_ONLINE

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_mc_subclass = f'''
            --SUBCLASS ---------------------------------------------------------
            --ACTUAL -------------------------------------------------------
            --CATEGORIA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_MC_SUBCLASS;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_MC_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_MC
                ,SUM(VENTA) VENTA_CAT_MC
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_MC
                ,SUM(UNIDADES) UNIDADES_CAT_MC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_nmc_subclass = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_NMC_SUBCLASS;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_NMC_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_NMC
                ,SUM(VENTA) VENTA_CAT_NMC
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_NMC
                ,SUM(UNIDADES) UNIDADES_CAT_NMC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_total_subclass = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_TOTAL_SUBCLASS;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_TOTAL_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT
                ,SUM(VENTA) VENTA_CAT
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT
                ,SUM(UNIDADES) UNIDADES_CAT

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_online_subclass = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_ONLINE_SUBCLASS;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_ONLINE_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_ONLINE
                ,SUM(VENTA) VENTA_CAT_ONLINE
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_ONLINE
                ,SUM(UNIDADES) UNIDADES_CAT_ONLINE

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_CAT_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_CAT_MC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_CAT_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_CAT_MC_ONLINE
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_CAT_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_CAT_NMC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_CAT_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_CAT_NMC_ONLINE

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_subclass = f'''
            --JOIN ACTUAL_SUBCLASS --------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_SUBCLASS;
            CREATE TABLE #MON_RAD_ACTUAL_SUBCLASS AS (
                SELECT
                    ID_RADIOGRAFIA
                    ,CLASS_DESC
                    ,SUBCLASS_DESC
                    ,PROD_TYPE_DESC
                    ,CLIENTES_MARCA
                    ,VENTA_MARCA
                    ,VENTA_MARCA_MC
                    ,VENTA_MARCA_NMC
                    ,TX_MARCA
                    ,TX_MARCA_MC
                    ,TX_MARCA_NMC
                    ,UNIDADES_MARCA
                    ,UNIDADES_MARCA_MC
                    ,UNIDADES_MARCA_NMC
                    ,VENTA_MARCA_ONLINE
                    ,VENTA_MARCA_MC_ONLINE
                    ,VENTA_MARCA_NMC_ONLINE
                    ,CLIENTES_CAT
                    ,VENTA_CAT
                    ,VENTA_CAT_MC
                    ,VENTA_CAT_NMC
                    ,TX_CAT
                    ,TX_CAT_MC
                    ,TX_CAT_NMC
                    ,UNIDADES_CAT
                    ,UNIDADES_CAT_MC
                    ,UNIDADES_CAT_NMC
                    ,VENTA_CAT_ONLINE
                    ,VENTA_CAT_MC_ONLINE
                    ,VENTA_CAT_NMC_ONLINE
                FROM #MON_RAD_ACTUAL_MARCA_TOTAL_SUBCLASS A
                FULL JOIN #MON_RAD_ACTUAL_MARCA_MC_SUBCLASS B USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_MARCA_NMC_SUBCLASS C USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_MARCA_ONLINE_SUBCLASS D USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_TOTAL_SUBCLASS E USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_MC_SUBCLASS F USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_NMC_SUBCLASS G USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_ONLINE_SUBCLASS H USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
            );

            -- SELECT * FROM #MON_RAD_ACTUAL_SUBCLASS;

            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_TOTAL_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_MC_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_NMC_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_ONLINE_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_TOTAL_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_MC_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_NMC_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_ONLINE_SUBCLASS;
        '''

        query_aa_marca_mc_subclass = f'''
            --SUBCLASS ---------------------------------------------------------
            --AA -------------------------------------------------------
            --MARCA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_MC_SUBCLASS;
            CREATE TABLE #MON_RAD_AA_MARCA_MC_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_MC_AA
                ,SUM(VENTA) VENTA_MARCA_MC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_MC_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_MC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_marca_nmc_subclass = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_NMC_SUBCLASS;
            CREATE TABLE #MON_RAD_AA_MARCA_NMC_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_NMC_AA
                ,SUM(VENTA) VENTA_MARCA_NMC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_NMC_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_NMC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_marca_total_subclass = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_TOTAL_SUBCLASS;
            CREATE TABLE #MON_RAD_AA_MARCA_TOTAL_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_AA
                ,SUM(VENTA) VENTA_MARCA_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_marca_online_subclass = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_ONLINE_SUBCLASS;
            CREATE TABLE #MON_RAD_AA_MARCA_ONLINE_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_ONLINE_AA
                ,SUM(VENTA) VENTA_MARCA_ONLINE_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_ONLINE_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_ONLINE_AA

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_MARCA_MC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_MARCA_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_MARCA_MC_ONLINE_AA
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_MARCA_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_MARCA_NMC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_MARCA_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_MARCA_NMC_ONLINE_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_mc_subclass = f'''
            --SUBCLASS ---------------------------------------------------------
            --AA -------------------------------------------------------
            --CATEGORIA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_MC_SUBCLASS;
            CREATE TABLE #MON_RAD_AA_CAT_MC_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_MC_AA
                ,SUM(VENTA) VENTA_CAT_MC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_MC_AA
                ,SUM(UNIDADES) UNIDADES_CAT_MC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_nmc_subclass = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_NMC_SUBCLASS;
            CREATE TABLE #MON_RAD_AA_CAT_NMC_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_NMC_AA
                ,SUM(VENTA) VENTA_CAT_NMC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_NMC_AA
                ,SUM(UNIDADES) UNIDADES_CAT_NMC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_total_subclass = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_TOTAL_SUBCLASS;
            CREATE TABLE #MON_RAD_AA_CAT_TOTAL_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_AA
                ,SUM(VENTA) VENTA_CAT_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_AA
                ,SUM(UNIDADES) UNIDADES_CAT_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_online_subclass = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_ONLINE_SUBCLASS;
            CREATE TABLE #MON_RAD_AA_CAT_ONLINE_SUBCLASS AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_ONLINE_AA
                ,SUM(VENTA) VENTA_CAT_ONLINE_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_ONLINE_AA
                ,SUM(UNIDADES) UNIDADES_CAT_ONLINE_AA

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_CAT_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_CAT_MC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_CAT_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_CAT_MC_ONLINE_AA
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_CAT_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_CAT_NMC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_CAT_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_CAT_NMC_ONLINE_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_subclass = f'''
            --JOIN AA_SUBCLASS --------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_SUBCLASS;
            CREATE TABLE #MON_RAD_AA_SUBCLASS AS (
                SELECT
                    ID_RADIOGRAFIA
                    ,CLASS_DESC
                    ,SUBCLASS_DESC
                    ,PROD_TYPE_DESC
                    ,CLIENTES_MARCA_AA
                    ,VENTA_MARCA_AA
                    ,VENTA_MARCA_MC_AA
                    ,VENTA_MARCA_NMC_AA
                    ,TX_MARCA_AA
                    ,TX_MARCA_MC_AA
                    ,TX_MARCA_NMC_AA
                    ,UNIDADES_MARCA_AA
                    ,UNIDADES_MARCA_MC_AA
                    ,UNIDADES_MARCA_NMC_AA
                    ,VENTA_MARCA_ONLINE_AA
                    ,VENTA_MARCA_MC_ONLINE_AA
                    ,VENTA_MARCA_NMC_ONLINE_AA
                    ,CLIENTES_CAT_AA
                    ,VENTA_CAT_AA
                    ,VENTA_CAT_MC_AA
                    ,VENTA_CAT_NMC_AA
                    ,TX_CAT_AA
                    ,TX_CAT_MC_AA
                    ,TX_CAT_NMC_AA
                    ,UNIDADES_CAT_AA
                    ,UNIDADES_CAT_MC_AA
                    ,UNIDADES_CAT_NMC_AA
                    ,VENTA_CAT_ONLINE_AA
                    ,VENTA_CAT_MC_ONLINE_AA
                    ,VENTA_CAT_NMC_ONLINE_AA
                FROM #MON_RAD_AA_MARCA_TOTAL_SUBCLASS A
                FULL JOIN #MON_RAD_AA_MARCA_MC_SUBCLASS B USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_MARCA_NMC_SUBCLASS C USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_MARCA_ONLINE_SUBCLASS D USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_TOTAL_SUBCLASS E USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_MC_SUBCLASS F USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_NMC_SUBCLASS G USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_ONLINE_SUBCLASS H USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
            );

            -- SELECT * FROM #MON_RAD_AA_SUBCLASS;

            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_TOTAL_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_MC_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_NMC_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_ONLINE_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_TOTAL_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_MC_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_NMC_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_ONLINE_SUBCLASS;
        '''

        query_subclass = f'''
            --JOIN SUBCLASS----------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_CAT_SUBCLASS;
            CREATE TABLE #MON_RAD_CAT_SUBCLASS AS (
                SELECT
                    *
                FROM #MON_RAD_ACTUAL_SUBCLASS
                LEFT JOIN #MON_RAD_AA_SUBCLASS USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
            );

            --SELECT * FROM #MON_RAD_CAT_SUBCLASS;

            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_SUBCLASS;
            DROP TABLE IF EXISTS #MON_RAD_AA_SUBCLASS;

            --GUARDAR SUBCLASS EN DB----------------------------------------------------------
            DELETE CHEDRAUI.MON_RAD_CAT WHERE ID_RADIOGRAFIA = '{id}' AND CLASS_DESC <> 'TOTAL' AND SUBCLASS_DESC <> 'TOTAL' AND PROD_TYPE_DESC = 'TOTAL';
            INSERT INTO CHEDRAUI.MON_RAD_CAT SELECT * FROM #MON_RAD_CAT_SUBCLASS;
        '''

        query_actual_marca_mc_prodtype = f'''
            --PRODTYPE ---------------------------------------------------------
            --ACTUAL -------------------------------------------------------
            --MARCA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_MC_PRODTYPE;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_MC_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_MC
                ,SUM(VENTA) VENTA_MARCA_MC
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_MC
                ,SUM(UNIDADES) UNIDADES_MARCA_MC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_marca_nmc_prodtype = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_NMC_PRODTYPE;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_NMC_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_NMC
                ,SUM(VENTA) VENTA_MARCA_NMC
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_NMC
                ,SUM(UNIDADES) UNIDADES_MARCA_NMC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_marca_total_prodtype = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_TOTAL_PRODTYPE;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_TOTAL_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA
                ,SUM(VENTA) VENTA_MARCA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA
                ,SUM(UNIDADES) UNIDADES_MARCA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_marca_online_prodtype = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_ONLINE_PRODTYPE;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_ONLINE_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_ONLINE
                ,SUM(VENTA) VENTA_MARCA_ONLINE
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_ONLINE
                ,SUM(UNIDADES) UNIDADES_MARCA_ONLINE

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_MARCA_MC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_MARCA_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_MARCA_MC_ONLINE
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_MARCA_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_MARCA_NMC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_MARCA_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_MARCA_NMC_ONLINE

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_mc_prodtype = f'''
            --PRODTYPE ---------------------------------------------------------
            --ACTUAL -------------------------------------------------------
            --CATEGORIA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_MC_PRODTYPE;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_MC_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_MC
                ,SUM(VENTA) VENTA_CAT_MC
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_MC
                ,SUM(UNIDADES) UNIDADES_CAT_MC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_nmc_prodtype = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_NMC_PRODTYPE;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_NMC_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_NMC
                ,SUM(VENTA) VENTA_CAT_NMC
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_NMC
                ,SUM(UNIDADES) UNIDADES_CAT_NMC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_total_prodtype = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_TOTAL_PRODTYPE;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_TOTAL_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT
                ,SUM(VENTA) VENTA_CAT
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT
                ,SUM(UNIDADES) UNIDADES_CAT

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_cat_online_prodtype = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_ONLINE_PRODTYPE;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_ONLINE_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_ONLINE
                ,SUM(VENTA) VENTA_CAT_ONLINE
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_ONLINE
                ,SUM(UNIDADES) UNIDADES_CAT_ONLINE

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_CAT_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_CAT_MC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_CAT_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_CAT_MC_ONLINE
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_CAT_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_CAT_NMC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_CAT_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_CAT_NMC_ONLINE

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_actual_prodtype = f'''
            --JOIN ACTUAL_PRODTYPE --------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_PRODTYPE;
            CREATE TABLE #MON_RAD_ACTUAL_PRODTYPE AS (
                SELECT
                    ID_RADIOGRAFIA
                    ,CLASS_DESC
                    ,SUBCLASS_DESC
                    ,PROD_TYPE_DESC
                    ,CLIENTES_MARCA
                    ,VENTA_MARCA
                    ,VENTA_MARCA_MC
                    ,VENTA_MARCA_NMC
                    ,TX_MARCA
                    ,TX_MARCA_MC
                    ,TX_MARCA_NMC
                    ,UNIDADES_MARCA
                    ,UNIDADES_MARCA_MC
                    ,UNIDADES_MARCA_NMC
                    ,VENTA_MARCA_ONLINE
                    ,VENTA_MARCA_MC_ONLINE
                    ,VENTA_MARCA_NMC_ONLINE
                    ,CLIENTES_CAT
                    ,VENTA_CAT
                    ,VENTA_CAT_MC
                    ,VENTA_CAT_NMC
                    ,TX_CAT
                    ,TX_CAT_MC
                    ,TX_CAT_NMC
                    ,UNIDADES_CAT
                    ,UNIDADES_CAT_MC
                    ,UNIDADES_CAT_NMC
                    ,VENTA_CAT_ONLINE
                    ,VENTA_CAT_MC_ONLINE
                    ,VENTA_CAT_NMC_ONLINE
                FROM #MON_RAD_ACTUAL_MARCA_TOTAL_PRODTYPE A
                FULL JOIN #MON_RAD_ACTUAL_MARCA_MC_PRODTYPE B USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_MARCA_NMC_PRODTYPE C USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_MARCA_ONLINE_PRODTYPE D USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_TOTAL_PRODTYPE E USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_MC_PRODTYPE F USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_NMC_PRODTYPE G USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_ACTUAL_CAT_ONLINE_PRODTYPE H USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
            );

            -- SELECT * FROM #MON_RAD_ACTUAL_PRODTYPE;

            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_TOTAL_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_MC_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_NMC_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_ONLINE_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_TOTAL_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_MC_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_NMC_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_ONLINE_PRODTYPE;
        '''

        query_aa_marca_mc_prodtype = f'''
            --PRODTYPE ---------------------------------------------------------
            --AA -------------------------------------------------------
            --MARCA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_MC_PRODTYPE;
            CREATE TABLE #MON_RAD_AA_MARCA_MC_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_MC_AA
                ,SUM(VENTA) VENTA_MARCA_MC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_MC_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_MC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_marca_nmc_prodtype = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_NMC_PRODTYPE;
            CREATE TABLE #MON_RAD_AA_MARCA_NMC_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_NMC_AA
                ,SUM(VENTA) VENTA_MARCA_NMC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_NMC_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_NMC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_marca_total_prodtype = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_TOTAL_PRODTYPE;
            CREATE TABLE #MON_RAD_AA_MARCA_TOTAL_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_AA
                ,SUM(VENTA) VENTA_MARCA_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_marca_online_prodtype = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_ONLINE_PRODTYPE;
            CREATE TABLE #MON_RAD_AA_MARCA_ONLINE_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_ONLINE_AA
                ,SUM(VENTA) VENTA_MARCA_ONLINE_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_ONLINE_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_ONLINE_AA

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_MARCA_MC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_MARCA_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_MARCA_MC_ONLINE_AA
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_MARCA_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_MARCA_NMC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_MARCA_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_MARCA_NMC_ONLINE_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_mc_prodtype = f'''
            --PRODTYPE ---------------------------------------------------------
            --AA -------------------------------------------------------
            --CATEGORIA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_MC_PRODTYPE;
            CREATE TABLE #MON_RAD_AA_CAT_MC_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_MC_AA
                ,SUM(VENTA) VENTA_CAT_MC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_MC_AA
                ,SUM(UNIDADES) UNIDADES_CAT_MC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_nmc_prodtype = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_NMC_PRODTYPE;
            CREATE TABLE #MON_RAD_AA_CAT_NMC_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_NMC_AA
                ,SUM(VENTA) VENTA_CAT_NMC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_NMC_AA
                ,SUM(UNIDADES) UNIDADES_CAT_NMC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_total_prodtype = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_TOTAL_PRODTYPE;
            CREATE TABLE #MON_RAD_AA_CAT_TOTAL_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_AA
                ,SUM(VENTA) VENTA_CAT_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_AA
                ,SUM(UNIDADES) UNIDADES_CAT_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_cat_online_prodtype = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_ONLINE_PRODTYPE;
            CREATE TABLE #MON_RAD_AA_CAT_ONLINE_PRODTYPE AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_ONLINE_AA
                ,SUM(VENTA) VENTA_CAT_ONLINE_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_ONLINE_AA
                ,SUM(UNIDADES) UNIDADES_CAT_ONLINE_AA

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_CAT_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_CAT_MC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_CAT_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_CAT_MC_ONLINE_AA
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_CAT_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_CAT_NMC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_CAT_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_CAT_NMC_ONLINE_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            AND IND_DUPLICADO = 0
            GROUP BY 1,2,3,4
            );
        '''

        query_aa_prodtype = f'''
            --JOIN AA_PRODTYPE --------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_PRODTYPE;
            CREATE TABLE #MON_RAD_AA_PRODTYPE AS (
                SELECT
                    ID_RADIOGRAFIA
                    ,CLASS_DESC
                    ,SUBCLASS_DESC
                    ,PROD_TYPE_DESC
                    ,CLIENTES_MARCA_AA
                    ,VENTA_MARCA_AA
                    ,VENTA_MARCA_MC_AA
                    ,VENTA_MARCA_NMC_AA
                    ,TX_MARCA_AA
                    ,TX_MARCA_MC_AA
                    ,TX_MARCA_NMC_AA
                    ,UNIDADES_MARCA_AA
                    ,UNIDADES_MARCA_MC_AA
                    ,UNIDADES_MARCA_NMC_AA
                    ,VENTA_MARCA_ONLINE_AA
                    ,VENTA_MARCA_MC_ONLINE_AA
                    ,VENTA_MARCA_NMC_ONLINE_AA
                    ,CLIENTES_CAT_AA
                    ,VENTA_CAT_AA
                    ,VENTA_CAT_MC_AA
                    ,VENTA_CAT_NMC_AA
                    ,TX_CAT_AA
                    ,TX_CAT_MC_AA
                    ,TX_CAT_NMC_AA
                    ,UNIDADES_CAT_AA
                    ,UNIDADES_CAT_MC_AA
                    ,UNIDADES_CAT_NMC_AA
                    ,VENTA_CAT_ONLINE_AA
                    ,VENTA_CAT_MC_ONLINE_AA
                    ,VENTA_CAT_NMC_ONLINE_AA
                FROM #MON_RAD_AA_MARCA_TOTAL_PRODTYPE A
                FULL JOIN #MON_RAD_AA_MARCA_MC_PRODTYPE B USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_MARCA_NMC_PRODTYPE C USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_MARCA_ONLINE_PRODTYPE D USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_TOTAL_PRODTYPE E USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_MC_PRODTYPE F USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_NMC_PRODTYPE G USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
                FULL JOIN #MON_RAD_AA_CAT_ONLINE_PRODTYPE H USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
            );

            -- SELECT * FROM #MON_RAD_AA_PRODTYPE;

            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_TOTAL_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_MC_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_NMC_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_ONLINE_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_TOTAL_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_MC_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_NMC_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_ONLINE_PRODTYPE;
        '''

        query_prodtype = f'''
            --JOIN PRODTYPE----------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_CAT_PRODTYPE;
            CREATE TABLE #MON_RAD_CAT_PRODTYPE AS (
                SELECT
                    *
                FROM #MON_RAD_ACTUAL_PRODTYPE
                LEFT JOIN #MON_RAD_AA_PRODTYPE USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC)
            );

            --SELECT * FROM #MON_RAD_CAT_PRODTYPE;

            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_PRODTYPE;
            DROP TABLE IF EXISTS #MON_RAD_AA_PRODTYPE;

            --GUARDAR PRODTYPE EN DB----------------------------------------------------------
            DELETE CHEDRAUI.MON_RAD_CAT WHERE ID_RADIOGRAFIA = '{id}' AND CLASS_DESC <> 'TOTAL' AND SUBCLASS_DESC <> 'TOTAL' AND PROD_TYPE_DESC <> 'TOTAL';
            INSERT INTO CHEDRAUI.MON_RAD_CAT SELECT * FROM #MON_RAD_CAT_PRODTYPE;
        '''

        lis_query_actual_total = [query_actual_marca_mc_total, query_actual_marca_nmc_total, query_actual_marca_total_total, query_actual_marca_online_total, query_actual_cat_mc_total, query_actual_cat_nmc_total, query_actual_cat_total_total, query_actual_cat_online_total, query_actual_total]
        lis_query_aa_total = [query_aa_marca_mc_total, query_aa_marca_nmc_total, query_aa_marca_total_total, query_aa_marca_online_total, query_aa_cat_mc_total, query_aa_cat_nmc_total, query_aa_cat_total_total, query_aa_cat_online_total, query_aa_total]
        lis_query_total = lis_query_actual_total + lis_query_aa_total + [query_total]

        lis_query_actual_class = [query_actual_marca_mc_class, query_actual_marca_nmc_class, query_actual_marca_total_class, query_actual_marca_online_class, query_actual_cat_mc_class, query_actual_cat_nmc_class, query_actual_cat_total_class, query_actual_cat_online_class, query_actual_class]
        lis_query_aa_class = [query_aa_marca_mc_class, query_aa_marca_nmc_class, query_aa_marca_total_class, query_aa_marca_online_class, query_aa_cat_mc_class, query_aa_cat_nmc_class, query_aa_cat_total_class, query_aa_cat_online_class, query_aa_class]
        lis_query_class = lis_query_actual_class + lis_query_aa_class + [query_class]

        lis_query_actual_subclass = [query_actual_marca_mc_subclass, query_actual_marca_nmc_subclass, query_actual_marca_total_subclass, query_actual_marca_online_subclass, query_actual_cat_mc_subclass, query_actual_cat_nmc_subclass, query_actual_cat_total_subclass, query_actual_cat_online_subclass, query_actual_subclass]
        lis_query_aa_subclass = [query_aa_marca_mc_subclass, query_aa_marca_nmc_subclass, query_aa_marca_total_subclass, query_aa_marca_online_subclass, query_aa_cat_mc_subclass, query_aa_cat_nmc_subclass, query_aa_cat_total_subclass, query_aa_cat_online_subclass, query_aa_subclass]
        lis_query_subclass = lis_query_actual_subclass + lis_query_aa_subclass + [query_subclass]

        lis_query_actual_prodtype = [query_actual_marca_mc_prodtype, query_actual_marca_nmc_prodtype, query_actual_marca_total_prodtype, query_actual_marca_online_prodtype, query_actual_cat_mc_prodtype, query_actual_cat_nmc_prodtype, query_actual_cat_total_prodtype, query_actual_cat_online_prodtype, query_actual_prodtype]
        lis_query_aa_prodtype = [query_aa_marca_mc_prodtype, query_aa_marca_nmc_prodtype, query_aa_marca_total_prodtype, query_aa_marca_online_prodtype, query_aa_cat_mc_prodtype, query_aa_cat_nmc_prodtype, query_aa_cat_total_prodtype, query_aa_cat_online_prodtype, query_aa_prodtype]
        lis_query_prodtype = lis_query_actual_prodtype + lis_query_aa_prodtype + [query_prodtype]

        return lis_query_total + lis_query_class + lis_query_subclass + lis_query_prodtype

    def get_queries_rad_marcas(self, id):
        query_actual_marca_mc_total = f'''
            --TOTAL ---------------------------------------------------------
            --ACTUAL -------------------------------------------------------
            --MARCA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_MC_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_MC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_MC
                ,SUM(VENTA) VENTA_MARCA_MC
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_MC
                ,SUM(UNIDADES) UNIDADES_MARCA_MC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_actual_marca_nmc_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_NMC_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_NMC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_NMC
                ,SUM(VENTA) VENTA_MARCA_NMC
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_NMC
                ,SUM(UNIDADES) UNIDADES_MARCA_NMC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_actual_marca_total_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_TOTAL_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_TOTAL_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA
                ,SUM(VENTA) VENTA_MARCA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA
                ,SUM(UNIDADES) UNIDADES_MARCA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_actual_marca_online_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_ONLINE_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_MARCA_ONLINE_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_ONLINE
                ,SUM(VENTA) VENTA_MARCA_ONLINE
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_ONLINE
                ,SUM(UNIDADES) UNIDADES_MARCA_ONLINE

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_MARCA_MC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_MARCA_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_MARCA_MC_ONLINE
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_MARCA_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_MARCA_NMC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_MARCA_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_MARCA_NMC_ONLINE

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_actual_cat_mc_total = f'''
            --TOTAL ---------------------------------------------------------
            --ACTUAL -------------------------------------------------------
            --CATEGORIA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_MC_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_MC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_MC
                ,SUM(VENTA) VENTA_CAT_MC
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_MC
                ,SUM(UNIDADES) UNIDADES_CAT_MC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_actual_cat_nmc_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_NMC_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_NMC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_NMC
                ,SUM(VENTA) VENTA_CAT_NMC
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_NMC
                ,SUM(UNIDADES) UNIDADES_CAT_NMC

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_actual_cat_total_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_TOTAL_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_TOTAL_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT
                ,SUM(VENTA) VENTA_CAT
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT
                ,SUM(UNIDADES) UNIDADES_CAT

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_actual_cat_online_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_ONLINE_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_CAT_ONLINE_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_ONLINE
                ,SUM(VENTA) VENTA_CAT_ONLINE
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_ONLINE
                ,SUM(UNIDADES) UNIDADES_CAT_ONLINE

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_CAT_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_CAT_MC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_CAT_MC_ONLINE
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_CAT_MC_ONLINE
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_CAT_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_CAT_NMC_ONLINE
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_CAT_NMC_ONLINE
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_CAT_NMC_ONLINE

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ACTUAL'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_actual_total = f'''
            --JOIN ACTUAL_TOTAL --------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_TOTAL;
            CREATE TABLE #MON_RAD_ACTUAL_TOTAL AS (
            SELECT
                ID_RADIOGRAFIA
                ,'TODAS' CLASS_DESC
                ,'TODAS' SUBCLASS_DESC
                ,'TODAS' PROD_TYPE_DESC
                ,MARCA
                ,CLIENTES_MARCA
                ,VENTA_MARCA
                ,VENTA_MARCA_MC
                ,VENTA_MARCA_NMC
                ,TX_MARCA
                ,TX_MARCA_MC
                ,TX_MARCA_NMC
                ,UNIDADES_MARCA
                ,UNIDADES_MARCA_MC
                ,UNIDADES_MARCA_NMC
                ,VENTA_MARCA_ONLINE
                ,VENTA_MARCA_MC_ONLINE
                ,VENTA_MARCA_NMC_ONLINE
                ,CLIENTES_CAT
                ,VENTA_CAT
                ,VENTA_CAT_MC
                ,VENTA_CAT_NMC
                ,TX_CAT
                ,TX_CAT_MC
                ,TX_CAT_NMC
                ,UNIDADES_CAT
                ,UNIDADES_CAT_MC
                ,UNIDADES_CAT_NMC
                ,VENTA_CAT_ONLINE
                ,VENTA_CAT_MC_ONLINE
                ,VENTA_CAT_NMC_ONLINE
            FROM #MON_RAD_ACTUAL_MARCA_TOTAL_TOTAL A
            FULL JOIN #MON_RAD_ACTUAL_MARCA_MC_TOTAL B USING(ID_RADIOGRAFIA, MARCA)
            FULL JOIN #MON_RAD_ACTUAL_MARCA_NMC_TOTAL C USING(ID_RADIOGRAFIA, MARCA)
            FULL JOIN #MON_RAD_ACTUAL_MARCA_ONLINE_TOTAL D USING(ID_RADIOGRAFIA, MARCA)
            FULL JOIN #MON_RAD_ACTUAL_CAT_TOTAL_TOTAL E USING(ID_RADIOGRAFIA, MARCA)
            FULL JOIN #MON_RAD_ACTUAL_CAT_MC_TOTAL F USING(ID_RADIOGRAFIA, MARCA)
            FULL JOIN #MON_RAD_ACTUAL_CAT_NMC_TOTAL G USING(ID_RADIOGRAFIA, MARCA)
            FULL JOIN #MON_RAD_ACTUAL_CAT_ONLINE_TOTAL H USING(ID_RADIOGRAFIA, MARCA)
            );

            -- SELECT * FROM #MON_RAD_ACTUAL_TOTAL;

            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_TOTAL_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_MC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_NMC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_MARCA_ONLINE_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_TOTAL_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_MC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_NMC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_CAT_ONLINE_TOTAL;
        '''

        query_aa_marca_mc_total = f'''
            --TOTAL ---------------------------------------------------------
            --AA -------------------------------------------------------
            --MARCA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_MC_TOTAL;
            CREATE TABLE #MON_RAD_AA_MARCA_MC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_MC_AA
                ,SUM(VENTA) VENTA_MARCA_MC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_MC_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_MC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_aa_marca_nmc_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_NMC_TOTAL;
            CREATE TABLE #MON_RAD_AA_MARCA_NMC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_NMC_AA
                ,SUM(VENTA) VENTA_MARCA_NMC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_NMC_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_NMC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_aa_marca_total_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_TOTAL_TOTAL;
            CREATE TABLE #MON_RAD_AA_MARCA_TOTAL_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_AA
                ,SUM(VENTA) VENTA_MARCA_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_aa_marca_online_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_ONLINE_TOTAL;
            CREATE TABLE #MON_RAD_AA_MARCA_ONLINE_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_MARCA_ONLINE_AA
                ,SUM(VENTA) VENTA_MARCA_ONLINE_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA_ONLINE_AA
                ,SUM(UNIDADES) UNIDADES_MARCA_ONLINE_AA

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_MARCA_MC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_MARCA_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_MARCA_MC_ONLINE_AA
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_MARCA_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_MARCA_NMC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_MARCA_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_MARCA_NMC_ONLINE_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_aa_cat_mc_total = f'''
            --TOTAL ---------------------------------------------------------
            --AA -------------------------------------------------------
            --CATEGORIA -------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_MC_TOTAL;
            CREATE TABLE #MON_RAD_AA_CAT_MC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_MC_AA
                ,SUM(VENTA) VENTA_CAT_MC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_MC_AA
                ,SUM(UNIDADES) UNIDADES_CAT_MC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            AND IND_MC = 1
            --   AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_aa_cat_nmc_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_NMC_TOTAL;
            CREATE TABLE #MON_RAD_AA_CAT_NMC_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_NMC_AA
                ,SUM(VENTA) VENTA_CAT_NMC_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_NMC_AA
                ,SUM(UNIDADES) UNIDADES_CAT_NMC_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            AND IND_MC = 0
            --   AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_aa_cat_total_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_TOTAL_TOTAL;
            CREATE TABLE #MON_RAD_AA_CAT_TOTAL_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA

                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_AA
                ,SUM(VENTA) VENTA_CAT_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_AA
                ,SUM(UNIDADES) UNIDADES_CAT_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            --   AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_aa_cat_online_total = f'''
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_ONLINE_TOTAL;
            CREATE TABLE #MON_RAD_AA_CAT_ONLINE_TOTAL AS (
            SELECT 
                '{id}' ID_RADIOGRAFIA
                ,MARCA
                
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT_ONLINE_AA
                ,SUM(VENTA) VENTA_CAT_ONLINE_AA
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT_ONLINE_AA
                ,SUM(UNIDADES) UNIDADES_CAT_ONLINE_AA

                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN CUSTOMER_CODE END) CLIENTES_CAT_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN VENTA END) VENTA_CAT_MC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 1 THEN INVOICE_NO END) TX_CAT_MC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 1 THEN UNIDADES END) UNIDADES_CAT_MC_ONLINE_AA
                
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN CUSTOMER_CODE END) CLIENTES_CAT_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN VENTA END) VENTA_CAT_NMC_ONLINE_AA
                ,COUNT(DISTINCT CASE WHEN IND_MC = 0 THEN INVOICE_NO END) TX_CAT_NMC_ONLINE_AA
                ,SUM(CASE WHEN IND_MC = 0 THEN UNIDADES END) UNIDADES_CAT_NMC_ONLINE_AA

            FROM CHEDRAUI.VTA
            WHERE PERIODO = 'ANO_ANTERIOR'
            --   AND IND_MARCA = 1
            --   AND IND_MC = 0
            AND IND_ONLINE = 1
            --   AND IND_DUPLICADO = 0
            GROUP BY 1,2
            );
        '''

        query_aa_total = f'''
            --JOIN AA_TOTAL --------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_AA_TOTAL;
            CREATE TABLE #MON_RAD_AA_TOTAL AS (
            SELECT
                ID_RADIOGRAFIA
                ,'TODAS' CLASS_DESC
                ,'TODAS' SUBCLASS_DESC
                ,'TODAS' PROD_TYPE_DESC
                ,MARCA
                ,CLIENTES_MARCA_AA
                ,VENTA_MARCA_AA
                ,VENTA_MARCA_MC_AA
                ,VENTA_MARCA_NMC_AA
                ,TX_MARCA_AA
                ,TX_MARCA_MC_AA
                ,TX_MARCA_NMC_AA
                ,UNIDADES_MARCA_AA
                ,UNIDADES_MARCA_MC_AA
                ,UNIDADES_MARCA_NMC_AA
                ,VENTA_MARCA_ONLINE_AA
                ,VENTA_MARCA_MC_ONLINE_AA
                ,VENTA_MARCA_NMC_ONLINE_AA
                ,CLIENTES_CAT_AA
                ,VENTA_CAT_AA
                ,VENTA_CAT_MC_AA
                ,VENTA_CAT_NMC_AA
                ,TX_CAT_AA
                ,TX_CAT_MC_AA
                ,TX_CAT_NMC_AA
                ,UNIDADES_CAT_AA
                ,UNIDADES_CAT_MC_AA
                ,UNIDADES_CAT_NMC_AA
                ,VENTA_CAT_ONLINE_AA
                ,VENTA_CAT_MC_ONLINE_AA
                ,VENTA_CAT_NMC_ONLINE_AA
            FROM #MON_RAD_AA_MARCA_TOTAL_TOTAL A
            FULL JOIN #MON_RAD_AA_MARCA_MC_TOTAL B USING(ID_RADIOGRAFIA, MARCA)
            FULL JOIN #MON_RAD_AA_MARCA_NMC_TOTAL C USING(ID_RADIOGRAFIA, MARCA)
            FULL JOIN #MON_RAD_AA_MARCA_ONLINE_TOTAL D USING(ID_RADIOGRAFIA, MARCA)
            FULL JOIN #MON_RAD_AA_CAT_TOTAL_TOTAL E USING(ID_RADIOGRAFIA, MARCA)
            FULL JOIN #MON_RAD_AA_CAT_MC_TOTAL F USING(ID_RADIOGRAFIA, MARCA)
            FULL JOIN #MON_RAD_AA_CAT_NMC_TOTAL G USING(ID_RADIOGRAFIA, MARCA)
            FULL JOIN #MON_RAD_AA_CAT_ONLINE_TOTAL H USING(ID_RADIOGRAFIA, MARCA)
            );

            -- SELECT * FROM #MON_RAD_AA_TOTAL;

            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_TOTAL_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_MC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_NMC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_MARCA_ONLINE_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_TOTAL_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_MC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_NMC_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_CAT_ONLINE_TOTAL;
        '''

        query_total = f'''
            --JOIN TOTAL----------------------------------------------------------
            DROP TABLE IF EXISTS #MON_RAD_CAT_TOTAL;
            CREATE TABLE #MON_RAD_CAT_TOTAL AS (
                SELECT
                    *
                FROM #MON_RAD_ACTUAL_TOTAL
                LEFT JOIN #MON_RAD_AA_TOTAL USING(ID_RADIOGRAFIA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC, MARCA)
            
            UNION
            
            SELECT
                ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
                ,'TOTAL' MARCA
                ,CLIENTES_MARCA
                ,VENTA_MARCA
                ,VENTA_MARCA_MC
                ,VENTA_MARCA_NMC
                ,TX_MARCA
                ,TX_MARCA_MC
                ,TX_MARCA_NMC
                ,UNIDADES_MARCA
                ,UNIDADES_MARCA_MC
                ,UNIDADES_MARCA_NMC
                ,VENTA_MARCA_ONLINE
                ,VENTA_MARCA_MC_ONLINE
                ,VENTA_MARCA_NMC_ONLINE
                ,CLIENTES_CAT
                ,VENTA_CAT
                ,VENTA_CAT_MC
                ,VENTA_CAT_NMC
                ,TX_CAT
                ,TX_CAT_MC
                ,TX_CAT_NMC
                ,UNIDADES_CAT
                ,UNIDADES_CAT_MC
                ,UNIDADES_CAT_NMC
                ,VENTA_CAT_ONLINE
                ,VENTA_CAT_MC_ONLINE
                ,VENTA_CAT_NMC_ONLINE
                ,CLIENTES_MARCA_AA
                ,VENTA_MARCA_AA
                ,VENTA_MARCA_MC_AA
                ,VENTA_MARCA_NMC_AA
                ,TX_MARCA_AA
                ,TX_MARCA_MC_AA
                ,TX_MARCA_NMC_AA
                ,UNIDADES_MARCA_AA
                ,UNIDADES_MARCA_MC_AA
                ,UNIDADES_MARCA_NMC_AA
                ,VENTA_MARCA_ONLINE_AA
                ,VENTA_MARCA_MC_ONLINE_AA
                ,VENTA_MARCA_NMC_ONLINE_AA
                ,CLIENTES_CAT_AA
                ,VENTA_CAT_AA
                ,VENTA_CAT_MC_AA
                ,VENTA_CAT_NMC_AA
                ,TX_CAT_AA
                ,TX_CAT_MC_AA
                ,TX_CAT_NMC_AA
                ,UNIDADES_CAT_AA
                ,UNIDADES_CAT_MC_AA
                ,UNIDADES_CAT_NMC_AA
                ,VENTA_CAT_ONLINE_AA
                ,VENTA_CAT_MC_ONLINE_AA
                ,VENTA_CAT_NMC_ONLINE_AA
            FROM CHEDRAUI.MON_RAD_CAT
            WHERE ID_RADIOGRAFIA = '{id}'
            AND CLASS_DESC = 'TOTAL'
            AND SUBCLASS_DESC = 'TOTAL'
            AND PROD_TYPE_DESC = 'TOTAL'
            );

            --SELECT * FROM #MON_RAD_CAT_TOTAL;

            DROP TABLE IF EXISTS #MON_RAD_ACTUAL_TOTAL;
            DROP TABLE IF EXISTS #MON_RAD_AA_TOTAL;

            --GUARDAR TOTAL EN DB----------------------------------------------------------
            DELETE CHEDRAUI.MON_RAD_MARCA WHERE ID_RADIOGRAFIA = '{id}';
            INSERT INTO CHEDRAUI.MON_RAD_MARCA SELECT * FROM #MON_RAD_CAT_TOTAL;
        '''

        lis_query_actual = [query_actual_marca_mc_total, query_actual_marca_nmc_total, query_actual_marca_total_total, query_actual_marca_online_total, query_actual_cat_mc_total, query_actual_cat_nmc_total, query_actual_cat_total_total, query_actual_cat_online_total, query_actual_total]
        lis_query_aa = [query_aa_marca_mc_total, query_aa_marca_nmc_total, query_aa_marca_total_total, query_aa_marca_online_total, query_aa_cat_mc_total, query_aa_cat_nmc_total, query_aa_cat_total_total, query_aa_cat_online_total, query_aa_total]
        lis_query_total = lis_query_actual + lis_query_aa + [query_total]

        return lis_query_total
    
    def get_queries_rad_venta(self, id, proveedores, nombre):
        query_rad_desc = f"""
            DROP TABLE IF EXISTS #MON_RAD_DESC;
            CREATE TABLE #MON_RAD_DESC (
                ID_RADIOGRAFIA TEXT
                ,NOMBRE TEXT
                ,PROVEEDOR TEXT
                ,VIGENCIA_INI DATE
                ,VIGENCIA_FIN DATE
            );

            INSERT INTO #MON_RAD_DESC VALUES
            ('{id}' ,'{nombre}', '{proveedores}', '{self.dict_fechas['fecha_ini']}', '{self.dict_fechas['fecha_fin']}');

            DELETE CHEDRAUI.MON_RAD_DESC WHERE ID_RADIOGRAFIA = '{id}';
            INSERT INTO CHEDRAUI.MON_RAD_DESC SELECT * FROM #MON_RAD_DESC;
        """

        query_vta = f"""
            -- 1. TABLA DE VENTAS
            DROP TABLE IF EXISTS CHEDRAUI.VTA;
            CREATE TABLE CHEDRAUI.VTA AS (
            SELECT
                1::INT AS IND_MC
                ,LEFT(A.INVOICE_DATE, 7) MES
                ,A.PRODUCT_CODE
                ,A.INVOICE_NO
            --     ,A.INVOICE_DATE
                ,A.CUSTOMER_CODE_TY AS CUSTOMER_CODE

                ,B.CLASS_DESC
                ,B.SUBCLASS_DESC
                ,B.PROD_TYPE_DESC
                ,B.PRODUCT_DESCRIPTION
            --     ,B.PRODUCT_BRAND_VALUE
            --     ,B.PROD_ATTRIB15

                ,B.IND_MARCA
                ,B.PROVEEDOR
                ,B.MARCA
                ,B.IND_DUPLICADO
                
                ,CASE
                WHEN INVOICE_DATE BETWEEN '{self.dict_fechas['fecha_ini']}' AND '{self.dict_fechas['fecha_fin']}' THEN 'ACTUAL'
                WHEN INVOICE_DATE BETWEEN '{self.dict_fechas['fecha_ini_a']}' AND '{self.dict_fechas['fecha_fin_a']}' THEN 'ANO_ANTERIOR'
                END PERIODO

                ,COALESCE(C.NSE, 'NO SEGMENTADO') NSE
                ,COALESCE(C.TIPO_FAMILIA, 'NO SEGMENTADO') TIPO_FAMILIA
            --     ,COALESCE(C.CONTACT_INFO, 'NO SEGMENTADO') CONTACT_INFO
            --     ,COALESCE(C.VALID_CONTACT_INFO, 'NO SEGMENTADO') VALID_CONTACT_INFO

                ,D.REGION
                ,D.FORMATO_TIENDA

            --     ,E.FORMATO_TIENDA FORMATO_TIENDA_FAV
            --     ,E.STORE_DESCRIPTION DESCRIPCION_TIENDA_FAV
                
                ,F.IND_ONLINE
                            
                ,SUM(A.SALE_NET_VAL) AS VENTA
                ,SUM(A.SALE_TOT_QTY) AS UNIDADES
                    
            --     ,A.SALE_TOT_DISC_VAL COSTO

            FROM FCT_SALE_LINE A
            INNER JOIN #PRODUCTOS B USING(PRODUCT_CODE)
            LEFT JOIN CHEDRAUI.V_CUSTOMER_CONTACT AS C ON A.CUSTOMER_CODE_TY = C.CUSTOMER_CODE AND C.CONTACT_INFO IS NOT NULL
            LEFT JOIN CHEDRAUI.V_STORE D ON A.STORE_CODE = D.STORE_CODE AND A.STORE_KEY = D.STORE_KEY -- STORE OF THE CURRENT SALE
            --   LEFT JOIN CHEDRAUI.V_STORE E ON C.STORE_CODE = E.STORE_CODE AND C.STORE_KEY = E.STORE_KEY -- FAVORITE STORE OF CUSTOMER
            LEFT JOIN (SELECT DISTINCT INVOICE_NO, CASE WHEN CHANNEL_TYPE IN ('WEB','APP','CC HY') THEN 1 ELSE 0 END IND_ONLINE FROM FCT_SALE_HEADER) F USING(INVOICE_NO)
            WHERE (A.SALE_NET_VAL > 0 AND A.BUSINESS_TYPE = 'R')
            AND INVOICE_DATE BETWEEN '{self.dict_fechas['fecha_ini_a']}' AND '{self.dict_fechas['fecha_fin']}'
            {'AND IND_ONLINE = 1' if self.ind_online == 1 else ''}
            --AND FORMATO_TIENDA~'SUPERCITO'
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
            
            UNION ALL
            
            SELECT
                0::INT AS IND_MC
                ,LEFT(A.INVOICE_DATE, 7) MES
                ,A.PRODUCT_CODE
                ,A.INVOICE_NO
            --     ,A.INVOICE_DATE
                ,NULL CUSTOMER_CODE

                ,B.CLASS_DESC
                ,B.SUBCLASS_DESC
                ,B.PROD_TYPE_DESC
                ,B.PRODUCT_DESCRIPTION
            --     ,B.PRODUCT_BRAND_VALUE
            --     ,B.PROD_ATTRIB15
                
                ,B.IND_MARCA
                ,B.PROVEEDOR
                ,B.MARCA
                ,B.IND_DUPLICADO
                
                ,CASE
                WHEN INVOICE_DATE BETWEEN '{self.dict_fechas['fecha_ini']}' AND '{self.dict_fechas['fecha_fin']}' THEN 'ACTUAL'
                WHEN INVOICE_DATE BETWEEN '{self.dict_fechas['fecha_ini_a']}' AND '{self.dict_fechas['fecha_fin_a']}' THEN 'ANO_ANTERIOR'
                END PERIODO

                ,NULL NSE
                ,NULL TIPO_FAMILIA
            --     ,NULL CONTACT_INFO
            --     ,NULL VALID_CONTACT_INFO

                ,D.REGION
                ,D.FORMATO_TIENDA

            --     ,NULL FORMATO_TIENDA_FAV
            --     ,NULL DESCRIPCION_TIENDA_FAV
                
                ,F.IND_ONLINE
                            
                ,SUM(A.SALE_NET_VAL) AS VENTA
                ,SUM(A.SALE_TOT_QTY) AS UNIDADES
                    
            --     ,A.SALE_TOT_DISC_VAL COSTO

            FROM FCT_SALE_LINE_NM A
            INNER JOIN #PRODUCTOS B USING(PRODUCT_CODE)
            LEFT JOIN CHEDRAUI.V_STORE D ON A.STORE_CODE = D.STORE_CODE AND A.STORE_KEY = D.STORE_KEY -- STORE OF THE CURRENT SALE
            LEFT JOIN (SELECT DISTINCT INVOICE_NO, CASE WHEN CHANNEL_TYPE IN ('WEB','APP','CC HY') THEN 1 ELSE 0 END IND_ONLINE FROM FCT_SALE_HEADER_NM) F USING(INVOICE_NO)
            WHERE (A.SALE_NET_VAL > 0 AND A.BUSINESS_TYPE = 'R')
            AND INVOICE_DATE BETWEEN '{self.dict_fechas['fecha_ini_a']}' AND '{self.dict_fechas['fecha_fin']}'
            {'AND IND_ONLINE = 1' if self.ind_online == 1 else ''}
            --AND FORMATO_TIENDA~'SUPERCITO'
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
            );
            """

        return [query_rad_desc, query_vta]

    def get_queries_rad_lista_productos(self, id):
        query_rad_productos = f"""
            -- 2. PRODUCTOS MARCA Y CATEGORIA
            DROP TABLE IF EXISTS #MON_RAD_PRODUCTOS;
            CREATE TABLE #MON_RAD_PRODUCTOS AS (
            SELECT
                '{id}' ID_RADIOGRAFIA
                ,CLASS_CODE
                ,CLASS_DESC
                ,SUBCLASS_CODE
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
                ,PRODUCT_CODE
                ,PRODUCT_BRAND_VALUE
                ,PROD_ATTRIB15
                ,COMMERCIALB_DESC
                ,PRODUCT_DESCRIPTION
                ,IND_MARCA
                ,MARCA
                ,PROVEEDOR
            FROM #PRODUCTOS
            );
            DELETE CHEDRAUI.MON_RAD_PRODUCTOS WHERE ID_RADIOGRAFIA ='{id}';
            INSERT INTO CHEDRAUI.MON_RAD_PRODUCTOS SELECT * FROM #MON_RAD_PRODUCTOS;
        """
        return [query_rad_productos]

    def get_queries_rad_productos(self, id):
        query_rad_datos_producto = f"""
        -- 4. DATOS PRODUCTO
            DROP TABLE IF EXISTS #MON_RAD_PRODUCTO;
            CREATE TABLE #MON_RAD_PRODUCTO AS (
            SELECT
                '{id}' ID_RADIOGRAFIA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
                ,MARCA
                ,PRODUCT_CODE
                ,PRODUCT_DESCRIPTION

                --ACTUAL
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACTUAL' AND IND_MC=1 THEN CUSTOMER_CODE END) CLIENTES_MARCA
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' THEN VENTA END) VENTA_MARCA
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_MC=1 THEN VENTA END) VENTA_MARCA_MC
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_MC=0 THEN VENTA END) VENTA_MARCA_NMC
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACTUAL' THEN INVOICE_NO END) TX_MARCA
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACTUAL' AND IND_MC=1 THEN INVOICE_NO END) TX_MARCA_MC
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ACTUAL' AND IND_MC=0 THEN INVOICE_NO END) TX_MARCA_NMC
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' THEN UNIDADES END) UNIDADES_MARCA
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_MC=1 THEN UNIDADES END) UNIDADES_MARCA_MC
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_MC=0 THEN UNIDADES END) UNIDADES_MARCA_NMC
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE=1 THEN VENTA END) VENTA_MARCA_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_MC=1 AND IND_ONLINE=1 THEN VENTA END) VENTA_MARCA_MC_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_MC=0 AND IND_ONLINE=1 THEN VENTA END) VENTA_MARCA_NMC_ONLINE

                --ANTERIOR
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_MC=1 THEN CUSTOMER_CODE END) CLIENTES_MARCA_AA
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN VENTA END) VENTA_MARCA_AA
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_MC=1 THEN VENTA END) VENTA_MARCA_MC_AA
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_MC=0 THEN VENTA END) VENTA_MARCA_NMC_AA
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN INVOICE_NO END) TX_MARCA_AA
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_MC=1 THEN INVOICE_NO END) TX_MARCA_MC_AA
                ,COUNT(DISTINCT CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_MC=0 THEN INVOICE_NO END) TX_MARCA_NMC_AA
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' THEN UNIDADES END) UNIDADES_MARCA_AA
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_MC=1 THEN UNIDADES END) UNIDADES_MARCA_MC_AA
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_MC=0 THEN UNIDADES END) UNIDADES_MARCA_NMC_AA
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_ONLINE=1 THEN VENTA END) VENTA_MARCA_ONLINE_AA
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_MC=1 AND IND_ONLINE=1 THEN VENTA END) VENTA_MARCA_MC_ONLINE_AA
                ,SUM(CASE WHEN PERIODO = 'ANO_ANTERIOR' AND IND_MC=0 AND IND_ONLINE=1 THEN VENTA END) VENTA_MARCA_NMC_ONLINE_AA
            FROM CHEDRAUI.VTA
            WHERE IND_MARCA = 1
            GROUP BY 1,2,3,4,5,6,7
            HAVING VENTA_MARCA > 0
            );

            DELETE  CHEDRAUI.MON_RAD_PRODUCTO WHERE ID_RADIOGRAFIA='{id}';
            INSERT INTO CHEDRAUI.MON_RAD_PRODUCTO SELECT * FROM #MON_RAD_PRODUCTO ;
        """
        return [query_rad_datos_producto]

    def get_queries_rad_funnel_clientes(self, id):
        query_rad_funnel_clientes = f"""
            -- 6. NUMERO DE COMPRAS
            --NUMERO DE COMPRAS
            DROP TABLE IF EXISTS #NUM_COMPRAS;
            CREATE TABLE #NUM_COMPRAS AS (
            WITH
            --MARCA
            __COMPRAS_POR_CLIENTE_MARCA AS (
                SELECT
                'MARCA' TIPO
                ,MARCA
                ,CUSTOMER_CODE
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 THEN INVOICE_NO END) TX_MARCA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 0 THEN INVOICE_NO END) TX_COMPETENCIA
                ,SUM(CASE WHEN IND_MARCA = 1 THEN VENTA END) VENTA_MARCA
                ,SUM(CASE WHEN IND_MARCA = 0 THEN VENTA END) VENTA_COMPETENCIA
                FROM CHEDRAUI.VTA
                WHERE IND_MC = 1
                AND PERIODO = 'ACTUAL'
                GROUP BY 1,2,3
            )
            ,__COMPRAS_POR_CLIENTE_MARCA_TOTAL AS (
            SELECT
                'MARCA' TIPO
                ,'TOTAL' MARCA
                ,CUSTOMER_CODE
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 THEN INVOICE_NO END) TX_MARCA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 0 THEN INVOICE_NO END) TX_COMPETENCIA
                ,SUM(CASE WHEN IND_MARCA = 1 THEN VENTA END) VENTA_MARCA
                ,SUM(CASE WHEN IND_MARCA = 0 THEN VENTA END) VENTA_COMPETENCIA
                FROM CHEDRAUI.VTA
                WHERE IND_MC = 1
                AND PERIODO = 'ACTUAL'
                GROUP BY 1,2,3
            )
            ,__COMPRAS_POR_CLIENTE_CAT AS (
                SELECT
                'CAT' TIPO
                ,'CAT' MARCA
                ,CUSTOMER_CODE
                ,COUNT(DISTINCT INVOICE_NO) TX_MARCA
                ,0::INT TX_COMPETENCIA
                ,SUM(VENTA) VENTA_MARCA
                ,0::INT VENTA_COMPETENCIA
                FROM CHEDRAUI.VTA
                WHERE IND_MC = 1
                AND PERIODO = 'ACTUAL'
                GROUP BY 1,2,3
            )
            ,__NUM_COMPRAS AS (
                SELECT
                TIPO
                ,MARCA
                --CLIENTES
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT
                ,COUNT(DISTINCT CASE WHEN TX_MARCA >= 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA
                ,COUNT(DISTINCT CASE WHEN TX_MARCA = 1 THEN CUSTOMER_CODE END) CLIENTES_UNA_VEZ
                ,COUNT(DISTINCT CASE WHEN TX_MARCA > 1 THEN CUSTOMER_CODE END) CLIENTES_MAS_UNA_VEZ
                ,COUNT(DISTINCT CASE WHEN TX_MARCA = 1 AND TX_COMPETENCIA = 0 THEN CUSTOMER_CODE END) CLIENTES_SOLO_MARCA_UNA_VEZ
                ,COUNT(DISTINCT CASE WHEN TX_MARCA > 1 AND TX_COMPETENCIA = 0 THEN CUSTOMER_CODE END) CLIENTES_SOLO_MARCA_MAS_UNA_VEZ
                --VENTA  
                ,SUM(VENTA_MARCA + VENTA_COMPETENCIA) VENTA_CAT
                ,SUM(CASE WHEN TX_MARCA >= 1 THEN VENTA_MARCA END) VENTA_MARCA
                ,SUM(CASE WHEN TX_MARCA = 1 THEN VENTA_MARCA END) VENTA_UNA_VEZ
                ,SUM(CASE WHEN TX_MARCA > 1 THEN VENTA_MARCA END) VENTA_MAS_UNA_VEZ
                ,SUM(CASE WHEN TX_MARCA = 1 AND TX_COMPETENCIA = 0 THEN VENTA_MARCA END) VENTA_SOLO_MARCA_UNA_VEZ
                ,SUM(CASE WHEN TX_MARCA > 1 AND TX_COMPETENCIA = 0 THEN VENTA_MARCA END) VENTA_SOLO_MARCA_MAS_UNA_VEZ

                FROM __COMPRAS_POR_CLIENTE_MARCA
                GROUP BY 1,2
                
                UNION
                
                SELECT
                TIPO
                ,MARCA
                --CLIENTES
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT
                ,COUNT(DISTINCT CASE WHEN TX_MARCA >= 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA
                ,COUNT(DISTINCT CASE WHEN TX_MARCA = 1 THEN CUSTOMER_CODE END) CLIENTES_UNA_VEZ
                ,COUNT(DISTINCT CASE WHEN TX_MARCA > 1 THEN CUSTOMER_CODE END) CLIENTES_MAS_UNA_VEZ
                ,COUNT(DISTINCT CASE WHEN TX_MARCA = 1 AND TX_COMPETENCIA = 0 THEN CUSTOMER_CODE END) CLIENTES_SOLO_MARCA_UNA_VEZ
                ,COUNT(DISTINCT CASE WHEN TX_MARCA > 1 AND TX_COMPETENCIA = 0 THEN CUSTOMER_CODE END) CLIENTES_SOLO_MARCA_MAS_UNA_VEZ
                --VENTA  
                ,SUM(VENTA_MARCA + VENTA_COMPETENCIA) VENTA_CAT
                ,SUM(CASE WHEN TX_MARCA >= 1 THEN VENTA_MARCA END) VENTA_MARCA
                ,SUM(CASE WHEN TX_MARCA = 1 THEN VENTA_MARCA END) VENTA_UNA_VEZ
                ,SUM(CASE WHEN TX_MARCA > 1 THEN VENTA_MARCA END) VENTA_MAS_UNA_VEZ
                ,SUM(CASE WHEN TX_MARCA = 1 AND TX_COMPETENCIA = 0 THEN VENTA_MARCA END) VENTA_SOLO_MARCA_UNA_VEZ
                ,SUM(CASE WHEN TX_MARCA > 1 AND TX_COMPETENCIA = 0 THEN VENTA_MARCA END) VENTA_SOLO_MARCA_MAS_UNA_VEZ
            
                FROM __COMPRAS_POR_CLIENTE_MARCA_TOTAL
                GROUP BY 1,2
                
                UNION
                
                SELECT
                TIPO
                ,MARCA
                --CLIENTES
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT
                ,COUNT(DISTINCT CASE WHEN TX_MARCA >= 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA
                ,COUNT(DISTINCT CASE WHEN TX_MARCA = 1 THEN CUSTOMER_CODE END) CLIENTES_UNA_VEZ
                ,COUNT(DISTINCT CASE WHEN TX_MARCA > 1 THEN CUSTOMER_CODE END) CLIENTES_MAS_UNA_VEZ
                ,NULL CLIENTES_SOLO_MARCA_UNA_VEZ
                ,NULL CLIENTES_SOLO_MARCA_MAS_UNA_VEZ
                --VENTA  
                ,SUM(VENTA_MARCA + VENTA_COMPETENCIA) VENTA_CAT
                ,SUM(CASE WHEN TX_MARCA >= 1 THEN VENTA_MARCA END) VENTA_MARCA
                ,SUM(CASE WHEN TX_MARCA = 1 THEN VENTA_MARCA END) VENTA_UNA_VEZ
                ,SUM(CASE WHEN TX_MARCA > 1 THEN VENTA_MARCA END) VENTA_MAS_UNA_VEZ
                ,NULL VENTA_SOLO_MARCA_UNA_VEZ
                ,NULL VENTA_SOLO_MARCA_MAS_UNA_VEZ
            
                FROM __COMPRAS_POR_CLIENTE_CAT
                GROUP BY 1,2
            )

            SELECT * FROM __NUM_COMPRAS
            );

            --DORMIDOS Y PERDIDOS
            DROP TABLE IF EXISTS #DORMIDOS_PERDIDOS;
            CREATE TABLE #DORMIDOS_PERDIDOS AS (
            WITH
                __CLIENTES_MARCA AS (
                SELECT
                MARCA
                ,CUSTOMER_CODE

                ,MAX(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_d1']}' AND '{self.dict_fechas['fecha_fin_d1']}' AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_MARCA_D1
                ,MAX(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_d2']}' AND '{self.dict_fechas['fecha_fin_d2']}' AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_MARCA_D2
                ,MAX(CASE WHEN (MES BETWEEN '{self.dict_fechas['fecha_ini_d2']}' AND '{self.dict_fechas['fecha_fin_d2']}' OR MES BETWEEN '{self.dict_fechas['fecha_ini_p2']}' AND '{self.dict_fechas['fecha_fin_p2']}') AND IND_MARCA = 0 THEN 1 ELSE 0 END) IND_COMPETENCIA_D2

                ,MAX(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_p1']}' AND '{self.dict_fechas['fecha_fin_p1']}' AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_MARCA_P1
                ,MAX(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_p2']}' AND '{self.dict_fechas['fecha_fin_p2']}' AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_MARCA_P2
                ,MAX(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_p2']}' AND '{self.dict_fechas['fecha_fin_p2']}' AND IND_MARCA = 0 THEN 1 ELSE 0 END) IND_COMPETENCIA_P2
                
                ,SUM(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_d1']}' AND '{self.dict_fechas['fecha_fin_d2']}' AND IND_MARCA = 1 THEN VENTA ELSE 0 END) AS VENTA_DORMIDOS
                ,SUM(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_p1']}' AND '{self.dict_fechas['fecha_fin_p2']}' AND IND_MARCA = 1 THEN VENTA ELSE 0 END) AS VENTA_PERDIDOS
                
                FROM CHEDRAUI.VTA
                WHERE MES BETWEEN '{self.dict_fechas['fecha_ini_p1']}' AND '{self.dict_fechas['fecha_fin_p2']}'
                AND IND_MC = 1
                GROUP BY 1,2
            )
            ,__CLIENTES_MARCA_TOTAL AS (
                SELECT
                'TOTAL' MARCA
                ,CUSTOMER_CODE

                ,MAX(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_d1']}' AND '{self.dict_fechas['fecha_fin_d1']}' AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_MARCA_D1
                ,MAX(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_d2']}' AND '{self.dict_fechas['fecha_fin_d2']}' AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_MARCA_D2
                ,MAX(CASE WHEN (MES BETWEEN '{self.dict_fechas['fecha_ini_d2']}' AND '{self.dict_fechas['fecha_fin_d2']}' OR MES BETWEEN '{self.dict_fechas['fecha_ini_p2']}' AND '{self.dict_fechas['fecha_fin_p2']}') AND IND_MARCA = 0 THEN 1 ELSE 0 END) IND_COMPETENCIA_D2

                ,MAX(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_p1']}' AND '{self.dict_fechas['fecha_fin_p1']}' AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_MARCA_P1
                ,MAX(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_p2']}' AND '{self.dict_fechas['fecha_fin_p2']}' AND IND_MARCA = 1 THEN 1 ELSE 0 END) IND_MARCA_P2
                ,MAX(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_p2']}' AND '{self.dict_fechas['fecha_fin_p2']}' AND IND_MARCA = 0 THEN 1 ELSE 0 END) IND_COMPETENCIA_P2
                
                ,SUM(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_d1']}' AND '{self.dict_fechas['fecha_fin_d2']}' AND IND_MARCA = 1 THEN VENTA ELSE 0 END) AS VENTA_DORMIDOS
                ,SUM(CASE WHEN MES BETWEEN '{self.dict_fechas['fecha_ini_p1']}' AND '{self.dict_fechas['fecha_fin_p2']}' AND IND_MARCA = 1 THEN VENTA ELSE 0 END) AS VENTA_PERDIDOS
                
                FROM CHEDRAUI.VTA
                WHERE MES BETWEEN '{self.dict_fechas['fecha_ini_p1']}' AND '{self.dict_fechas['fecha_fin_p2']}'
                AND IND_MC = 1
                GROUP BY 1,2
            )
            ,__DORMIDOS_PERDIDOS AS (
                -- SELECT TOP 10 * FROM __CLIENTES;
                SELECT
                MARCA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA_D1 = 1 AND IND_MARCA_D2 = 0 AND IND_COMPETENCIA_D2 = 1 THEN CUSTOMER_CODE END) DORMIDOS
                ,COUNT(DISTINCT CASE WHEN IND_MARCA_P1 = 1 AND IND_MARCA_P2 = 0 AND IND_COMPETENCIA_P2 = 1 THEN CUSTOMER_CODE END) PERDIDOS
                ,SUM(CASE WHEN IND_MARCA_D1 = 1 AND IND_MARCA_D2 = 0 AND IND_COMPETENCIA_D2 = 1 THEN VENTA_DORMIDOS END) VENTA_DORMIDOS
                ,SUM(CASE WHEN IND_MARCA_P1 = 1 AND IND_MARCA_P2 = 0 AND IND_COMPETENCIA_P2 = 1 THEN VENTA_PERDIDOS END) VENTA_PERDIDOS
                FROM __CLIENTES_MARCA
                GROUP BY 1
            
                UNION
            
                SELECT
                MARCA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA_D1 = 1 AND IND_MARCA_D2 = 0 AND IND_COMPETENCIA_D2 = 1 THEN CUSTOMER_CODE END) DORMIDOS
                ,COUNT(DISTINCT CASE WHEN IND_MARCA_P1 = 1 AND IND_MARCA_P2 = 0 AND IND_COMPETENCIA_P2 = 1 THEN CUSTOMER_CODE END) PERDIDOS
                ,SUM(CASE WHEN IND_MARCA_D1 = 1 AND IND_MARCA_D2 = 0 AND IND_COMPETENCIA_D2 = 1 THEN VENTA_DORMIDOS END) VENTA_DORMIDOS
                ,SUM(CASE WHEN IND_MARCA_P1 = 1 AND IND_MARCA_P2 = 0 AND IND_COMPETENCIA_P2 = 1 THEN VENTA_PERDIDOS END) VENTA_PERDIDOS
                FROM __CLIENTES_MARCA_TOTAL
                GROUP BY 1
            )
            SELECT
                *
            FROM __DORMIDOS_PERDIDOS
            );

            --10.DATOS COMPRAS
            DROP TABLE IF EXISTS #MON_RAD_FUNNEL_CLIENTES;
            CREATE TABLE #MON_RAD_FUNNEL_CLIENTES AS (
            SELECT
                '{id}' ID_RADIOGRAFIA
                ,TIPO
                ,A.MARCA
                --CLIENTES
                ,CLIENTES_CAT
                ,CLIENTES_MARCA
                ,CLIENTES_UNA_VEZ
                ,CLIENTES_MAS_UNA_VEZ
                ,CLIENTES_SOLO_MARCA_UNA_VEZ
                ,CLIENTES_SOLO_MARCA_MAS_UNA_VEZ
                ,DORMIDOS
                ,PERDIDOS
                --VENTA  
                ,VENTA_CAT
                ,VENTA_MARCA
                ,VENTA_UNA_VEZ
                ,VENTA_MAS_UNA_VEZ
                ,VENTA_SOLO_MARCA_UNA_VEZ
                ,VENTA_SOLO_MARCA_MAS_UNA_VEZ
                ,VENTA_DORMIDOS
                ,VENTA_PERDIDOS
            FROM #NUM_COMPRAS A
            LEFT JOIN #DORMIDOS_PERDIDOS B ON A.MARCA = B.MARCA AND A.TIPO = 'MARCA'
            ORDER BY 1,3 DESC
            );

            DELETE CHEDRAUI.MON_RAD_FUNNEL_CLIENTES WHERE ID_RADIOGRAFIA = '{id}';
            INSERT INTO CHEDRAUI.MON_RAD_FUNNEL_CLIENTES SELECT * FROM #MON_RAD_FUNNEL_CLIENTES;
        """
        return [query_rad_funnel_clientes]

    def get_queries_rad_evolucion(self, id):
        query_rad_evolucion = f"""
            -- 7. EVOLUCIÓN MARCA
            DROP TABLE IF EXISTS #MON_RAD_EVO;
            CREATE TABLE #MON_RAD_EVO AS (
            SELECT
                '{id}' ID_RADIOGRAFIA
                ,MES
                ,MARCA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 THEN CUSTOMER_CODE END) CLIENTES_MARCA
                ,SUM(CASE WHEN IND_MARCA = 1 THEN VENTA END) VENTA_MARCA
                ,COUNT(DISTINCT CASE WHEN IND_MARCA = 1 THEN INVOICE_NO END) TX_MARCA
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES_CAT
                ,SUM(VENTA) VENTA_CAT
                ,COUNT(DISTINCT INVOICE_NO) TX_CAT
            FROM CHEDRAUI.VTA 
            WHERE IND_MC = 1
            AND PERIODO = 'ACTUAL'
            GROUP BY 1,2,3
            );

            DELETE CHEDRAUI.MON_RAD_EVO WHERE ID_RADIOGRAFIA='{id}';
            INSERT INTO CHEDRAUI.MON_RAD_EVO SELECT * FROM #MON_RAD_EVO;
        """
        return [query_rad_evolucion]

    def get_queries_rad_segmentos(self, id):
        query_rad_segmentado = f"""
            -- 8. SEGMENTADO
            DROP TABLE IF EXISTS #MON_RAD_SEGMENTADO;
            CREATE TABLE #MON_RAD_SEGMENTADO AS (
            WITH __SEGMENTADO AS (
            --NSE Y FAMILIA
                --MARCA
            SELECT
                'NSE_FAMILIA' TABLA
                ,MARCA
                ,NSE
                ,TIPO_FAMILIA
                ,'TOTAL' REGION
                ,'TOTAL' FORMATO_TIENDA
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES
            FROM CHEDRAUI.VTA
            WHERE IND_MC = 1
            AND IND_MARCA = 1
            AND PERIODO = 'ACTUAL'
            GROUP BY 1,2,3,4,5,6

            UNION
            
                --TOTAL MARCA
            SELECT
                'NSE_FAMILIA' TABLA
                ,'TOTAL' MARCA --AUNQUE MARCA ES GLOBAL, NO OCUPAMOS IND_DUPLICADO YA QUE SE CUENTAN DISTINCTOS CLIENTES, NO SE DUPLICAN. SOLO VENTA SE DUPLICARÍA.
                ,NSE
                ,TIPO_FAMILIA
                ,'TOTAL' REGION
                ,'TOTAL' FORMATO_TIENDA
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES
            FROM CHEDRAUI.VTA
            WHERE IND_MC = 1
            AND IND_MARCA = 1
            AND PERIODO = 'ACTUAL'
            GROUP BY 1,2,3,4,5,6
            
            UNION
            
            --REGION
                --MARCA
            SELECT
                'REGION' TABLA
                ,MARCA
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA
                ,REGION
                ,'TOTAL' FORMATO_TIENDA
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES
            FROM CHEDRAUI.VTA
            WHERE IND_MC = 1
            AND IND_MARCA = 1
            AND PERIODO = 'ACTUAL'
            GROUP BY 1,2,3,4,5,6
            
            UNION
            
                --TOTAL MARCA
            SELECT
                'REGION' TABLA
                ,'TOTAL' MARCA --AUNQUE MARCA ES GLOBAL, NO OCUPAMOS IND_DUPLICADO YA QUE SE CUENTAN DISTINCTOS CLIENTES, NO SE DUPLICAN. SOLO VENTA SE DUPLICARÍA.
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA
                ,REGION
                ,'TOTAL' FORMATO_TIENDA
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES
            FROM CHEDRAUI.VTA
            WHERE IND_MC = 1
            AND IND_MARCA = 1
            AND PERIODO = 'ACTUAL'
            GROUP BY 1,2,3,4,5,6
            
            UNION
            
            --TIENDA
                --MARCA
            SELECT
                'FORMATO_TIENDA' TABLA
                ,MARCA
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA
                ,'TOTAL' REGION
                ,FORMATO_TIENDA
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES
            FROM CHEDRAUI.VTA
            WHERE IND_MC = 1
            AND IND_MARCA = 1
            AND PERIODO = 'ACTUAL'
            GROUP BY 1,2,3,4,5,6
            
            UNION
            
                --TOTAL MARCA
            SELECT
                'FORMATO_TIENDA' TABLA
                ,'TOTAL' MARCA --AUNQUE MARCA ES GLOBAL, NO OCUPAMOS IND_DUPLICADO YA QUE SE CUENTAN DISTINCTOS CLIENTES, NO SE DUPLICAN. SOLO VENTA SE DUPLICARÍA.
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA
                ,'TOTAL' REGION
                ,FORMATO_TIENDA
                ,COUNT(DISTINCT CUSTOMER_CODE) CLIENTES
            FROM CHEDRAUI.VTA
            WHERE IND_MC = 1
            AND IND_MARCA = 1
            AND PERIODO = 'ACTUAL'
            GROUP BY 1,2,3,4,5,6
            )
            SELECT
                '{id}' ID_RADIOGRAFIA    
                ,*
            FROM __SEGMENTADO
            );

            DELETE CHEDRAUI.MON_RAD_SEGMENTADO WHERE ID_RADIOGRAFIA = '{id}';
            INSERT INTO CHEDRAUI.MON_RAD_SEGMENTADO SELECT * FROM #MON_RAD_SEGMENTADO UNION SELECT  '{id}' AS ID_RADIOGRAFIA, * FROM CHEDRAUI.MON_RAD_SEGMENTOS_CHEDRAUI;
        """
        return [query_rad_segmentado]

    def get_queries_rad_corta(self):
        query_meses = f'''
            --CREAR FECHAS DE MES
            DROP TABLE IF EXISTS #MESES;
            CREATE TABLE #MESES AS (
            WITH __MESES AS (
            SELECT DISTINCT
                LEFT(INVOICE_DATE, 7) MES
                ,EXTRACT(MONTH FROM INVOICE_DATE) MES_NUMERO
                ,CASE
                WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_rad_corta_var['date_dash']} THEN 'ACTUAL'
                WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_rad_corta_var['date_dash_aa']} THEN 'ANO_ANTERIOR'
                END AS PERIODO
                ,CASE WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_rad_corta_var['date_dash_campana']} THEN 1 ELSE 0 END AS IND_CAMPANA
                
            FROM FCT_SALE_HEADER
            WHERE LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_rad_corta_var['mes_ini_analisis_aa']}' AND '{self.dict_rad_corta_var['mes_fin_analisis']}'
            )
            SELECT
            *
            ,ROW_NUMBER() OVER(ORDER BY MES) IND_MES
            FROM __MESES
            );

            -- SELECT * FROM #MESES ORDER BY 1;
        '''

        query_tx = f'''
            DROP TABLE IF EXISTS #TX;
            CREATE TABLE #TX AS (
            SELECT
                INVOICE_NO
                ,IND_MARCA
                ,MARCA
                ,IND_ONLINE
                ,CASE WHEN SUM(SALE_NET_VAL) >= {self.dict_rad_corta_var['condicion'] if self.dict_rad_corta_var['condicion'] else 0} THEN 1 ELSE 0 END AS IND_ELEGIBLE
                ,CASE WHEN SUM(SALE_NET_VAL) >= {self.dict_rad_corta_var['condicion_50']} THEN 1 ELSE 0 END AS IND_ELEGIBLE_50
                ,CASE WHEN SUM(SALE_NET_VAL) >= {self.dict_rad_corta_var['condicion_75']} THEN 1 ELSE 0 END AS IND_ELEGIBLE_75
                ,CASE WHEN SUM(SALE_NET_VAL) >= {self.dict_rad_corta_var['condicion_100']} THEN 1 ELSE 0 END AS IND_ELEGIBLE_100
                ,CASE WHEN SUM(SALE_NET_VAL) >= {self.dict_rad_corta_var['condicion_150']} THEN 1 ELSE 0 END AS IND_ELEGIBLE_150
                ,CASE WHEN SUM(SALE_NET_VAL) >= {self.dict_rad_corta_var['condicion_200']} THEN 1 ELSE 0 END AS IND_ELEGIBLE_200
            FROM FCT_SALE_LINE
            INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
            LEFT JOIN (SELECT DISTINCT INVOICE_NO, CASE WHEN CHANNEL_TYPE IN ('WEB','APP','CC HY') THEN 1 ELSE 0 END IND_ONLINE FROM FCT_SALE_HEADER) F USING(INVOICE_NO)
            WHERE LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_rad_corta_var['mes_ini_analisis_aa']}' AND '{self.dict_rad_corta_var['mes_fin_analisis']}'
            AND BUSINESS_TYPE = 'R'
            AND SALE_NET_VAL > 0
            --   AND UI_FLG = 0
            {'AND IND_ONLINE = 1' if self.dict_rad_corta_var['ind_online'] else ''}
            GROUP BY 1,2,3,4

            UNION

            SELECT
                INVOICE_NO
                ,IND_MARCA
                ,MARCA
                ,IND_ONLINE
                ,CASE WHEN SUM(SALE_NET_VAL) >= {self.dict_rad_corta_var['condicion'] if self.dict_rad_corta_var['condicion'] else 0} THEN 1 ELSE 0 END AS IND_ELEGIBLE
                ,CASE WHEN SUM(SALE_NET_VAL) >= {self.dict_rad_corta_var['condicion_50']} THEN 1 ELSE 0 END AS IND_ELEGIBLE_50
                ,CASE WHEN SUM(SALE_NET_VAL) >= {self.dict_rad_corta_var['condicion_75']} THEN 1 ELSE 0 END AS IND_ELEGIBLE_75
                ,CASE WHEN SUM(SALE_NET_VAL) >= {self.dict_rad_corta_var['condicion_100']} THEN 1 ELSE 0 END AS IND_ELEGIBLE_100
                ,CASE WHEN SUM(SALE_NET_VAL) >= {self.dict_rad_corta_var['condicion_150']} THEN 1 ELSE 0 END AS IND_ELEGIBLE_150
                ,CASE WHEN SUM(SALE_NET_VAL) >= {self.dict_rad_corta_var['condicion_200']} THEN 1 ELSE 0 END AS IND_ELEGIBLE_200
            FROM FCT_SALE_LINE_NM
            INNER JOIN #PRODUCTOS USING(PRODUCT_CODE)
            LEFT JOIN (SELECT DISTINCT INVOICE_NO, CASE WHEN CHANNEL_TYPE IN ('WEB','APP','CC HY') THEN 1 ELSE 0 END IND_ONLINE FROM FCT_SALE_HEADER_NM) F USING(INVOICE_NO)
            WHERE LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_rad_corta_var['mes_ini_analisis_aa']}' AND '{self.dict_rad_corta_var['mes_fin_analisis']}'
            AND BUSINESS_TYPE = 'R'
            AND SALE_NET_VAL > 0
            --   AND UI_FLG = 0
            {'AND IND_ONLINE = 1' if self.dict_rad_corta_var['ind_online'] else ''}
            GROUP BY 1,2,3,4
            );
        '''
        
        query_venta = f'''
            DROP TABLE IF EXISTS #VENTA;
            CREATE TABLE #VENTA AS (
            SELECT
                1::INT IND_MC
                ,CUSTOMER_CODE_TY
                ,COALESCE(NSE, 'NO SEGMENTADO') NSE
                ,COALESCE(TIPO_FAMILIA, 'NO SEGMENTADO') TIPO_FAMILIA
                ,EXTRACT(MONTH FROM INVOICE_DATE) MES_NUMERO
                ,CASE
                    WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_rad_corta_var['date_dash']} THEN 'ACTUAL'
                    WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_rad_corta_var['date_dash_aa']} THEN 'ANO_ANTERIOR'
                END AS PERIODO
                ,A.INVOICE_NO
                ,IND_ONLINE
                ,IND_ELEGIBLE
                ,IND_ELEGIBLE_50
                ,IND_ELEGIBLE_75
                ,IND_ELEGIBLE_100
                ,IND_ELEGIBLE_150
                ,IND_ELEGIBLE_200
                ,PROVEEDOR
                ,B.MARCA
                ,B.IND_MARCA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
                ,PRODUCT_DESCRIPTION
            --       ,PRODUCT_CODE
                ,IND_DUPLICADO
            --       ,STORE_CODE
            --       ,STORE_KEY
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
            WHERE LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_rad_corta_var['mes_ini_analisis_aa']}' AND '{self.dict_rad_corta_var['mes_fin_analisis']}'
            AND BUSINESS_TYPE = 'R'
            AND SALE_NET_VAL > 0
        --     AND UI_FLG = 0
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
            );
        '''

        query_venta_nm = f'''
            DROP TABLE IF EXISTS #VENTA_NM;
            CREATE TABLE #VENTA_NM AS (
            SELECT
                0::INT IND_MC
                ,NULL CUSTOMER_CODE_TY
                ,NULL  NSE
                ,NULL  TIPO_FAMILIA
                ,EXTRACT(MONTH FROM INVOICE_DATE) MES_NUMERO
                ,CASE
                    WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_rad_corta_var['date_dash']} THEN 'ACTUAL'
                    WHEN LEFT(INVOICE_DATE, 7) BETWEEN {self.dict_rad_corta_var['date_dash_aa']} THEN 'ANO_ANTERIOR'
                END AS PERIODO
                ,A.INVOICE_NO
                ,IND_ONLINE
                ,IND_ELEGIBLE
                ,IND_ELEGIBLE_50
                ,IND_ELEGIBLE_75
                ,IND_ELEGIBLE_100
                ,IND_ELEGIBLE_150
                ,IND_ELEGIBLE_200
                ,PROVEEDOR
                ,B.MARCA
                ,B.IND_MARCA
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
                ,PRODUCT_DESCRIPTION
            --       ,PRODUCT_CODE
                ,IND_DUPLICADO
            --       ,STORE_CODE
            --       ,STORE_KEY
                ,STATE
                ,REGION
                ,FORMATO_TIENDA
                ,STORE_DESCRIPTION
                ,SUM(SALE_TOT_QTY) UNIDADES
                ,SUM(SALE_NET_VAL) VENTA
            FROM FCT_SALE_LINE_NM A
            INNER JOIN #PRODUCTOS B USING(PRODUCT_CODE)
            INNER JOIN #TX C USING(INVOICE_NO, IND_MARCA, MARCA)
            INNER JOIN CHEDRAUI.V_STORE USING(STORE_CODE, STORE_KEY)
            --     LEFT JOIN CHEDRAUI.V_CUSTOMER_CONTACT D ON A.CUSTOMER_CODE_TY = D.CUSTOMER_CODE
            WHERE LEFT(INVOICE_DATE, 7) BETWEEN '{self.dict_rad_corta_var['mes_ini_analisis_aa']}' AND '{self.dict_rad_corta_var['mes_fin_analisis']}'
            AND BUSINESS_TYPE = 'R'
            AND SALE_NET_VAL > 0
            --     AND UI_FLG = 0
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
            );
        '''

        query_indicadores = f'''
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

        query_segmentos_clientes = f'''
            DROP TABLE IF EXISTS #SEGMENTOS_CLIENTES;
            CREATE TABLE #SEGMENTOS_CLIENTES AS (
            WITH __INDICADORES_SEGMENTOS AS (
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
                ,SUM(VENTA_PO_FID_13) / 3 VENTA_PO_FID_13
                ,SUM(VENTA_PO_FID_14) / 3 VENTA_PO_FID_14
                ,SUM(VENTA_PO_FID_15) / 3 VENTA_PO_FID_15
                ,SUM(VENTA_PO_FID_16) / 3 VENTA_PO_FID_16
                ,SUM(VENTA_PO_FID_17) / 3 VENTA_PO_FID_17
                ,SUM(VENTA_PO_FID_18) / 3 VENTA_PO_FID_18
                ,SUM(VENTA_PO_FID_19) / 3 VENTA_PO_FID_19
                ,SUM(VENTA_PO_FID_20) / 3 VENTA_PO_FID_20
                ,SUM(VENTA_PO_FID_21) / 3 VENTA_PO_FID_21
                ,SUM(VENTA_PO_FID_22) / 3 VENTA_PO_FID_22
                ,SUM(VENTA_PO_FID_23) / 3 VENTA_PO_FID_23
                ,SUM(VENTA_PO_FID_24) / 3 VENTA_PO_FID_24
                
                --VENTA PO_DOR
                ,SUM(VENTA_PO_DOR_13) / 3 VENTA_PO_DOR_13
                ,SUM(VENTA_PO_DOR_14) / 3 VENTA_PO_DOR_14
                ,SUM(VENTA_PO_DOR_15) / 3 VENTA_PO_DOR_15
                ,SUM(VENTA_PO_DOR_16) / 3 VENTA_PO_DOR_16
                ,SUM(VENTA_PO_DOR_17) / 3 VENTA_PO_DOR_17
                ,SUM(VENTA_PO_DOR_18) / 3 VENTA_PO_DOR_18
                ,SUM(VENTA_PO_DOR_19) / 3 VENTA_PO_DOR_19
                ,SUM(VENTA_PO_DOR_20) / 3 VENTA_PO_DOR_20
                ,SUM(VENTA_PO_DOR_21) / 3 VENTA_PO_DOR_21
                ,SUM(VENTA_PO_DOR_22) / 3 VENTA_PO_DOR_22
                ,SUM(VENTA_PO_DOR_23) / 3 VENTA_PO_DOR_23
                ,SUM(VENTA_PO_DOR_24) / 3 VENTA_PO_DOR_24
                
                --VENTA PO_PER
                ,SUM(VENTA_PO_PER_13) / 6 VENTA_PO_PER_13
                ,SUM(VENTA_PO_PER_14) / 6 VENTA_PO_PER_14
                ,SUM(VENTA_PO_PER_15) / 6 VENTA_PO_PER_15
                ,SUM(VENTA_PO_PER_16) / 6 VENTA_PO_PER_16
                ,SUM(VENTA_PO_PER_17) / 6 VENTA_PO_PER_17
                ,SUM(VENTA_PO_PER_18) / 6 VENTA_PO_PER_18
                ,SUM(VENTA_PO_PER_19) / 6 VENTA_PO_PER_19
                ,SUM(VENTA_PO_PER_20) / 6 VENTA_PO_PER_20
                ,SUM(VENTA_PO_PER_21) / 6 VENTA_PO_PER_21
                ,SUM(VENTA_PO_PER_22) / 6 VENTA_PO_PER_22
                ,SUM(VENTA_PO_PER_23) / 6 VENTA_PO_PER_23
                ,SUM(VENTA_PO_PER_24) / 6 VENTA_PO_PER_24
                
                --VENTA PO_REC
                ,SUM(VENTA_PO_REC_13) / 9 VENTA_PO_REC_13
                ,SUM(VENTA_PO_REC_14) / 9 VENTA_PO_REC_14
                ,SUM(VENTA_PO_REC_15) / 9 VENTA_PO_REC_15
                ,SUM(VENTA_PO_REC_16) / 9 VENTA_PO_REC_16
                ,SUM(VENTA_PO_REC_17) / 9 VENTA_PO_REC_17
                ,SUM(VENTA_PO_REC_18) / 9 VENTA_PO_REC_18
                ,SUM(VENTA_PO_REC_19) / 9 VENTA_PO_REC_19
                ,SUM(VENTA_PO_REC_20) / 9 VENTA_PO_REC_20
                ,SUM(VENTA_PO_REC_21) / 9 VENTA_PO_REC_21
                ,SUM(VENTA_PO_REC_22) / 9 VENTA_PO_REC_22
                ,SUM(VENTA_PO_REC_23) / 9 VENTA_PO_REC_23
                ,SUM(VENTA_PO_REC_24) / 9 VENTA_PO_REC_24
                
                --VENTA PO_NUEVOS
                ,SUM(VENTA_PO_NUEVOS_13) / 12 VENTA_PO_NUEVOS_13
                ,SUM(VENTA_PO_NUEVOS_14) / 12 VENTA_PO_NUEVOS_14
                ,SUM(VENTA_PO_NUEVOS_15) / 12 VENTA_PO_NUEVOS_15
                ,SUM(VENTA_PO_NUEVOS_16) / 12 VENTA_PO_NUEVOS_16
                ,SUM(VENTA_PO_NUEVOS_17) / 12 VENTA_PO_NUEVOS_17
                ,SUM(VENTA_PO_NUEVOS_18) / 12 VENTA_PO_NUEVOS_18
                ,SUM(VENTA_PO_NUEVOS_19) / 12 VENTA_PO_NUEVOS_19
                ,SUM(VENTA_PO_NUEVOS_20) / 12 VENTA_PO_NUEVOS_20
                ,SUM(VENTA_PO_NUEVOS_21) / 12 VENTA_PO_NUEVOS_21
                ,SUM(VENTA_PO_NUEVOS_22) / 12 VENTA_PO_NUEVOS_22
                ,SUM(VENTA_PO_NUEVOS_23) / 12 VENTA_PO_NUEVOS_23
                ,SUM(VENTA_PO_NUEVOS_24) / 12 VENTA_PO_NUEVOS_24

                --VENTA PO_REPETIDORES
                ,SUM(VENTA_PO_REPETIDORES_13) / 12 VENTA_PO_REPETIDORES_13
                ,SUM(VENTA_PO_REPETIDORES_14) / 12 VENTA_PO_REPETIDORES_14
                ,SUM(VENTA_PO_REPETIDORES_15) / 12 VENTA_PO_REPETIDORES_15
                ,SUM(VENTA_PO_REPETIDORES_16) / 12 VENTA_PO_REPETIDORES_16
                ,SUM(VENTA_PO_REPETIDORES_17) / 12 VENTA_PO_REPETIDORES_17
                ,SUM(VENTA_PO_REPETIDORES_18) / 12 VENTA_PO_REPETIDORES_18
                ,SUM(VENTA_PO_REPETIDORES_19) / 12 VENTA_PO_REPETIDORES_19
                ,SUM(VENTA_PO_REPETIDORES_20) / 12 VENTA_PO_REPETIDORES_20
                ,SUM(VENTA_PO_REPETIDORES_21) / 12 VENTA_PO_REPETIDORES_21
                ,SUM(VENTA_PO_REPETIDORES_22) / 12 VENTA_PO_REPETIDORES_22
                ,SUM(VENTA_PO_REPETIDORES_23) / 12 VENTA_PO_REPETIDORES_23
                ,SUM(VENTA_PO_REPETIDORES_24) / 12 VENTA_PO_REPETIDORES_24

                --VENTA PO_LEALES
                ,SUM(VENTA_PO_LEALES_13) / 12 VENTA_PO_LEALES_13
                ,SUM(VENTA_PO_LEALES_14) / 12 VENTA_PO_LEALES_14
                ,SUM(VENTA_PO_LEALES_15) / 12 VENTA_PO_LEALES_15
                ,SUM(VENTA_PO_LEALES_16) / 12 VENTA_PO_LEALES_16
                ,SUM(VENTA_PO_LEALES_17) / 12 VENTA_PO_LEALES_17
                ,SUM(VENTA_PO_LEALES_18) / 12 VENTA_PO_LEALES_18
                ,SUM(VENTA_PO_LEALES_19) / 12 VENTA_PO_LEALES_19
                ,SUM(VENTA_PO_LEALES_20) / 12 VENTA_PO_LEALES_20
                ,SUM(VENTA_PO_LEALES_21) / 12 VENTA_PO_LEALES_21
                ,SUM(VENTA_PO_LEALES_22) / 12 VENTA_PO_LEALES_22
                ,SUM(VENTA_PO_LEALES_23) / 12 VENTA_PO_LEALES_23
                ,SUM(VENTA_PO_LEALES_24) / 12 VENTA_PO_LEALES_24

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

                FROM __INDICADORES_SEGMENTOS
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
                PROVEEDOR
                ,MARCA
                ,MES
                
                ,CLIENTES AS CLIENTES_SEGMENTOS
                ,CASE WHEN CLIENTES > 0 THEN RECOMPRA::NUMERIC / CLIENTES ELSE 0 END AS "%RECOMPRA"
                ,CASE WHEN CLIENTES > 0 THEN NUEVOS::NUMERIC / CLIENTES ELSE 0 END AS "%NUEVOS"
                ,CASE WHEN CLIENTES > 0 THEN FID::NUMERIC / CLIENTES ELSE 0 END AS "%FID"
                ,CASE WHEN CLIENTES > 0 THEN REC::NUMERIC / CLIENTES ELSE 0 END AS "%REC"
                ,CASE WHEN CLIENTES > 0 THEN DOR::NUMERIC / CLIENTES ELSE 0 END AS "%DOR"
                ,CASE WHEN CLIENTES > 0 THEN PER::NUMERIC / CLIENTES ELSE 0 END AS "%PER"
                ,CASE WHEN CLIENTES > 0 THEN REPETIDORES::NUMERIC / CLIENTES ELSE 0 END AS "%REPETIDORES"
                ,CASE WHEN CLIENTES > 0 THEN LEALES::NUMERIC / CLIENTES ELSE 0 END AS "%LEALES"
                
                ,CASE WHEN PO_NUEVOS > 0 THEN NUEVOS::NUMERIC / PO_NUEVOS ELSE 0 END AS "%TASA_NUEVOS"
                ,CASE WHEN PO_FID > 0 THEN FID::NUMERIC / PO_FID ELSE 0 END AS "%TASA_FID"
                ,CASE WHEN PO_REC > 0 THEN REC::NUMERIC / PO_REC ELSE 0 END AS "%TASA_REC"
                ,CASE WHEN PO_DOR > 0 THEN DOR::NUMERIC / PO_DOR ELSE 0 END AS "%TASA_DOR"
                ,CASE WHEN PO_PER > 0 THEN PER::NUMERIC / PO_PER ELSE 0 END AS "%TASA_PER"
                ,CASE WHEN PO_REPETIDORES > 0 THEN REPETIDORES::NUMERIC / PO_REPETIDORES ELSE 0 END AS "%TASA_REPETIDORES"
                ,CASE WHEN PO_LEALES > 0 THEN LEALES::NUMERIC / PO_LEALES ELSE 0 END AS "%TASA_LEALES"

                ,VENTA AS VENTA_SEGMENTOS
                ,VENTA_RECOMPRA
                ,VENTA_NUEVOS
                ,VENTA_FID
                ,VENTA_DOR
                ,VENTA_PER
                ,VENTA_REC
                ,VENTA_REPETIDORES
                ,VENTA_LEALES
                
                ,CASE WHEN VENTA > 0 THEN VENTA_RECOMPRA::NUMERIC / VENTA ELSE 0 END AS "%VENTA_RECOMPRA"
                ,CASE WHEN VENTA > 0 THEN VENTA_NUEVOS::NUMERIC / VENTA ELSE 0 END AS "%VENTA_NUEVOS"
                ,CASE WHEN VENTA > 0 THEN VENTA_FID::NUMERIC / VENTA ELSE 0 END AS "%VENTA_FID"
                ,CASE WHEN VENTA > 0 THEN VENTA_DOR::NUMERIC / VENTA ELSE 0 END AS "%VENTA_DOR"    
                ,CASE WHEN VENTA > 0 THEN VENTA_PER::NUMERIC / VENTA ELSE 0 END AS "%VENTA_PER"
                ,CASE WHEN VENTA > 0 THEN VENTA_REC::NUMERIC / VENTA ELSE 0 END AS "%VENTA_REC"
                ,CASE WHEN VENTA > 0 THEN VENTA_REPETIDORES::NUMERIC / VENTA ELSE 0 END AS "%VENTA_REPETIDORES"
                ,CASE WHEN VENTA > 0 THEN VENTA_LEALES::NUMERIC / VENTA ELSE 0 END AS "%VENTA_LEALES"    

                ,CASE WHEN VENTA_PO_RECOMPRA > 0 THEN VENTA_RECOMPRA::NUMERIC / VENTA_PO_RECOMPRA ELSE 0 END AS "%TASA_VENTA_RECOMPRA"
                ,CASE WHEN VENTA_PO_NUEVOS > 0 THEN VENTA_RECOMPRA::NUMERIC / VENTA_PO_NUEVOS ELSE 0 END AS "%TASA_VENTA_PO_NUEVOS"
                ,CASE WHEN VENTA_PO_FID > 0 THEN VENTA_FID::NUMERIC / VENTA_PO_FID ELSE 0 END AS "%TASA_VENTA_PO_FID"
                ,CASE WHEN VENTA_PO_DOR > 0 THEN VENTA_DOR::NUMERIC / VENTA_PO_DOR ELSE 0 END AS "%TASA_VENTA_PO_DOR"
                ,CASE WHEN VENTA_PO_PER > 0 THEN VENTA_PER::NUMERIC / VENTA_PO_PER ELSE 0 END AS "%TASA_VENTA_PO_PER"
                ,CASE WHEN VENTA_PO_REC > 0 THEN VENTA_REC::NUMERIC / VENTA_PO_REC ELSE 0 END AS "%TASA_VENTA_PO_REC"
                ,CASE WHEN VENTA_PO_REPETIDORES > 0 THEN VENTA_REPETIDORES::NUMERIC / VENTA_PO_REPETIDORES ELSE 0 END AS "%TASA_VENTA_PO_REPETIDORES"
                ,CASE WHEN VENTA_PO_LEALES > 0 THEN VENTA_LEALES::NUMERIC / VENTA_PO_LEALES ELSE 0 END AS "%TASA_VENTA_PO_LEALES"

            FROM __RESUMEN_SEGMENTOS A
            LEFT JOIN #MESES B USING(IND_MES)
            );
            '''
        
        query_total_marca_mc_nmc = f'''
            --TOTAL
            DROP TABLE IF EXISTS #TOTAL_MARCA_MC_NMC;
            CREATE TABLE #TOTAL_MARCA_MC_NMC AS (
            SELECT
                'TOTAL_MARCA_MC_NMC' TABLA
                ,PROVEEDOR
                ,MARCA-- A.MARCA
                ,'TOTAL' MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,'TOTAL' REGION
                ,'TOTAL' STATE
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                ,'TOTAL' PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE
            
                ,VENTA_50::NUMERIC / TX_50 AS TX_MEDIO_50
                ,VENTA_75::NUMERIC / TX_75 AS TX_MEDIO_75
                ,VENTA_100::NUMERIC / TX_100 AS TX_MEDIO_100
                ,VENTA_150::NUMERIC / TX_150 AS TX_MEDIO_150
                ,VENTA_200::NUMERIC / TX_200 AS TX_MEDIO_200
                
                ,AVG("%RECOMPRA") "%RECOMPRA"
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) CAT_VENTA_AA_ANUAL

            FROM (SELECT * FROM #VENTA UNION ALL SELECT * FROM #VENTA_NM) A
            LEFT JOIN (SELECT MES, MES_NUMERO FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            --   WHERE IND_DUPLICADO = 0
            --   AND IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_total = f'''
            --TOTAL
            DROP TABLE IF EXISTS #TOTAL;
            CREATE TABLE #TOTAL AS (
            SELECT
                'TOTAL' TABLA
                ,'TOTAL' PROVEEDOR
                ,'TOTAL' MARCA-- A.MARCA
                ,'TOTAL' MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,'TOTAL' REGION
                ,'TOTAL' STATE
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                ,'TOTAL' PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE
            
                ,VENTA_50::NUMERIC / TX_50 AS TX_MEDIO_50
                ,VENTA_75::NUMERIC / TX_75 AS TX_MEDIO_75
                ,VENTA_100::NUMERIC / TX_100 AS TX_MEDIO_100
                ,VENTA_150::NUMERIC / TX_150 AS TX_MEDIO_150
                ,VENTA_200::NUMERIC / TX_200 AS TX_MEDIO_200
                
                ,AVG("%RECOMPRA") "%RECOMPRA"
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY TABLA) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY TABLA) CAT_VENTA_AA_ANUAL

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            WHERE IND_DUPLICADO = 0
            AND IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_proveedor = f'''
            --PROVEEDOR
            DROP TABLE IF EXISTS #PROVEEDOR;
            CREATE TABLE #PROVEEDOR AS (
            SELECT
                'PROVEEDOR' TABLA
                ,PROVEEDOR
                ,'TOTAL' MARCA-- A.MARCA
                ,'TOTAL' MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,'TOTAL' REGION
                ,'TOTAL' STATE
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                ,'TOTAL' PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE
            
                ,VENTA_50::NUMERIC / TX_50 AS TX_MEDIO_50
                ,VENTA_75::NUMERIC / TX_75 AS TX_MEDIO_75
                ,VENTA_100::NUMERIC / TX_100 AS TX_MEDIO_100
                ,VENTA_150::NUMERIC / TX_150 AS TX_MEDIO_150
                ,VENTA_200::NUMERIC / TX_200 AS TX_MEDIO_200
                
                ,AVG("%RECOMPRA") "%RECOMPRA"
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR) CAT_VENTA_AA_ANUAL

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            WHERE IND_DUPLICADO = 0
            AND IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_marca = f'''
            --PROVEEDOR
            DROP TABLE IF EXISTS #MARCA;
            CREATE TABLE #MARCA AS (
            SELECT
                'MARCA' TABLA
                ,PROVEEDOR
                ,MARCA-- A.MARCA
                ,'TOTAL' MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,'TOTAL' REGION
                ,'TOTAL' STATE
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                ,'TOTAL' PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE

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
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) CAT_VENTA_AA_ANUAL

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            --   WHERE IND_DUPLICADO = 0
            WHERE IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_marca_tienda = f'''
            --TIENDA
            DROP TABLE IF EXISTS #MARCA_TIENDA;
            CREATE TABLE #MARCA_TIENDA AS (
            SELECT
                'MARCA_TIENDA' TABLA
                ,PROVEEDOR
                ,MARCA-- A.MARCA
                ,'TOTAL' MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,REGION
                ,STATE
                ,FORMATO_TIENDA
                ,STORE_DESCRIPTION
                
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                ,'TOTAL' PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE

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
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, REGION, STATE, FORMATO_TIENDA, STORE_DESCRIPTION) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, REGION, STATE, FORMATO_TIENDA, STORE_DESCRIPTION) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, REGION, STATE, FORMATO_TIENDA, STORE_DESCRIPTION) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, REGION, STATE, FORMATO_TIENDA, STORE_DESCRIPTION) CAT_VENTA_AA_ANUAL

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            WHERE IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_marca_nse_familia = f'''
            --NSE
            DROP TABLE IF EXISTS #MARCA_NSE_FAMILIA;
            CREATE TABLE #MARCA_NSE_FAMILIA AS (
            SELECT
                'MARCA_NSE_FAMILIA' TABLA
                ,PROVEEDOR
                ,MARCA-- A.MARCA
                ,'TOTAL' MES
                
                ,NSE
                ,TIPO_FAMILIA

                ,'TOTAL' REGION
                ,'TOTAL' STATE
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                ,'TOTAL' PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE

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
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, NSE, TIPO_FAMILIA) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, NSE, TIPO_FAMILIA) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY PROVEEDOR, MARCA, TABLA, NSE, TIPO_FAMILIA) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY PROVEEDOR, MARCA, TABLA, NSE, TIPO_FAMILIA) CAT_VENTA_AA_ANUAL

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            WHERE IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_marca_producto = f'''
            --PRODUCTO
            DROP TABLE IF EXISTS #MARCA_PRODUCTO;
            CREATE TABLE #MARCA_PRODUCTO AS (
            SELECT
                'MARCA_PRODUCTO' TABLA
                ,PROVEEDOR
                ,MARCA-- A.MARCA
                ,'TOTAL' MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,'TOTAL' REGION
                ,'TOTAL' STATE
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
                ,PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE

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
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC, PRODUCT_DESCRIPTION) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC, PRODUCT_DESCRIPTION) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC, PRODUCT_DESCRIPTION) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC, PRODUCT_DESCRIPTION) CAT_VENTA_AA_ANUAL

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            WHERE IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_marca_mes = f'''
            --MES MARCA
            DROP TABLE IF EXISTS #MARCA_MES;
            CREATE TABLE #MARCA_MES AS (
            SELECT
                'MARCA_MES' TABLA
                ,A.PROVEEDOR
                ,MARCA-- A.MARCA
                ,B.MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,'TOTAL' REGION
                ,'TOTAL' STATE
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                ,'TOTAL' PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE
            
                ,VENTA_50::NUMERIC / TX_50 AS TX_MEDIO_50
                ,VENTA_75::NUMERIC / TX_75 AS TX_MEDIO_75
                ,VENTA_100::NUMERIC / TX_100 AS TX_MEDIO_100
                ,VENTA_150::NUMERIC / TX_150 AS TX_MEDIO_150
                ,VENTA_200::NUMERIC / TX_200 AS TX_MEDIO_200
                
                ,AVG("%RECOMPRA") "%RECOMPRA"
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) CAT_VENTA_AA_ANUAL

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            WHERE IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_total_campana = f'''
            --CAMPANA PROVEEDOR
            DROP TABLE IF EXISTS #TOTAL_CAMPANA;
            CREATE TABLE #TOTAL_CAMPANA AS (
            SELECT
                'TOTAL_CAMPANA' TABLA
                ,'TOTAL' PROVEEDOR
                ,'TOTAL' MARCA-- A.MARCA
                ,'CAMPANA' MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,'TOTAL' REGION
                ,'TOTAL' STATE
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                ,'TOTAL' PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE
            
                ,VENTA_50::NUMERIC / TX_50 AS TX_MEDIO_50
                ,VENTA_75::NUMERIC / TX_75 AS TX_MEDIO_75
                ,VENTA_100::NUMERIC / TX_100 AS TX_MEDIO_100
                ,VENTA_150::NUMERIC / TX_150 AS TX_MEDIO_150
                ,VENTA_200::NUMERIC / TX_200 AS TX_MEDIO_200
                
                ,AVG("%RECOMPRA") "%RECOMPRA"
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY TABLA) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY TABLA) CAT_VENTA_AA_ANUAL

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO, IND_CAMPANA FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            WHERE IND_CAMPANA = 1
            AND IND_DUPLICADO = 0
            AND IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_proveedor_campana = f'''
            --CAMPANA PROVEEDOR
            DROP TABLE IF EXISTS #PROVEEDOR_CAMPANA;
            CREATE TABLE #PROVEEDOR_CAMPANA AS (
            SELECT
                'PROVEEDOR_CAMPANA' TABLA
                ,PROVEEDOR
                ,'TOTAL' MARCA-- A.MARCA
                ,'CAMPANA' MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,'TOTAL' REGION
                ,'TOTAL' STATE
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                ,'TOTAL' PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE
            
                ,VENTA_50::NUMERIC / TX_50 AS TX_MEDIO_50
                ,VENTA_75::NUMERIC / TX_75 AS TX_MEDIO_75
                ,VENTA_100::NUMERIC / TX_100 AS TX_MEDIO_100
                ,VENTA_150::NUMERIC / TX_150 AS TX_MEDIO_150
                ,VENTA_200::NUMERIC / TX_200 AS TX_MEDIO_200
                
                ,AVG("%RECOMPRA") "%RECOMPRA"
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR) CAT_VENTA_AA_ANUAL

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO, IND_CAMPANA FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            WHERE IND_CAMPANA = 1
            AND IND_DUPLICADO = 0
            AND IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_marca_campana = f'''
            --CAMPANA MARCA
            DROP TABLE IF EXISTS #MARCA_CAMPANA;
            CREATE TABLE #MARCA_CAMPANA AS (
            SELECT
                'MARCA_CAMPANA' TABLA
                ,PROVEEDOR
                ,MARCA-- A.MARCA
                ,'CAMPANA' MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,'TOTAL' REGION
                ,'TOTAL' STATE
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                ,'TOTAL' PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE
            
                ,VENTA_50::NUMERIC / TX_50 AS TX_MEDIO_50
                ,VENTA_75::NUMERIC / TX_75 AS TX_MEDIO_75
                ,VENTA_100::NUMERIC / TX_100 AS TX_MEDIO_100
                ,VENTA_150::NUMERIC / TX_150 AS TX_MEDIO_150
                ,VENTA_200::NUMERIC / TX_200 AS TX_MEDIO_200
                
                ,AVG("%RECOMPRA") "%RECOMPRA"
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA) CAT_VENTA_AA_ANUAL

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO, IND_CAMPANA FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            WHERE IND_CAMPANA = 1
            AND IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_marca_tienda_campana = f'''
            --CAMPANA MARCA
            DROP TABLE IF EXISTS #MARCA_TIENDA_CAMPANA;
            CREATE TABLE #MARCA_TIENDA_CAMPANA AS (
            SELECT
                'MARCA_TIENDA_CAMPANA' TABLA
                ,PROVEEDOR
                ,MARCA-- A.MARCA
                ,'CAMPANA' MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,REGION
                ,STATE
                ,FORMATO_TIENDA
                ,STORE_DESCRIPTION
                
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                ,'TOTAL' PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE
            
                ,VENTA_50::NUMERIC / TX_50 AS TX_MEDIO_50
                ,VENTA_75::NUMERIC / TX_75 AS TX_MEDIO_75
                ,VENTA_100::NUMERIC / TX_100 AS TX_MEDIO_100
                ,VENTA_150::NUMERIC / TX_150 AS TX_MEDIO_150
                ,VENTA_200::NUMERIC / TX_200 AS TX_MEDIO_200
                
                ,AVG("%RECOMPRA") "%RECOMPRA"
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, REGION ,STATE ,FORMATO_TIENDA ,STORE_DESCRIPTION) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, REGION ,STATE ,FORMATO_TIENDA ,STORE_DESCRIPTION) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, REGION ,STATE ,FORMATO_TIENDA ,STORE_DESCRIPTION) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, REGION ,STATE ,FORMATO_TIENDA ,STORE_DESCRIPTION) CAT_VENTA_AA_ANUAL

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO, IND_CAMPANA FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            WHERE IND_CAMPANA = 1
            AND IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_marca_nse_familia_campana = f'''
            --CAMPANA MARCA
            DROP TABLE IF EXISTS #MARCA_NSE_FAMILIA_CAMPANA;
            CREATE TABLE #MARCA_NSE_FAMILIA_CAMPANA AS (
            SELECT
                'MARCA_NSE_FAMILIA_CAMPANA' TABLA
                ,PROVEEDOR
                ,MARCA-- A.MARCA
                ,'CAMPANA' MES
                
                ,NSE
                ,TIPO_FAMILIA

                ,'TOTAL' REGION
                ,'TOTAL' STATE
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,'TOTAL' CLASS_DESC
                ,'TOTAL' SUBCLASS_DESC
                ,'TOTAL' PROD_TYPE_DESC
                ,'TOTAL' PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE
            
                ,VENTA_50::NUMERIC / TX_50 AS TX_MEDIO_50
                ,VENTA_75::NUMERIC / TX_75 AS TX_MEDIO_75
                ,VENTA_100::NUMERIC / TX_100 AS TX_MEDIO_100
                ,VENTA_150::NUMERIC / TX_150 AS TX_MEDIO_150
                ,VENTA_200::NUMERIC / TX_200 AS TX_MEDIO_200
                
                ,AVG("%RECOMPRA") "%RECOMPRA"
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, NSE, TIPO_FAMILIA) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, NSE, TIPO_FAMILIA) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, NSE, TIPO_FAMILIA) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, NSE, TIPO_FAMILIA) CAT_VENTA_AA_ANUAL

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO, IND_CAMPANA FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            WHERE IND_CAMPANA = 1
            AND IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_marca_producto_campana = f'''
            --CAMPANA MARCA
            DROP TABLE IF EXISTS #MARCA_PRODUCTO_CAMPANA;
            CREATE TABLE #MARCA_PRODUCTO_CAMPANA AS (
            SELECT
                'MARCA_PRODUCTO_CAMPANA' TABLA
                ,PROVEEDOR
                ,MARCA-- A.MARCA
                ,'CAMPANA' MES
                
                ,'TOTAL' NSE
                ,'TOTAL' TIPO_FAMILIA

                ,'TOTAL' REGION
                ,'TOTAL' STATE
                ,'TOTAL' FORMATO_TIENDA
                ,'TOTAL' STORE_DESCRIPTION
                
                ,CLASS_DESC
                ,SUBCLASS_DESC
                ,PROD_TYPE_DESC
                ,PRODUCT_DESCRIPTION

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
                
                ,SUM(CASE WHEN IND_MARCA = 1 AND PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) VENTA_ACTUAL_ONLINE
                ,SUM(CASE WHEN PERIODO = 'ACTUAL' AND IND_ONLINE = 1 THEN VENTA END) CAT_VENTA_ACTUAL_ONLINE
            
                ,VENTA_50::NUMERIC / TX_50 AS TX_MEDIO_50
                ,VENTA_75::NUMERIC / TX_75 AS TX_MEDIO_75
                ,VENTA_100::NUMERIC / TX_100 AS TX_MEDIO_100
                ,VENTA_150::NUMERIC / TX_150 AS TX_MEDIO_150
                ,VENTA_200::NUMERIC / TX_200 AS TX_MEDIO_200
                
                ,AVG("%RECOMPRA") "%RECOMPRA"
                ,AVG("%NUEVOS") "%NUEVOS"
                ,AVG("%FID") "%FID"
                ,AVG("%REC") "%REC"
                ,AVG("%DOR") "%DOR"
                ,AVG("%PER") "%PER"
                ,AVG("%REPETIDORES") "%REPETIDORES"
                ,AVG("%LEALES") "%LEALES"
                
                ,AVG("%TASA_NUEVOS") "%TASA_NUEVOS"
                ,AVG("%TASA_FID") "%TASA_FID"
                ,AVG("%TASA_REC") "%TASA_REC"
                ,AVG("%TASA_DOR") "%TASA_DOR"
                ,AVG("%TASA_PER") "%TASA_PER"
                ,AVG("%TASA_REPETIDORES") "%TASA_REPETIDORES"
                ,AVG("%TASA_LEALES") "%TASA_LEALES"
                
                ,AVG("%VENTA_RECOMPRA") "%VENTA_RECOMPRA"
                ,AVG("%VENTA_NUEVOS") "%VENTA_NUEVOS"
                ,AVG("%VENTA_FID") "%VENTA_FID"
                ,AVG("%VENTA_DOR") "%VENTA_DOR"
                ,AVG("%VENTA_PER") "%VENTA_PER"
                ,AVG("%VENTA_REC") "%VENTA_REC"
                ,AVG("%VENTA_REPETIDORES") "%VENTA_REPETIDORES"
                ,AVG("%VENTA_LEALES") "%VENTA_LEALES"

                ,AVG("%TASA_VENTA_RECOMPRA") "%TASA_VENTA_RECOMPRA"
                ,AVG("%TASA_VENTA_PO_NUEVOS") "%TASA_VENTA_PO_NUEVOS"
                ,AVG("%TASA_VENTA_PO_FID") "%TASA_VENTA_PO_FID"
                ,AVG("%TASA_VENTA_PO_DOR") "%TASA_VENTA_PO_DOR"
                ,AVG("%TASA_VENTA_PO_PER") "%TASA_VENTA_PO_PER"
                ,AVG("%TASA_VENTA_PO_REC") "%TASA_VENTA_PO_REC"
                ,AVG("%TASA_VENTA_PO_REPETIDORES") "%TASA_VENTA_PO_REPETIDORES"
                ,AVG("%TASA_VENTA_PO_LEALES") "%TASA_VENTA_PO_LEALES"
                
                ,AVG(VENTA_RECOMPRA) VENTA_RECOMPRA
                ,AVG(VENTA_NUEVOS) VENTA_NUEVOS
                ,AVG(VENTA_FID) VENTA_FID
                ,AVG(VENTA_DOR) VENTA_DOR
                ,AVG(VENTA_PER) VENTA_PER
                ,AVG(VENTA_REC) VENTA_REC
                ,AVG(VENTA_REPETIDORES) VENTA_REPETIDORES
                ,AVG(VENTA_LEALES) VENTA_LEALES

                ,SUM(VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC, PRODUCT_DESCRIPTION) VENTA_ACTUAL_ANUAL
                ,SUM(VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC, PRODUCT_DESCRIPTION) VENTA_AA_ANUAL

                ,SUM(CAT_VENTA_ACTUAL) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC, PRODUCT_DESCRIPTION) CAT_VENTA_ACTUAL_ANUAL
                ,SUM(CAT_VENTA_AA) OVER(PARTITION BY TABLA, PROVEEDOR, MARCA, CLASS_DESC, SUBCLASS_DESC, PROD_TYPE_DESC, PRODUCT_DESCRIPTION) CAT_VENTA_AA_ANUAL

            FROM #VENTA A
            LEFT JOIN (SELECT MES, MES_NUMERO, IND_CAMPANA FROM #MESES WHERE PERIODO = 'ACTUAL') B USING(MES_NUMERO)
            LEFT JOIN #SEGMENTOS_CLIENTES USING(PROVEEDOR, MARCA, MES)
            WHERE IND_CAMPANA = 1
            AND IND_MC = 1
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''
        
        query_agg = f'''
            DROP TABLE IF EXISTS #RAD_CORTA;
            CREATE TABLE #RAD_CORTA AS (
            WITH __AGG AS (
                SELECT * FROM #TOTAL_MARCA_MC_NMC
                UNION SELECT * FROM #TOTAL
            --     UNION SELECT * FROM #PROVEEDOR
                UNION SELECT * FROM #MARCA
                UNION SELECT * FROM #MARCA_TIENDA
                UNION SELECT * FROM #MARCA_NSE_FAMILIA
                UNION SELECT * FROM #MARCA_PRODUCTO
            --     UNION SELECT * FROM #TIENDA
            --     UNION SELECT * FROM #NSE_FAMILIA
            --     UNION SELECT * FROM #PRODUCTO
            --     UNION SELECT * FROM #PROVEEDOR_MES
                UNION SELECT * FROM #MARCA_MES
            --     UNION SELECT * FROM #TIENDA_MES
            --     UNION SELECT * FROM #NSE_FAMILIA_MES
            --     UNION SELECT * FROM #PRODUCTO_MES
                UNION SELECT * FROM #TOTAL_CAMPANA
            --     UNION SELECT * FROM #PROVEEDOR_CAMPANA
                UNION SELECT * FROM #MARCA_CAMPANA
                UNION SELECT * FROM #MARCA_TIENDA_CAMPANA
                UNION SELECT * FROM #MARCA_NSE_FAMILIA_CAMPANA
                UNION SELECT * FROM #MARCA_PRODUCTO_CAMPANA
                )
            SELECT
                TABLA
                ,A.PROVEEDOR
                ,A.MARCA
                ,A.MES
            
                ,NSE
                ,TIPO_FAMILIA
            
                ,REGION
                ,STATE
                ,FORMATO_TIENDA
                ,STORE_DESCRIPTION
                
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

                ,CASE WHEN CLIENTES_ACTUAL > 0 THEN TX_ACTUAL::NUMERIC / CLIENTES_ACTUAL ELSE 0 END AS FRECUENCIA_TX
                ,CASE WHEN CAT_CLIENTES_ACTUAL > 0 THEN CAT_TX_ACTUAL::NUMERIC / CAT_CLIENTES_ACTUAL ELSE 0 END AS CAT_FRECUENCIA_TX

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
                
                ,CASE WHEN VENTA_AA_ANUAL > 0 THEN VENTA_ACTUAL_ANUAL::NUMERIC / VENTA_AA_ANUAL - 1 ELSE 0 END AS CRECIMIENTO_VENTA_ANUAL
                ,CASE WHEN CAT_VENTA_AA_ANUAL > 0 THEN CAT_VENTA_ACTUAL_ANUAL::NUMERIC / CAT_VENTA_AA_ANUAL - 1 ELSE 0 END AS CAT_CRECIMIENTO_VENTA_ANUAL
            
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
                
                ,VENTA_ACTUAL_ONLINE
                ,CAT_VENTA_ACTUAL_ONLINE
                ,CASE WHEN VENTA_ACTUAL > 0 THEN VENTA_ACTUAL_ONLINE::NUMERIC / VENTA_ACTUAL ELSE 0 END AS "%VENTA_ONLINE"
                ,CASE WHEN CAT_VENTA_ACTUAL > 0 THEN CAT_VENTA_ACTUAL_ONLINE::NUMERIC / CAT_VENTA_ACTUAL ELSE 0 END AS "%CAT_VENTA_ONLINE"

                ,"%RECOMPRA"
                ,"%NUEVOS"
                ,"%FID"
                ,"%REC"
                ,"%DOR"
                ,"%PER"
                ,"%REPETIDORES"
                ,"%LEALES"
                
                ,"%TASA_NUEVOS"
                ,"%TASA_FID"
                ,"%TASA_REC"
                ,"%TASA_DOR"
                ,"%TASA_PER"
                ,"%TASA_REPETIDORES"
                ,"%TASA_LEALES"
                
                ,"%VENTA_RECOMPRA"
                ,"%VENTA_NUEVOS"
                ,"%VENTA_FID"
                ,"%VENTA_DOR"
                ,"%VENTA_PER"
                ,"%VENTA_REC"
                ,"%VENTA_REPETIDORES"
                ,"%VENTA_LEALES"

                ,"%TASA_VENTA_RECOMPRA"
                ,"%TASA_VENTA_PO_NUEVOS"
                ,"%TASA_VENTA_PO_FID"
                ,"%TASA_VENTA_PO_DOR"
                ,"%TASA_VENTA_PO_PER"
                ,"%TASA_VENTA_PO_REC"
                ,"%TASA_VENTA_PO_REPETIDORES"
                ,"%TASA_VENTA_PO_LEALES"

                ,VENTA_RECOMPRA
                ,VENTA_NUEVOS
                ,VENTA_FID
                ,VENTA_DOR
                ,VENTA_PER
                ,VENTA_REC
                ,VENTA_REPETIDORES
                ,VENTA_LEALES

            FROM __AGG A
            --   WHERE VENTA_ACTUAL > 0
            ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
            );
        '''

        query_insert_rad_corta = f'''
            DELETE CHEDRAUI.MON_BC_ANALISIS
            WHERE NOMBRE_ANALISIS = '{self.dict_rad_corta_var['nombre']}';

            INSERT INTO CHEDRAUI.MON_BC_ANALISIS
            SELECT '{self.dict_rad_corta_var['nombre']}'::VARCHAR(100) AS NOMBRE_ANALISIS, * FROM #RAD_CORTA;
        '''

        query_rad_desc = f'''
            --CALCULAR DIAS DE RECOMPRA POR CLIENTE
            DROP TABLE IF EXISTS #CLIENTES_RECOMPRA;
            CREATE TABLE #CLIENTES_RECOMPRA AS (
            WITH __TX_DIA AS (
                SELECT
                CUSTOMER_CODE_TY
                ,MARCA
                ,INVOICE_NO
                ,INVOICE_DATE
                ,LEAD(INVOICE_DATE) OVER(PARTITION BY MARCA, CUSTOMER_CODE_TY ORDER BY MARCA, INVOICE_DATE) NEXT_INVOICE_DATE
                ,DATEDIFF(DAYS, INVOICE_DATE, NEXT_INVOICE_DATE)::REAL DIAS_PARA_RECOMPRA
                FROM FCT_SALE_HEADER
                INNER JOIN (SELECT DISTINCT INVOICE_NO, MARCA FROM #VENTA WHERE IND_MARCA = 1) USING(INVOICE_NO)
            )
            SELECT
                CUSTOMER_CODE_TY
                ,MARCA
            --     ,MAX(INVOICE_DATE) LAST_INVOICE_DATE
            --     ,COUNT(DISTINCT INVOICE_DATE) DIAS_CON_COMPRA
                ,AVG(DIAS_PARA_RECOMPRA) PROMEDIO_DIAS_RECOMPRA
                ,AVG(CASE WHEN DIAS_PARA_RECOMPRA <= 30 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA_30
                ,AVG(CASE WHEN DIAS_PARA_RECOMPRA <= 90 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA_90
                ,AVG(CASE WHEN DIAS_PARA_RECOMPRA <= 180 THEN DIAS_PARA_RECOMPRA END) PROMEDIO_DIAS_RECOMPRA_180
                ,COUNT(DISTINCT INVOICE_NO) TX
                ,COUNT(DISTINCT CASE WHEN DIAS_PARA_RECOMPRA <= 30 THEN INVOICE_NO END) TX_30
                ,COUNT(DISTINCT CASE WHEN DIAS_PARA_RECOMPRA <= 90 THEN INVOICE_NO END) TX_90
                ,COUNT(DISTINCT CASE WHEN DIAS_PARA_RECOMPRA <= 180 THEN INVOICE_NO END) TX_180
            FROM __TX_DIA
            WHERE DIAS_PARA_RECOMPRA > 0
            GROUP BY 1,2
            );

            DROP TABLE IF EXISTS #RAD_DESC;
            CREATE TABLE #RAD_DESC AS (
            SELECT
                '{self.dict_rad_corta_var['nombre']}'::VARCHAR(100) AS NOMBRE_ANALISIS
                ,MARCA
                ,'{self.dict_rad_corta_var['mes_ini_campana']}' AS MES_INI_CAMPANA
                ,'{self.dict_rad_corta_var['mes_fin_campana']}' AS MES_FIN_CAMPANA
                ,'{self.dict_rad_corta_var['mes_ini_analisis']}' AS MES_INI_ANALISIS
                ,'{self.dict_rad_corta_var['mes_fin_analisis']}' AS MES_FIN_ANALISIS
                ,'{self.dict_rad_corta_var['condicion']}' AS CONDICION_COMPRA
                ,AVG(PROMEDIO_DIAS_RECOMPRA) PROMEDIO_DIAS_RECOMPRA
                ,AVG(PROMEDIO_DIAS_RECOMPRA_30) PROMEDIO_DIAS_RECOMPRA_30
                ,AVG(PROMEDIO_DIAS_RECOMPRA_90) PROMEDIO_DIAS_RECOMPRA_90
                ,AVG(PROMEDIO_DIAS_RECOMPRA_180) PROMEDIO_DIAS_RECOMPRA_180
                ,SUM(TX)::NUMERIC / COUNT(DISTINCT CUSTOMER_CODE_TY) AS FRECUENCIA_TX
                ,SUM(TX_30)::NUMERIC / COUNT(DISTINCT CASE WHEN TX_30 > 0 THEN CUSTOMER_CODE_TY END) AS FRECUENCIA_TX_30
                ,SUM(TX_90)::NUMERIC / COUNT(DISTINCT CASE WHEN TX_90 > 0 THEN CUSTOMER_CODE_TY END) AS FRECUENCIA_TX_90
                ,SUM(TX_180)::NUMERIC / COUNT(DISTINCT CASE WHEN TX_180 > 0 THEN CUSTOMER_CODE_TY END) AS FRECUENCIA_TX_180
            FROM #CLIENTES_RECOMPRA
            GROUP BY 1,2,3,4,5,6,7
            );
        '''

        query_insert_rad_desc = f'''
            DELETE CHEDRAUI.MON_BC_NOMBRE
            WHERE NOMBRE_ANALISIS = '{self.dict_rad_corta_var['nombre']}';

            INSERT INTO CHEDRAUI.MON_BC_NOMBRE
            SELECT * FROM #RAD_DESC;
        '''

        # lis_queries = [query_meses, query_tx, query_venta, query_venta_nm, query_indicadores, query_segmentos_clientes, query_total_marca_mc_nmc, query_total, 
        #                query_proveedor, query_marca, query_marca_tienda, query_marca_nse_familia, query_marca_producto, query_marca_mes, query_total_campana,
        #                query_proveedor_campana, query_marca_campana, query_marca_tienda_campana, query_marca_nse_familia_campana, query_marca_producto_campana,
        #                query_agg, query_insert_rad_corta, query_rad_desc, query_insert_rad_desc]

        lis_queries = [query_meses, query_tx, query_venta, query_venta_nm, query_indicadores, query_segmentos_clientes, query_total_marca_mc_nmc, query_total, 
                       query_marca, query_marca_tienda, query_marca_nse_familia, query_marca_producto, query_marca_mes, query_total_campana,
                       query_marca_campana, query_marca_tienda_campana, query_marca_nse_familia_campana, query_marca_producto_campana,
                       query_agg, query_insert_rad_corta, query_rad_desc, query_insert_rad_desc]

        return lis_queries

    def validate_if_rad_exists(self, conn, inicio, termino, nombre):
        # Preguntar si las tablas de radiografia ya existen con el id y nombre ingresado
        id_rad, _, _ = self.__get_id_rad(conn, inicio, termino, nombre)
        df_res = conn.select(query=f"SELECT 1 AS RESULT FROM CHEDRAUI.MON_RAD_DESC WHERE UPPER(ID_RADIOGRAFIA) = '{id_rad}' LIMIT 1")
        return not df_res.empty

    def __get_id_rad(self, conn, inicio, termino, nombre):
        # Obtener el o los proveedores de los productos seleccionados. Si es más de 1 proveedor, concatenar con guión
        proveedores = conn.select(query=f'SELECT DISTINCT PROVEEDOR FROM #PRODUCTOS')['proveedor'].str.cat(sep='-')

        # Concatenar los dos ultimos digitos del año de inicio, 2 dígitos del mes de inicio, dos ultimos digitos del año de termino, 2 dígitos del mes de termino, los proveedores y el nombre
        id = f"{inicio[2:4]}{inicio[5:7]}_{termino[2:4]}{termino[5:7]}_{proveedores}_{nombre}".replace(' ', '_')
        return id.upper(), proveedores.upper(), nombre.upper()

    def create_tables_rad(self, conn, lis_tablas_seleccionadas, override=False):
        id_rad, proveedores, nombre = self.__get_id_rad(conn, self.dict_fechas['fecha_ini'], self.dict_fechas['fecha_fin'], self.nombre)

        lis_queries_venta = self.get_queries_rad_venta(id_rad, proveedores, nombre)
        lis_queries_lista_productos = self.get_queries_rad_lista_productos(id_rad)
        lis_queries_categorias = self.get_queries_rad_categorias(id_rad)
        lis_queries_productos = self.get_queries_rad_productos(id_rad)
        lis_queries_marcas = self.get_queries_rad_marcas(id_rad)
        lis_queries_funnel_clientes = self.get_queries_rad_funnel_clientes(id_rad)
        lis_queries_evolucion = self.get_queries_rad_evolucion(id_rad)
        lis_queries_segmentos = self.get_queries_rad_segmentos(id_rad)

        self.dict_tablas_radiografia_completa['Venta'] = lis_queries_venta
        self.dict_tablas_radiografia_completa['Lista de Productos'] = lis_queries_lista_productos
        self.dict_tablas_radiografia_completa['Categorias'] = lis_queries_categorias
        self.dict_tablas_radiografia_completa['Productos'] = lis_queries_productos
        self.dict_tablas_radiografia_completa['Marcas'] = lis_queries_marcas
        self.dict_tablas_radiografia_completa['Funnel Clientes'] = lis_queries_funnel_clientes
        self.dict_tablas_radiografia_completa['Evolucion'] = lis_queries_evolucion
        self.dict_tablas_radiografia_completa['Segmentos'] = lis_queries_segmentos

        lis_exec_queries = []
        
        # Agregar a lis_exec_queries las queries de las tablas seleccionadas
        for tabla in lis_tablas_seleccionadas:
            lis_exec_queries += self.dict_tablas_radiografia_completa[tabla]

        # Mostrar una barra de progreso para la creación de las tablas con tqdm
        for query in tqdm(lis_exec_queries, desc='Creating Radiografia tables'):
            # Si override no se especifica, la tabla no existe, se crea
            if override is None:
                conn.execute(query=query)
            # Si override es True, se sobreescribe la tabla
            elif override:
                print('Overriding tables...')
                conn.execute(query=query)
            # Si override es False, se espera que la tabla exista, no se hace nada. Salir de la función
            # else:
            #     return

    def create_tables_rad_corta(self, conn, override=False):
        lis_queries = self.get_queries_rad_corta()
        table_name = '#RAD_CORTA'

        # Si override no se especifica, la tabla no existe, se crea
        if override is None:
            # Crear tablas BC
            for query in tqdm(lis_queries, desc='Creando tablas Rad Corta'):
                conn.execute(query=query)

        # Si override es True, se sobreescribe la tabla
        elif override:
            conn.drop_temporal_tables(table_name)

            for query in tqdm(lis_queries, desc='Creando tablas Rad Corta'):
                conn.execute(query)
        # Si override es False, se espera que la tabla exista, no se hace nada. Salir de la función
        else:
            return

        self.df_rad_corta = conn.select(query=f'SELECT * FROM {table_name}')

    def select_radiografias(self, conn):
        query = """
            SELECT DISTINCT
                PROVEEDOR
                ,NOMBRE
            FROM CHEDRAUI.MON_RAD_DESC
        """
        dic = conn.select(query).groupby('proveedor')['nombre'].apply(list).to_dict()
        keys = list(dic.keys())
        return dic, keys

    def get_rad_corta_data(self):
        return self.df_rad_corta
