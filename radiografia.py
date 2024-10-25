import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta

# Create a Class to handle the Radiografia data
class Radiografia():
    def __init__(self):
        self.set_rad_variables()
        self.set_dict_tablas_radiografia_completa()
    
    def set_rad_variables(self, inicio='', termino='', nombre=''):
        self.dict_fechas = {}
        self.nombre = nombre
        self.__get_fechas_campana(inicio, termino)

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
            'Segmentos': None,
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
            WHERE ID_RADIOGRAFIA = '$[ID_RADIOGRAFIA]'
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
        
        # Agregar a lis_queries las queries de las tablas seleccionadas
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

