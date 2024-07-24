import pandas as pd

# Create a Class to handle the Radiografia data
class Radiografia():
    def __init__(self):
        self.set_rad_variables()
    
    def set_rad_variables(self, inicio='', termino=''):
        self.inicio = inicio
        self.termino = termino
        self.__get_fechas_campana()

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
        
    def get_query_create_rad_tables(self, table_name):
        query = f"""
            SELECT
                *
            INTO
                {table_name}
            FROM
                (
                    SELECT
                        *
                    FROM
                        [dbo].[RAD]
                    WHERE
                        [Fecha] >= '{self.dict_fechas['ini']}'
                        AND [Fecha] <= '{self.dict_fechas['fin']}'
                ) AS [RAD]
        """
        return query

    def create_tables_rad(self, conn, override=False):
        # Crear tabla temporal de Radiografía
        table_name = '#RAD'
        query = self.get_query_create_rad_tables(table_name)
        print(query)
        # Si override no se especifica, la tabla no existe, se crea
        if override is None:
            conn.execute(query=query)
        # Si override es True, se sobreescribe la tabla
        elif override:
            conn.override_table(table_name, query)
        # Si override es False, se espera que la tabla exista, no se hace nada. Salir de la función
        else:
            return