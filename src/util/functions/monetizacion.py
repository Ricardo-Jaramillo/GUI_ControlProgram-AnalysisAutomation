from util.functions.connection import Conn
from util.functions.productos import Productos
from util.functions.publicos_objetivo import PublicosObjetivo
from util.functions.radiografia import Radiografia
from util.functions.campana import Campana
from util.functions.analisis_html import Analisis
from pathlib import Path

schema = Conn().get_schema()

# Create a Class to handle the Monetizacion data that inherits from the Conn class
class Monetizacion(Conn, Productos):
    
    def __init__(self, name='Monetizacion'):
        Conn.__init__(self, name=name)
        Productos.__init__(self)
        self.po = PublicosObjetivo()
        self.rad = Radiografia()
        self.camp = Campana()
        self.analisis = Analisis()
        # self.ds = DataScience()

    def get_marcas_proveedores(self):
        query = f'''
            SELECT DISTINCT
                MARCA
                ,PROVEEDOR
            FROM {schema}.MON_CRM_SKU_RAZONSOCIAL
        '''
        df = self.select(query)
        print('Obteniendo marcas y proveedores...')
        # Reemplaza nulos por 'Sin Marca' y 'Sin Proveedor'
        df['marca'] = df['marca'].fillna('SIN MARCA')
        df['proveedor'] = df['proveedor'].fillna('SIN PROVEEDOR')

        return sorted(list(df.marca.unique())), sorted(list(df.proveedor.unique()))
    
    def get_campanas(self):
        query = f"SELECT * FROM {schema}.MON_CAMP_DESC ORDER BY NOMBRE"
        return self.select(query)

    def generar_productos(self, skus, marcas, proveedores, clases, subclases, prod_type_desc, override):
        # Set variables
        self.set_products(skus=skus, marcas=marcas, proveedores=proveedores, clases=clases, subclases=subclases, prod_type_desc=prod_type_desc)
        # Create Productos table
        self.create_tabla_productos(self, override)

    def generar_po(self, tiendas, excluir, is_online, condicion, inicio, termino, override):
        # Set variables
        self.po.set_pos_variables(tiendas=tiendas, excluir=excluir, is_online=is_online, condicion=condicion, inicio=inicio, termino=termino)
        # Create PO table
        self.po.create_table_pos_temporal(self, override)

    # Función para extraer datos para Radiografía Corta
    def generar_datos_rad_corta(self, nombre, inicio_campana, fin_campana, inicio_analisis,fin_analisis, condicion, online, override):
        self.rad.set_rad_corta_variables(nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion, online)
        # Create Radiografía Corta tables
        self.rad.create_tables_rad_corta(self, override)

    def generar_analisis_bc(self, nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion, elegible, familia, nse, tiendas, lis_agg):
        self.po.set_bc_variables(nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion, elegible, familia, nse, tiendas)
        # Create BusinessCase tables
        self.po.create_table_analisis_bc(self, lis_agg)

    def get_bc_options_familia(self):
        return self.po.get_bc_options_familia(self)

    def get_bc_options_nse(self):
        return self.po.get_bc_options_nse(self)

    def guardar_reporte_analisis_bc(self, df, file_path):
        # Extraer la ruta completa del folder y el nombre del archivo
        foldername = Path(file_path).parent
        # Quitar cualquier extensión del nombre del archivo
        filename = file_path.split('/')[-1]
        
        self.analisis.set_df(df)
        self.analisis.save_html(nombre=self.po.dict_bc_analisis_var['nombre'], foldername=foldername, filename=filename, show_figs=False)
        # self.analisis.return_html()

    def obtener_analisis_bc(self):
        # Get Analisis BusinessCase data
        return self.po.get_analisis_bc_data()
    
    def obtener_bc(self):
        # Get BusinessCase data
        return self.po.get_bc_data()

    def generar_po_envios_conteo(self, venta_antes, venta_camp, cond_antes, cond_camp, online):
        # Set variables
        self.po.set_po_filtros_variables(venta_antes=venta_antes, venta_camp=venta_camp, cond_antes=cond_antes, cond_camp=cond_camp, online=online)
        # Ver Conteo de  Público Objetivo con Filtros aplicados
        self.po.create_table_po_envios_conteo(self)

    def generar_listas_envio(self, canales, grupo_control, prioridad_online):
        # Set variables
        self.po.set_listas_envio_variables(canales=canales, grupo_control=grupo_control, prioridad_online=prioridad_online)
        # Crear Listas de Envío
        self.po.create_table_listas_envio(self)
    
    def generar_datos_rad(self, inicio, termino, nombre, online, override, lis_seleccion):
        # Set variables
        self.rad.set_rad_variables(inicio=inicio, termino=termino, nombre=nombre, online=online)
        # Create RAD table
        self.rad.create_tables_rad(self, lis_seleccion, override=override)

    def separar_listas_envio(self):
        # Separar Listas de Envío
        self.po.separar_listas_envio()

    def obtener_info_campana(self, nombre):
        # Obtener la información de la campaña
        return self.camp.get_campana_info(self, nombre)
    
    def guardar_info_campana(self, nombre_campana, table_name, df):        
        # Guardar la información de la campaña
        return self.camp.guardar_info_campana(self, nombre_campana, table_name, df)

    def eliminar_info_campana(self, nombre_campana):
        # Eliminar la información de la campaña
        self.camp.eliminar_info_campana(self, nombre_campana)
    
    def actualizar_resultados_campana(self, nombre_campana, lis_seleccion):
        # Crear las variables de la campaña
        self.camp.set_campana_variables(self, nombre_campana)
        # Actualizar la campaña con el nombre proporcionado
        self.camp.actualizar_resultados_campana(self, lis_seleccion)

    def obtener_nombres_tablas_campanas(self):
        # Obtener los nombres de las tablas de las campañas
        return self.camp.get_table_names_campana()
    
    def obtener_total_cadena_tiendas(self):
        # Obtener el total de la cadena de tiendas
        return self.camp.get_total_cadena_tiendas(self)
    
    def obtener_lista_productos(self):
        # Validar que la tabla #PRODUCTOS exista y obtener los productos ingresados
        if self.validate_if_table_exists('#PRODUCTOS'):
            lista = self.df_productos[self.df_productos['ind_marca'] == 1]['product_code'].apply(str).str.cat(sep=',')
        else:
            lista = None
        return lista

    def obtener_lista_opciones(self, nombre): # REVISAR FUNCION
        # Obtener los nombres de las opciones de las tablas
        if nombre == 'Campañas':
            return self.camp.get_table_names_resultados()
        elif nombre == 'Radiografia Completa':
            return self.rad.get_table_names_radiografia_completa()

    def obtener_lista_opciones_agg_analisis(self):
        return self.po.get_agg_analisis()

    def validate_campaign_products(self, campaign_name):
        # Validar que la campaña tenga productos
        skus = self.camp.get_campaign_products(self, campaign_name)
        # Si hay lista de productos, crear la tabla de #productos a partir de la lista de skus y retornar True, si no hay productos, retornar False
        if bool(skus):
            # Dropear tabla de productos si existe
            self.drop_temporal_tables(['#PRODUCTOS'])

            # Crear tabla de productos
            print('Creando tabla de productos...')
            self.generar_productos(skus=skus, marcas='', proveedores='', clases='', subclases='', prod_type_desc='', override=None)
            return True
        else:
            return False

    def ejecutar_ds(self):
        # self.ds.create_analisis_ds()
        pass