from connection import Conn
from productos import Productos
from publicos_objetivo import PublicosObjetivo
from radiografia import Radiografia
from campana import Campana

# Create a Class to handle the Monetizacion data that inherits from the Conn class
class Monetizacion(Conn, Productos):
    
    def __init__(self, name='Monetizacion'):
        Conn.__init__(self, name=name)
        Productos.__init__(self)
        self.po = PublicosObjetivo()
        self.rad = Radiografia()
        self.camp = Campana()
        # self.ds = DataScience()

    def get_marcas_proveedores(self):
        query = f'''
            SELECT DISTINCT
                MARCA
                ,PROVEEDOR
            FROM CHEDRAUI.MON_CRM_SKU_RAZONSOCIAL
        '''
        df = self.select(query)

        # Reemplaza nulos por 'Sin Marca' y 'Sin Proveedor'
        df['marca'] = df['marca'].fillna('SIN MARCA')
        df['proveedor'] = df['proveedor'].fillna('SIN PROVEEDOR')

        return sorted(list(df.marca.unique())), sorted(list(df.proveedor.unique()))
    
    def get_campanas(self):
        query = "SELECT * FROM CHEDRAUI.MON_CAMP_DESC ORDER BY NOMBRE"
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

    # Función para extraer datos para el BusinessCase
    def generar_datos_bc(self, override):
        # Create BusinessCase tables
        self.po.create_tables_bc(self, override)

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
    
    def generar_datos_rad(self, inicio, termino, nombre, override):
        # Set variables
        self.rad.set_rad_variables(inicio=inicio, termino=termino, nombre=nombre)
        # Create RAD table
        self.rad.create_tables_rad(self, override=override)

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
    
    def actualizar_resultados_campana(self, nombre_campana):
        # Crear las variables de la campaña
        self.camp.set_campana_variables(self, nombre_campana)
        # Actualizar la campaña con el nombre proporcionado
        self.camp.actualizar_resultados_campana(self)

    def obtener_nombres_tablas_campanas(self):
        # Obtener los nombres de las tablas de las campañas
        return self.camp.get_table_names_campana()
    
    def obtener_total_cadena_tiendas(self):
        # Obtener el total de la cadena de tiendas
        return self.camp.get_total_cadena_tiendas(self)

    def ejecutar_ds(self):
        # self.ds.create_analisis_ds()
        pass