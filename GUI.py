# Importar librerías necesarias
from tkinter import ttk, filedialog, messagebox
import tkinter as tk
from tkcalendar import DateEntry
import pandas as pd
from PIL import Image
from PIL import ImageTk
# Import Libraries
from publicos_objetivo import *
import warnings
from monetizacion import Monetizacion
from pandasgui import show
from pandastable import Table, TableModel

# Ignore SQLAlchemy warnings
warnings.filterwarnings('ignore')


class App:
    def __init__(self, root):
        self.mon = Monetizacion()
        self.root = root
        self.root.title("Cognodata Monetización - Data Science")
        self.set_icon(".\images\icono_cogno_resized.png")
        self.create_main_layout()
        self.create_menu()

    # Query para obtener los datos entre comillas simples y separados por coma
    @staticmethod
    def add_quotes(text):
        lis = []
        for item in text.split(','):
            item = f"'{item}'"
            lis.append(item)
        return (',').join(lis) if text else ''

    def set_icon(self, icon_path):
        # Cargar imagen
        img = Image.open(icon_path)
        img = img.resize((32, 32))  # Redimensionar la imagen si es necesario
        self.icon = ImageTk.PhotoImage(img)  # Guardar referencia a la imagen
        self.root.iconphoto(False, self.icon)  # Establecer el ícono

    def display_image(self, image_path):
        try:
            img = Image.open(image_path)
            self.img_tk = ImageTk.PhotoImage(img)  # Keep a reference to the image
            self.image_label.config(image=self.img_tk)
            # self.image_label.image = self.img_tk
        except Exception as e:
            print(f"Error loading image: {e}")

    def create_main_layout(self):
        # Create the main layout
        self.menu_frame = tk.Frame(self.root)
        self.menu_frame.pack(side=tk.LEFT, fill=tk.Y, padx=20, pady=20)

        self.content_frame = tk.Frame(self.root)
        self.content_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=20, pady=20)

        self.image_label = tk.Label(self.content_frame)
        self.image_label.pack(expand=True)
        
        self.display_image(".\images\logo_cogno.png")

    def end_program(self):
        # super().close()
        self.mon.close()
        self.root.quit()

    def create_menu(self):

        tk.Label(self.menu_frame, text="Menú Principal", font=("Arial", 14, "bold")).pack(pady=10)

        buttons = [
            ("1. Ingresar Productos", self.ingresar_productos),
            ("2. Generar Públicos Objetivos", self.generar_publicos_objetivos),
            ("3. Generar BusinessCase", self.generar_bc),
            # ("4. Generar Listas de envío", self.generar_listas_envio),
            # ("5. Generar Radiografía", self.generar_rad),
            ("6. Ver/Guardar Datos", self.ver_guardar_datos),
            ("Salir", self.end_program)
        ]

        for (text, command) in buttons:
            button = tk.Button(self.menu_frame, text=text, command=command, width=30)
            button.pack(pady=5)

    def show_menu(self):
        self.clear_content_frame()
        self.menu_frame.pack(side=tk.LEFT, fill=tk.Y, padx=20, pady=20)
        self.image_label.pack(expand=True)  # Ensure the image label is packed again
        self.display_image(".\images\logo_cogno.png")  # Display the placeholder image again

    def clear_content_frame(self):
        for widget in self.content_frame.winfo_children():
            widget.pack_forget()

    def validate_entries_productos(self, *args):
        lis = []

        if len(args) == 1 and isinstance(args[0], list):
            entries = args[0]
        else:
            entries = args

        for entry in entries:
            lis.append(entry.get().strip())

        if sum(bool(x) for x in lis) > 1:
            messagebox.showwarning("Advertencia", "Por favor ingrese solo un campo.")
            return False
        return True

    # Función para limpiar los campos de productos
    def clear_entries_productos(self, entry_skus, entry_marcas, entry_proveedores, entry_clases, entry_subclases, entry_prod_types):
        skus = entry_skus.get().strip().replace(', ', ',')
        marcas = self.add_quotes(entry_marcas.get().strip().replace(', ', ','))
        proveedores = self.add_quotes(entry_proveedores.get().strip().replace(', ', ','))
        clases = self.add_quotes((',').join([option for option, var in entry_clases.items() if var.get()]))
        subclases = self.add_quotes((',').join([option for option, var in entry_subclases.items() if var.get()]))
        prod_types = self.add_quotes((',').join([option for option, var in entry_prod_types.items() if var.get()]))
        
        return skus, marcas, proveedores, clases, subclases, prod_types

    # Función para agregar productos a DB
    def submit_productos(self, entry_skus, entry_marcas, entry_proveedores, entry_clases, entry_subclases, entry_prod_types):
        if self.validate_entries_productos(entry_marcas, entry_proveedores, entry_skus):
            # Limpiar los campos de productos
            skus, marcas, proveedores, clases, subclases, prod_types = self.clear_entries_productos(entry_skus, entry_marcas, entry_proveedores, entry_clases, entry_subclases, entry_prod_types)

            # Preguntar si la tabla ya existe
            if self.mon.validate_if_table_exists('#PRODUCTOS'):
                override = messagebox.askyesno("Advertencia", "Ya hay productos ingresados, ¿Desea sobreescribirlos?")
            else:
                override = None
                
            self.mon.generar_productos(skus=skus, marcas=marcas, proveedores=proveedores, clases=clases, subclases=subclases, prod_type_desc=prod_types, override=override)
            self.show_dataframe(self.mon.get_productos_agg(), "Productos")

    def productos_layout(self, clases, subclases, prod_types):
        self.menu_frame.pack_forget()
        self.clear_content_frame()
        self.content_frame.pack(padx=100)

        # Crear frames para las dos columnas
        left_frame = tk.Frame(self.content_frame)
        right_frame = tk.Frame(self.content_frame)
        left_frame.pack(side=tk.LEFT, padx=10, pady=10)
        right_frame.pack(side=tk.RIGHT, padx=10, pady=10)

        # Crear los campos para ingresar productos
        tk.Label(left_frame, text="Ingresar Productos separados por coma", font=('Arial', 10, 'bold')).pack(pady=5)

        # Ingresar productos en la columna izquierda
        tk.Label(left_frame, text="SKUs:").pack()
        entry_skus = tk.Entry(left_frame)
        entry_skus.pack()
        
        tk.Label(left_frame, text="Marcas:").pack()
        entry_marcas = tk.Entry(left_frame)
        entry_marcas.pack()
        
        tk.Label(left_frame, text="Proveedor(es):").pack()
        entry_proveedores = tk.Entry(left_frame)
        entry_proveedores.pack()

        # Filtrar por categorías de productos en la columna derecha
        tk.Label(right_frame, text="Filtrar por Clase, Sub-Clase y Tipo (opcional).", font=('Arial', 10, 'bold')).pack(pady=5)
        # Diccionario de opciones seleccionadas
        selected_options_clases = {}
        selected_options_subclases = {}
        selected_options_prod_types = {}
        
        tk.Label(right_frame, text="Clase:").pack()
        entry_clases = tk.Menubutton(right_frame, text="Select Clase", relief=tk.RAISED, bg="light gray", activebackground="gray")
        entry_clases.menu = tk.Menu(entry_clases, tearoff=0)
        entry_clases["menu"] = entry_clases.menu
        for clase in clases:
            selected_options_clases[clase] = tk.BooleanVar()
            entry_clases.menu.add_checkbutton(label=clase, variable=selected_options_clases[clase])
        entry_clases.pack()

        tk.Label(right_frame, text="Sub-clase:").pack()
        entry_subclases = tk.Menubutton(right_frame, text="Select Sub-clase", relief=tk.RAISED, bg="light gray", activebackground="gray")
        entry_subclases.menu = tk.Menu(entry_subclases, tearoff=0)
        entry_subclases["menu"] = entry_subclases.menu
        for subclase in subclases:
            selected_options_subclases[subclase] = tk.BooleanVar()
            entry_subclases.menu.add_checkbutton(label=subclase, variable=selected_options_subclases[subclase])
        entry_subclases.pack()

        tk.Label(right_frame, text="Tipo de producto:").pack()
        entry_prod_types = tk.Menubutton(right_frame, text="Select Tipo de producto", relief=tk.RAISED, bg="light gray", activebackground="gray")
        entry_prod_types.menu = tk.Menu(entry_prod_types, tearoff=0)
        entry_prod_types["menu"] = entry_prod_types.menu
        for prod_type in prod_types:
            selected_options_prod_types[prod_type] = tk.BooleanVar()
            entry_prod_types.menu.add_checkbutton(label=prod_type, variable=selected_options_prod_types[prod_type])
        entry_prod_types.pack()

        # Verificar si hay productos ingresados, si es así, botón para ver productos
        if not self.mon.df_productos.empty:
            # Buton para ver productos agrupados
            tk.Label(right_frame, text="Productos Ingresados", font=("Arial", 10, "bold")).pack(pady=5)
            tk.Button(right_frame, text="Ver Productos Ingresados", command=lambda: self.show_dataframe(self.mon.get_productos_agg(), 'Productos')).pack(pady=5)

        # Botón para ingresar productos
        tk.Button(left_frame, text="Ingresar", command=lambda: self.submit_productos(entry_skus, entry_marcas, entry_proveedores, selected_options_clases, selected_options_subclases, selected_options_prod_types)).pack(pady=10)
        tk.Button(left_frame, text="Regresar al Menú", command=self.show_menu).pack()

    def ingresar_productos(self):
        # Verificar si hay productos ingresados, si es así, mostrar advertencia
        if not self.mon.df_productos.empty:
            # Extraer las categorías de productos
            clases = self.mon.df_productos['class_desc'].unique()
            subclases = self.mon.df_productos['subclass_desc'].unique()
            prod_types = self.mon.df_productos['prod_type_desc'].unique()
            # Mostrar advertencia y botón para ver productos
            messagebox.showinfo("Información", "Ya hay productos ingresados.")
        else:
            clases, subclases, prod_types = '', '', ''

        self.productos_layout(clases, subclases, prod_types)

    # Función para validar los campos de PO
    def validate_entries_po(self, entry_tiendas='', entry_is_online=0, entry_condicion=0, entry_inicio='', entry_termino=''):

        # Validar que inicio es fecha en formato YYYY-MM-DD
        try:
                inicio = pd.to_datetime(entry_inicio.get().strip())
                termino = pd.to_datetime(entry_termino.get().strip())
        except:
            messagebox.showwarning("Advertencia", "Por favor ingrese fechas en formato YYYY-MM-DD.")
            return False
        
        # validar que is_online es Si o No, y convertir a booleano
        if entry_is_online.get() not in [0, 1]:
            messagebox.showwarning("Advertencia", "Por favor ingrese solo 'Si' para Venta Online, dejar en blanco para total.")
            return False
        
        # validar condicion es tipo numerico
        if entry_condicion.get().strip():
            if not entry_condicion.get().strip().isdigit():
                messagebox.showwarning("Advertencia", "Por favor ingrese un valor numérico para Condición de Compra.")
                return False

        # Validar que las tiendas sean de 4 digitos separadas por coma
        if entry_tiendas.get().strip():
            if not all(len(x) == 4 for x in entry_tiendas.get().strip().split(',')):
                messagebox.showwarning("Advertencia", "Por favor ingrese tiendas de 4 dígitos separadas por coma.")
                return False
        
        return True
    
    # Funcion para limpiar los campos de PO
    def clear_entries_po(self, entry_tiendas, entry_is_online, entry_condicion, entry_inicio, entry_termino):
        tiendas = self.add_quotes(entry_tiendas.get().replace(', ', ','))
        is_online = entry_is_online.get()
        condicion = entry_condicion.get()
        inicio = entry_inicio.get()
        termino = entry_termino.get()
        
        return tiendas, is_online, condicion, inicio, termino

    # Función para agregar POs a DB
    def submit_publicos(self, entry_tiendas, var, entry_condicion, entry_inicio, entry_termino):
        if self.validate_entries_po(entry_tiendas=entry_tiendas, entry_is_online=var, entry_condicion=entry_condicion, entry_inicio=entry_inicio, entry_termino=entry_termino):
            # Limpiar los campos de PO
            tiendas, is_online, condicion, inicio, termino = self.clear_entries_po(entry_tiendas, var, entry_condicion, entry_inicio, entry_termino)

            # Preguntar si ya existe la tabla PRODUCTOS
            if not self.mon.validate_if_table_exists('#PRODUCTOS'):
                messagebox.showwarning("Advertencia", "Por favor ingrese productos antes de generar Públicos Objetivos.")
                return
            
            # Preguntar si la tabla PO ya existe
            if self.mon.validate_if_table_exists('#PO'):
                override = messagebox.askyesno("Advertencia", "Ya hay Públicos Objetivos generados, ¿Desea sobreescribirlos?")
            else:
                override = None
            
            self.mon.generar_po(tiendas=tiendas, is_online=is_online, condicion=condicion, inicio=inicio, termino=termino, override=override)
            self.show_dataframe(self.mon.po.df_pos_agg, "Públicos Objetivos")

    def generar_publicos_objetivos(self):
        self.menu_frame.pack_forget()
        self.clear_content_frame()

        tk.Label(self.content_frame, text="Públicos Objetivos", font=("Arial", 14, "bold")).pack(pady=10)
        # Periodo de la campaña
        tk.Label(self.content_frame, text="Periodo del PO", font=("Arial", 10, "bold")).pack(pady=10)
        tk.Label(self.content_frame, text="Inicio:").pack()
        entry_inicio = DateEntry(self.content_frame, date_pattern='yyyy-mm-dd')
        entry_inicio.pack()
        
        tk.Label(self.content_frame, text="Termino:").pack()
        entry_termino = DateEntry(self.content_frame, date_pattern='yyyy-mm-dd')
        entry_termino.pack()

        # Datos de la campaña
        tk.Label(self.content_frame, text="Filtros de la campaña (opcional)", font=("Arial", 10, "bold")).pack(pady=10)
        tk.Label(self.content_frame, text="Tiendas (store_code, separados por coma):").pack()
        entry_tiendas = tk.Entry(self.content_frame)
        entry_tiendas.pack()
        
        var = tk.IntVar()
        entry_is_online = tk.Checkbutton(self.content_frame, text="Venta Online?", variable=var)
        entry_is_online.pack()
        
        tk.Label(self.content_frame, text="Condición de Compra (Monto):").pack()
        entry_condicion = tk.Entry(self.content_frame)
        entry_condicion.pack()
        
        tk.Button(self.content_frame, text="Calcular Públicos Objetivo", command=lambda: self.submit_publicos(entry_tiendas, var, entry_condicion, entry_inicio, entry_termino)).pack(pady=10)
        tk.Button(self.content_frame, text="Regresar al Menú", command=self.show_menu).pack()

    def ver_guardar_datos(self):
        self.menu_frame.pack_forget()
        self.clear_content_frame()

        # Crear un frame izquierdo y derecho para los botones
        tk.Label(self.content_frame, text="Datos Ingresados", font=("Arial", 12, "bold")).pack(pady=10)

        left_frame = tk.Frame(self.content_frame)
        right_frame = tk.Frame(self.content_frame)
        left_frame.pack(side=tk.LEFT, padx=10, pady=10)
        right_frame.pack(side=tk.RIGHT, padx=10, pady=10)
        
        options = ['Productos', 'Públicos Objetivos', 'BusinessCase']
        for option in options:
            tk.Button(left_frame, width=30, text=f"Ver {option}", command=lambda opt=option: self.map_title_to_dataframe(opt, type='show')).pack(pady=5)
            tk.Button(right_frame, width=30, text=f"Guardar {option}", command=lambda opt=option: self.map_title_to_dataframe(opt, type='save')).pack(pady=5)
        tk.Button(self.content_frame, text="Regresar al Menú", command=self.show_menu).pack(pady=5, side=tk.BOTTOM)

    def map_title_to_dataframe(self, title, type=None):
        # Dictionary to map the title to the dataframe
        dic = {
            'Productos': (self.mon.df_productos, 'Productos'),
            'Públicos Objetivos': (self.mon.po.df_pos_agg, 'Públicos Objetivos'),
            'BusinessCase': ([self.mon.po.df_bc_tx, self.mon.po.df_bc_unidades, self.mon.po.df_bc_tx_medio],
                             ['BusinessCase - Número de Tickets', 'BusinessCase - Número de Unidades', 'BusinessCase - Ticket Medio'])
        }
        if type == 'save':
            self.save_dataframe(dic[title][0], dic[title][1])
        elif type == 'show':
            self.show_dataframe(dic[title][0], dic[title][1])

    def show_dataframe(self, lis_dataframe: list, lis_title: list):
        
        if not isinstance(lis_dataframe, list):
            lis_dataframe = [lis_dataframe]
            lis_title = [lis_title]

        for dataframe, title in zip(lis_dataframe, lis_title):
            if dataframe.empty:
                messagebox.showwarning("Advertencia", f"No hay {title} ingresados. Por favor genere los datos en la sección correspondiente.")
                return
        
            top = tk.Toplevel(self.root)
            top.title(title)
            
            # Definir el tamaño de la ventana a 800x600
            top.geometry("800x600")

            frame = tk.Frame(top)
            frame.pack(fill='both', expand=True)

            table = Table(frame, dataframe=dataframe, showtoolbar=False, showstatusbar=False)
            table.show()

    def save_dataframe(self, lis_dataframe: list, lis_title: list):

        if not isinstance(lis_dataframe, list):
            lis_dataframe = [lis_dataframe]
            lis_title = [lis_title]

        for dataframe, title in zip(lis_dataframe, lis_title):
            if dataframe.empty:
                messagebox.showwarning("Advertencia", f"No hay {title} ingresados. Por favor genere los datos en la sección correspondiente.")
                return
    
            if dataframe.empty:
                messagebox.showwarning("Advertencia", f"No hay {title} para guardar.")
                return

            file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
            if file_path:
                self.mon.df_productos.to_csv(file_path, index=False)
                messagebox.showinfo("Información", title + " guardado exitosamente.")

    def generar_bc(self):
        # Crear layout para el BusinessCase

        # Verificar si hay productos y POs generados

        # Extraer datos para el BusinessCase
        self.mon.generar_datos_bc()
        self.show_dataframe(self.mon.po.df_bc_tx, "BusinessCase - Número de Tickets")
        self.show_dataframe(self.mon.po.df_bc_unidades, "BusinessCase - Número de Unidades")
        self.show_dataframe(self.mon.po.df_bc_tx_medio, "BusinessCase - Ticket Medio")

# root = tk.Tk()
# app = App(root)
# root.mainloop()
