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
        self.clases = ''
        self.subclases = ''
        self.prod_types = ''
        self.root = root
        self.root.title("Cognodata Monetización - Data Science")
        self.set_icon(".\images\icono_cogno_resized.png")
        self.create_main_layout()
        self.create_menu()
        self.root.resizable(0, 0)

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
            ("4. Generar Listas de envío", self.generar_listas),
            ("5. Generar Radiografía", self.generar_rad),
            ("6. Resultados de Campañas", self.generar_resultados),
            ("7. Ver/Guardar Datos", self.ver_guardar_datos),
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

    # Función para agregar productos a DB
    def submit_productos(self, entry_skus, entry_marcas, entry_proveedores):
        if self.validate_entries_productos(entry_skus, entry_marcas, entry_proveedores):
            # Extraer los datos de los campos de productos
            skus = entry_skus.get().strip().replace(', ', ',')
            marcas = self.add_quotes(entry_marcas.get().strip().replace(', ', ','))
            proveedores = self.add_quotes(entry_proveedores.get().strip().replace(', ', ','))

            df = self.mon.get_df_categorias(skus=skus, marcas=marcas, proveedores=proveedores)

            # Preguntar si se desea filtrar los productos
            op = messagebox.askyesno("Advertencia", "¿Desea filtrar por Categorías?")
            print('Filtrar productos:', op)
            print('Printing Clases Antes...', self.clases, self.subclases, self.prod_types)
            if op:
                print('Printing Clases Durante...', self.clases, self.subclases, self.prod_types)
                ventana = tk.Toplevel(self.root)
                self.filtrar_productos(ventana, df)
                ventana.wait_window(ventana)
                # self.wait_window(self.ventana)
            # else:
            #     self.clases, subclases, prod_types = '', '', ''

            print('Printing Clases Después...', self.clases, self.subclases, self.prod_types)
            # Preguntar si la tabla ya existe
            if self.mon.validate_if_table_exists('#PRODUCTOS'):
                override = messagebox.askyesno("Advertencia", "Ya hay productos ingresados, ¿Desea sobreescribirlos?")
            else:
                override = None
                
            self.mon.generar_productos(skus=skus, marcas=marcas, proveedores=proveedores, clases=self.clases, subclases=self.subclases, prod_type_desc=self.prod_types, override=override)
            self.show_dataframe(self.mon.get_productos_agg(), "Productos")

    def filtrar_productos(self, ventana, df):
        
        categorias = df['class_desc'].unique()
        subcategorias = df['subclass_desc'].unique()
        prod_types = df['prod_type_desc'].unique()

        self.categoria_global = []
        self.subcategoria_global = []
        self.prod_type_global = []

        def actualizar_categoria(event):
            categoria_seleccionada = [listbox_categoria.get(i) for i in listbox_categoria.curselection()]
            subcategoria_seleccionada = [listbox_subcategoria.get(i) for i in listbox_subcategoria.curselection()]
            prod_type_seleccionado = [listbox_prod_type.get(i) for i in listbox_prod_type.curselection()]
            print('Seleccionados:', categoria_seleccionada, subcategoria_seleccionada, prod_type_seleccionado)
            print('Globales', self.categoria_global, self.subcategoria_global, self.prod_type_global)

            if categoria_seleccionada:
                if not self.subcategoria_global:
                    subcategoria_seleccionada = subcategorias
                else:
                    subcategoria_seleccionada = self.subcategoria_global
                if not self.prod_type_global:
                    prod_type_seleccionado = prod_types
                else:
                    prod_type_seleccionado = self.prod_type_global

                listbox_subcategoria.delete(0, tk.END)
                listbox_subcategoria.insert(tk.END, *df[df['class_desc'].isin(categoria_seleccionada) & df['subclass_desc'].isin(subcategoria_seleccionada) & df['prod_type_desc'].isin(prod_type_seleccionado)]['subclass_desc'].unique())

                listbox_prod_type.delete(0, tk.END)
                listbox_prod_type.insert(tk.END, *df[df['class_desc'].isin(categoria_seleccionada) & df['subclass_desc'].isin(subcategoria_seleccionada) & df['prod_type_desc'].isin(prod_type_seleccionado)]['prod_type_desc'].unique())
            

        def actualizar_subcategoria(event):
            categoria_seleccionada = [listbox_categoria.get(i) for i in listbox_categoria.curselection()]
            subcategoria_seleccionada = [listbox_subcategoria.get(i) for i in listbox_subcategoria.curselection()]
            prod_type_seleccionado = [listbox_prod_type.get(i) for i in listbox_prod_type.curselection()]
            print('Seleccionados:', categoria_seleccionada, subcategoria_seleccionada, prod_type_seleccionado)
            print('Globales', self.categoria_global, self.subcategoria_global, self.prod_type_global)

            if subcategoria_seleccionada:
                if not self.categoria_global:
                    categoria_seleccionada = categorias
                else:
                    categoria_seleccionada = self.categoria_global
                if not self.prod_type_global:
                    prod_type_seleccionado = prod_types
                else:
                    prod_type_seleccionado = self.prod_type_global

                listbox_categoria.delete(0, tk.END)
                listbox_categoria.insert(tk.END, *df[df['subclass_desc'].isin(subcategoria_seleccionada) & df['class_desc'].isin(categoria_seleccionada) & df['prod_type_desc'].isin(prod_type_seleccionado)]['class_desc'].unique())
                
                listbox_prod_type.delete(0, tk.END)
                listbox_prod_type.insert(tk.END, *df[df['subclass_desc'].isin(subcategoria_seleccionada) & df['class_desc'].isin(categoria_seleccionada) & df['prod_type_desc'].isin(prod_type_seleccionado)]['prod_type_desc'].unique())

        def actualizar_prod_type(event):
            categoria_seleccionada = [listbox_categoria.get(i) for i in listbox_categoria.curselection()]
            subcategoria_seleccionada = [listbox_subcategoria.get(i) for i in listbox_subcategoria.curselection()]
            prod_type_seleccionado = [listbox_prod_type.get(i) for i in listbox_prod_type.curselection()]
            print('Seleccionados:', categoria_seleccionada, subcategoria_seleccionada, prod_type_seleccionado)
            print('Globales', self.categoria_global, self.subcategoria_global, self.prod_type_global)

            if prod_type_seleccionado:
                if not self.categoria_global:
                    categoria_seleccionada = categorias
                else:
                    categoria_seleccionada = self.categoria_global
                if not self.subcategoria_global:
                    subcategoria_seleccionada = subcategorias
                else:
                    subcategoria_seleccionada = self.subcategoria_global

                listbox_categoria.delete(0, tk.END)
                listbox_categoria.insert(tk.END, *df[df['prod_type_desc'].isin(prod_type_seleccionado) & df['class_desc'].isin(categoria_seleccionada) & df['subclass_desc'].isin(subcategoria_seleccionada)]['class_desc'].unique())
                
                listbox_subcategoria.delete(0, tk.END)
                listbox_subcategoria.insert(tk.END, *df[df['prod_type_desc'].isin(prod_type_seleccionado) & df['class_desc'].isin(categoria_seleccionada) & df['subclass_desc'].isin(subcategoria_seleccionada)]['subclass_desc'].unique())
                
        def seleccionar_categoria():
            # global categoria_global
            categoria_seleccionada = [listbox_categoria.get(i) for i in listbox_categoria.curselection()]
            print('Seleccionados:', categoria_seleccionada)
            print('Globales', self.categoria_global, self.subcategoria_global, self.prod_type_global)
            if categoria_seleccionada:
                # Actualizar los valores de los listbox
                listbox_categoria.delete(0, tk.END)
                listbox_categoria.insert(tk.END, *categoria_seleccionada)

                # Desactivar edicion de los listbox
                listbox_categoria.config(state=tk.DISABLED)

                # Deshabilitar boton de seleccionar categoria y oscurecerlo
                boton_categoria.config(state=tk.DISABLED)
                boton_categoria.config(style='Dark.TButton')

                # Guardar la categoria seleccionada
                self.categoria_global = categoria_seleccionada
                print('Asignado a global:', self.categoria_global)
                print('Globales', self.categoria_global, self.subcategoria_global, self.prod_type_global)

        def seleccionar_subcategoria():
            # global subcategoria_global
            subcategoria_seleccionada = [listbox_subcategoria.get(i) for i in listbox_subcategoria.curselection()]
            print('Seleccionados:', subcategoria_seleccionada)
            print('Globales', self.categoria_global, self.subcategoria_global, self.prod_type_global)
            if subcategoria_seleccionada:
                # Actualizar los valores de los listbox
                listbox_subcategoria.delete(0, tk.END)
                listbox_subcategoria.insert(tk.END, *subcategoria_seleccionada)

                # Desactivar edicion de los listbox
                listbox_subcategoria.config(state=tk.DISABLED)
                
                # Deshabilitar boton de seleccionar subcategoria y oscurecerlo
                boton_subcategoria.config(state=tk.DISABLED)
                boton_subcategoria.config(style='Dark.TButton')

                # Guardar la subcategoria seleccionada
                self.subcategoria_global = subcategoria_seleccionada
                print('Asignado a global:', self.subcategoria_global)
                print('Globales', self.categoria_global, self.subcategoria_global, self.prod_type_global)

        def seleccionar_prod_type():
            # global prod_type_global
            prod_type_seleccionado = [listbox_prod_type.get(i) for i in listbox_prod_type.curselection()]
            print('Seleccionados:', prod_type_seleccionado)
            print('Globales', self.categoria_global, self.subcategoria_global, self.prod_type_global)
            if prod_type_seleccionado:
                # Actualizar los valores de los listbox
                listbox_prod_type.delete(0, tk.END)
                listbox_prod_type.insert(tk.END, *prod_type_seleccionado)
                
                # Desactivar edicion de los listbox
                listbox_prod_type.config(state=tk.DISABLED)

                # Deshabilitar boton de seleccionar prod_type y oscurecerlo
                boton_prod_type.config(state=tk.DISABLED)
                boton_prod_type.config(style='Dark.TButton')

                # Guardar el prod_type seleccionado
                self.prod_type_global = prod_type_seleccionado
                print('Asignado a global:', self.prod_type_global)
                print('Globales', self.categoria_global, self.subcategoria_global, self.prod_type_global)

        def reiniciar_selecciones():
            # global categoria_global
            # global subcategoria_global
            # global prod_type_global
            # Activar edicion de los listbox
            listbox_categoria.config(state=tk.NORMAL)
            listbox_subcategoria.config(state=tk.NORMAL)
            listbox_prod_type.config(state=tk.NORMAL)
            # Habilitar botones de seleccionar y aclararlos
            boton_categoria.config(state=tk.NORMAL)
            boton_categoria.config(style='TButton')
            boton_subcategoria.config(state=tk.NORMAL)
            boton_subcategoria.config(style='TButton')
            boton_prod_type.config(state=tk.NORMAL)
            boton_prod_type.config(style='TButton')
            # Borrar las selecciones
            listbox_categoria.delete(0, tk.END)
            listbox_subcategoria.delete(0, tk.END)
            listbox_prod_type.delete(0, tk.END)
            # Insertar los valores originales
            listbox_categoria.insert(tk.END, *categorias)
            listbox_subcategoria.insert(tk.END, *subcategorias)
            listbox_prod_type.insert(tk.END, *prod_types)
            # Reiniciar las variables globales
            self.categoria_global = []
            self.subcategoria_global = []
            self.prod_type_global = []
            
        def aplicar_filtros():
            # global categoria_global
            # global subcategoria_global
            # global prod_type_global

            categoria_seleccionada = (', ').join([item for item in listbox_categoria.get(0, tk.END)])
            subcategoria_seleccionada = (', ').join([item for item in listbox_subcategoria.get(0, tk.END)])
            prod_type_seleccionado = (', ').join([item for item in listbox_prod_type.get(0, tk.END)])

            print(f"Categoría: {categoria_seleccionada}")
            print(f"Subcategoría: {subcategoria_seleccionada}")
            print(f"Prod Type: {prod_type_seleccionado}")

            # Mostrar en un messagebox que los filtros se aplicaron correctamente
            messagebox.showinfo("Filtros Aplicados", f"Categoría: {categoria_seleccionada}\nSubcategoría: {subcategoria_seleccionada}\nProd Type: {prod_type_seleccionado}")
            # categoria_global = categoria_seleccionada
            # subcategoria_global = subcategoria_seleccionada
            # prod_type_global = prod_type_seleccionado
            
            self.clases = self.add_quotes(categoria_seleccionada.replace(', ', ','))
            self.subclases = self.add_quotes(subcategoria_seleccionada.replace(', ', ','))
            self.prod_types = self.add_quotes(prod_type_seleccionado.replace(', ', ','))
            ventana.destroy()

        # Crear la ventana principal

        ventana.title("Selección de Productos")

        # Crear los widgets
        label_categoria = ttk.Label(ventana, text="Categoría:")
        listbox_categoria = tk.Listbox(ventana, selectmode=tk.EXTENDED, width=25)
        listbox_categoria.insert(tk.END, *categorias)
        listbox_categoria.bind("<<ListboxSelect>>", actualizar_categoria)

        label_subcategoria = ttk.Label(ventana, text="Subcategoría:")
        listbox_subcategoria = tk.Listbox(ventana, selectmode=tk.EXTENDED, width=25)
        listbox_subcategoria.insert(tk.END, *subcategorias)
        listbox_subcategoria.bind("<<ListboxSelect>>", actualizar_subcategoria)

        label_prod_type = ttk.Label(ventana, text="Prod Type:")
        listbox_prod_type = tk.Listbox(ventana, selectmode=tk.EXTENDED, width=25)
        listbox_prod_type.insert(tk.END, *prod_types)
        listbox_prod_type.bind("<<ListboxSelect>>", actualizar_prod_type)

        # Boton para seleccionar cada uno de los elementos
        boton_categoria = ttk.Button(ventana, text="Seleccionar Categoría", command=seleccionar_categoria)
        boton_subcategoria = ttk.Button(ventana, text="Seleccionar Subcategoría", command=seleccionar_subcategoria)
        boton_prod_type = ttk.Button(ventana, text="Seleccionar Prod Type", command=seleccionar_prod_type)

        # Boton para reiniciar las selecciones
        boton_reiniciar = ttk.Button(ventana, text="Reiniciar Selecciones", command=reiniciar_selecciones)

        # Boton para aplicar los filtros
        boton_aplicar = ttk.Button(ventana, text="Aplicar Filtros", command=aplicar_filtros)

        # Posicionar los widgets en la ventana utilizando el sistema de gestión de geometría grid
        label_categoria.grid(row=0, column=0, padx=5)
        label_subcategoria.grid(row=0, column=1, padx=5)
        label_prod_type.grid(row=0, column=2, padx=5)

        listbox_categoria.grid(row=1, column=0, sticky="w", padx=5, pady=5)
        listbox_subcategoria.grid(row=1, column=1, sticky="w", padx=5, pady=5)
        listbox_prod_type.grid(row=1, column=2, sticky="w", padx=5, pady=5)

        boton_categoria.grid(row=2, column=0, padx=5, pady=5)
        boton_subcategoria.grid(row=2, column=1, padx=5, pady=5)
        boton_prod_type.grid(row=2, column=2, padx=5, pady=5)

        boton_reiniciar.grid(row=4, column=0, padx=5, pady=5)
        boton_aplicar.grid(row=4, column=2, padx=5, pady=5)

        # Configurar el tamaño de las columnas
        ventana.grid_columnconfigure(0, weight=1)
        ventana.grid_columnconfigure(1, weight=1)

        # return categoria_global, subcategoria_global, prod_type_global

        # Iniciar el bucle de eventos
        # ventana.mainloop()

    def ingresar_productos(self):
        # Verificar si hay productos ingresados, si es así, mostrar advertencia
        if not self.mon.df_productos.empty:
            messagebox.showinfo("Información", "Ya hay productos ingresados.")

        self.menu_frame.pack_forget()
        self.clear_content_frame()
        # self.content_frame.pack(padx=100)

        marcas, proveedores = self.mon.get_marcas_proveedores()

        frame = tk.Frame(self.content_frame)
        frame.pack(pady=10, padx=10)

        # Crear Labels y entradas para los productos
        tk.Label(frame, text="Seleccionar Productos", font=('Arial', 14, 'bold')).grid(row=0, column=0, columnspan=2, pady=10)
        
        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=1, column=0, columnspan=2, pady=5, sticky="we")

        # Definir los productos
        tk.Label(frame, text="SKUS").grid(row=2, column=0, pady=5)
        entry_skus = tk.Entry(frame, width=25)
        entry_skus.grid(row=2, column=1, pady=5)
        
        # Definir marcas
        tk.Label(frame, text="Marca(s):").grid(row=3, column=0, pady=5)
        entry_marcas = tk.StringVar()
        dropdown_marcas = ttk.Combobox(frame, textvariable=entry_marcas, values=marcas, state='normal', width=22)
        dropdown_marcas.grid(row=3, column=1, pady=5)
        
        # Definir proveedores
        tk.Label(frame, text="Proveedor(es):").grid(row=4, column=0, pady=5)
        entry_proveedores = tk.StringVar()
        dropdown_proveedores = ttk.Combobox(frame, textvariable=entry_proveedores, values=proveedores, state='normal', width=22)
        dropdown_proveedores.grid(row=4, column=1, pady=5)

        # Label para advertencia. Datos separados por coma
        tk.Label(frame, text="Nota: Ingresar datos separados por coma.", font=('Arial', 10, 'bold')).grid(row=5, column=0, columnspan=2, pady=10)

        # Ingresar productos
        tk.Button(frame, text="Ingresar", command=lambda: self.submit_productos(entry_skus, entry_marcas, entry_proveedores)).grid(row=6, column=1, pady=10)

        # Verificar si hay productos ingresados, si es así, botón para ver productos
        if not self.mon.df_productos.empty:
            # Buton para ver productos ya ingresados
            tk.Button(frame, text="Ver Productos", command=lambda: self.show_dataframe(self.mon.get_productos_agg(), 'Productos')).grid(row=6, column=0, pady=10)

        # Botón para ingresar productos
        # tk.Button(frame, text="Ingresar", command=lambda: self.submit_productos(entry_skus, entry_marcas, entry_proveedores, selected_options_clases, selected_options_subclases, selected_options_prod_types)).pack(pady=10)
        tk.Button(frame, text="Regresar al Menú", command=self.show_menu).grid(row=100, column=1, pady=10)

    # Función para validar los campos de PO
    def validate_entries_po(self, entry_tiendas, entry_is_online, entry_condicion, entry_inicio, entry_termino):

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
                             ['BusinessCase - Número de Tickets', 'BusinessCase - Número de Unidades', 'BusinessCase - Ticket Medio']),
            'Listas de Envío': ([self.mon.po.df_listas_envio], ['Listas de Envío']),
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

    # Validar los campos para el BusinessCase
    def validate_entries_bc(self, presupuesto='', sms=0, mail=0, cupon=0, wa=0, wa_ratio=0):
        if not presupuesto or not presupuesto.isdigit():
            messagebox.showwarning("Advertencia", "Por favor ingrese un presupuesto válido.")
            return False
        if not any([sms, mail, cupon, wa]):
            messagebox.showwarning("Advertencia", "Por favor seleccione al menos un canal.")
            return False
        if wa and not (wa_ratio or wa_ratio.isdigit()):
            messagebox.showwarning("Advertencia", "Por favor ingrese un porcentaje para WA.")
            return False
        
        return True
    
    # Función para limpiar los campos de BC
    def clear_entries_bc(self, entry_presupuesto, var_sms, var_mail, var_cupon, var_wa, var_wa_ratio):
        presupuesto = entry_presupuesto.get().strip()
        sms = var_sms.get()
        mail = var_mail.get()
        cupon = var_cupon.get()
        wa = var_wa.get()
        wa_ratio = var_wa_ratio.get().strip()
        
        return presupuesto, sms, mail, cupon, wa, wa_ratio

    def submit_businesscase(self, entry_presupuesto, var_sms, var_mail, var_cupon, var_wa, var_wa_ratio):
        # Limpia los campos de BC
        presupuesto, sms, mail, cupon, wa, wa_ratio = self.clear_entries_bc(entry_presupuesto, var_sms, var_mail, var_cupon, var_wa, var_wa_ratio)

        if self.validate_entries_bc(presupuesto, var_sms, var_mail, var_cupon, var_wa, var_wa_ratio):
            # Preguntar si ya existe la tabla PRODUCTOS
            if not self.mon.validate_if_table_exists('#PRODUCTOS'):
                messagebox.showwarning("Advertencia", "Por favor ingrese productos antes de generar BusinessCase.")
                return
            
            # Preguntar si ya existe la tabla PO
            if not self.mon.validate_if_table_exists('#PO'):
                messagebox.showwarning("Advertencia", "Por favor Genere los Públicos Objetivos antes de generar BusinessCase.")
                return

            # Verificar si se generó TX_MEDIO
            if self.mon.validate_if_table_exists('#TX_MEDIO'):
                override = messagebox.askyesno("Advertencia", "Ya hay datos para BC, ¿Desea sobreescribirlos?")
            else:
                override = None

            # Print para verificar que los datos se están pasando correctamente
            # print(presupuesto, sms, mail, cupon, wa, wa_ratio)

            # Extraer datos para el BusinessCase
            self.mon.generar_datos_bc(override=override)
            self.map_title_to_dataframe('BusinessCase', type='show')

    def generar_bc(self):
        # Crear layout para el BusinessCase
        self.menu_frame.pack_forget()
        self.clear_content_frame()

        # Crear un frame izquierdo y derecho para los botones
        tk.Label(self.content_frame, text="BusinessCase", font=("Arial", 14, "bold")).pack(pady=10)

        left_frame = tk.Frame(self.content_frame)
        right_frame = tk.Frame(self.content_frame)
        left_frame.pack(side=tk.LEFT, padx=10, pady=10)
        right_frame.pack(side=tk.RIGHT, padx=10, pady=10)

        # Label para Presupuesto, valor numérico
        tk.Label(left_frame, text="Presupuesto", font=("Arial", 10, "bold")).pack(pady=5)
        entry_presupuesto = tk.Entry(left_frame)
        entry_presupuesto.pack()

        # Entrada para para el porcentaje de WA
        tk.Label(left_frame, text="Porcentaje para WA", font=("Arial", 10, "bold")).pack(pady=5)
        var_wa_ratio = tk.Entry(left_frame)
        var_wa_ratio.pack()

        # Label para Canal
        tk.Label(left_frame, text="Canal", font=("Arial", 10, "bold")).pack(pady=5)
        # Casilla de verificación para canal SMS, Mail, Cupón y WA
        var_sms = tk.IntVar()
        var_mail = tk.IntVar()
        var_cupon = tk.IntVar()
        var_wa = tk.IntVar()
        tk.Checkbutton(left_frame, text="SMS", variable=var_sms).pack(side=tk.LEFT, padx=5)
        tk.Checkbutton(left_frame, text="Mail", variable=var_mail).pack(side=tk.LEFT, padx=5)
        tk.Checkbutton(left_frame, text="Cupón", variable=var_cupon).pack(side=tk.LEFT, padx=5)
        tk.Checkbutton(left_frame, text="WA", variable=var_wa).pack(side=tk.LEFT, padx=5)

        tk.Button(right_frame, text="Calcular Datos para BC", command=lambda: self.submit_businesscase(entry_presupuesto, var_sms, var_mail, var_cupon, var_wa, var_wa_ratio)).pack(pady=10)
        tk.Button(right_frame, text="Regresar al Menú", command=self.show_menu).pack(pady=5, side=tk.BOTTOM)

    # Función para validar entradas de listas
    def validate_entries_po_envios(self, entry_condicion, entry_excluir):
        # validar condicion es tipo numerico
        if entry_condicion.get().strip() and not entry_condicion.get().strip().isdigit():
            messagebox.showwarning("Advertencia", "Por favor ingrese un valor numérico para Condición de Compra.")
            return False
        # validar que excluir numerico separadas por coma
        if entry_excluir.get().strip() and not all(x.isdigit() for x in entry_excluir.get().strip().split(',')):
            messagebox.showwarning("Advertencia", "Por favor ingrese listas numéricas separadas por coma.")
            return False        
        return True
    
    def validate_entries_filtros(self, entries_canales:dict):
        entries = entries_canales.values()

        if not any([var.get() for var in entries]):
            messagebox.showwarning("Advertencia", "Por favor seleccione al menos un canal.")
            return False

        for entry in entries:
            if entry.get().strip() and not entry.get().strip().isdigit():
                messagebox.showwarning("Advertencia", "Por favor ingrese un valor numérico.")
                return False
        return True

    def submit_po_envios(self, entry_condicion, entry_excluir):
        # Validar las entradas
        if self.validate_entries_po_envios(entry_condicion, entry_excluir):
            # Limpiar los campos de listas
            condicion = entry_condicion.get().strip()
            excluir = self.add_quotes(entry_excluir.get().replace(', ', ','))

            # Preguntar si ya existe la tabla PRODUCTOS
            if not self.mon.validate_if_table_exists('#PRODUCTOS'):
                messagebox.showwarning("Advertencia", "Por favor ingrese productos antes de generar Listas de Envío.")
                return
            
            # Preguntar si ya existe la tabla PO
            if not self.mon.validate_if_table_exists('#PO'):
                messagebox.showwarning("Advertencia", "Por favor Genere los Públicos Objetivos antes de generar Listas de Envío.")
                return

            # Preguntar si ya existe la tabla PO envíos
            if self.mon.validate_if_table_exists('#PO_ENVIOS'):
                override = messagebox.askyesno("Advertencia", "Ya se ha generado un Público Objetivo para Envíos, ¿Desea sobreescribirlo?")
            else:
                override = None

            self.mon.generar_po_envios(condicion=condicion, excluir=excluir, override=override)
            messagebox.showinfo("Información", "Públicos Objetivos de Envíos generados exitosamente.")

    def submit_po_filtros(self, var_venta_antes, var_venta_camp, var_cond_antes, var_cond_camp):
        # Todas las entradas son opcionales
        venta_antes = var_venta_antes.get()
        venta_camp = var_venta_camp.get()
        cond_antes = var_cond_antes.get()
        cond_camp = var_cond_camp.get()

        # Preguntar si ya existe la tabla PO
        if not self.mon.validate_if_table_exists('#PO'):
            messagebox.showwarning("Advertencia", "Por favor Genere los Públicos Objetivos antes de generar Listas de Envío.")
            return
        
        # Preguntar si ya existe la tabla PO envíos, si no, generarla con los filtros cada vez
        if not self.mon.validate_if_table_exists('#PO_ENVIOS'):
            messagebox.showwarning("Advertencia", "No hay Públicos Objetivo de Envíos generados.")
        else:
            self.mon.generar_po_envios_conteo(venta_antes=venta_antes, venta_camp=venta_camp, cond_antes=cond_antes, cond_camp=cond_camp)
            self.show_dataframe(self.mon.po.df_po_conteo, f"Conteo de Clientes (Venta antes: {venta_antes} - Venta en: {venta_camp} - Condición antes: {cond_antes} - Condición en: {cond_camp}")

    def submit_canales(self, entries_canales:dict, var_grupo_control):
        # Validar las entradas de canales
        if self.validate_entries_filtros(entries_canales):
            # Limpiar los campos de canales
            canales = {canal: int(entry.get().strip()) if entry.get().strip() else 0 for canal, entry in entries_canales.items()}
            grupo_control = var_grupo_control.get()
            
            # Preguntar si ya existe la tabla PO
            if not self.mon.validate_if_table_exists('#PO'):
                messagebox.showwarning("Advertencia", "Por favor Genere los Públicos Objetivos antes de generar Listas de Envío.")
                return

            # Preguntar si ya existe la tabla PO envíos
            if not self.mon.validate_if_table_exists('#PO_ENVIOS'):
                messagebox.showwarning("Advertencia", "No hay Públicos Objetivo de Envíos generados.")
            else:
                self.mon.generar_listas_envio(canales=canales, grupo_control=grupo_control)
                self.show_dataframe(self.mon.po.df_listas_envio, "Listas de Envío")

    def generar_listas(self):
        # Crear layout para listas de envío
        self.menu_frame.pack_forget()
        self.clear_content_frame()

        # Crear un Frame para los botones
        frame = tk.Frame(self.content_frame)
        frame.pack()

        # Titulo de la sección
        tk.Label(frame, text="Listas de Envío", font=("Arial", 14, "bold")).grid(row=0, column=0, columnspan=10, pady=10)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=1, column=0, columnspan=3, pady=5, sticky="we")

        # Generar el úblico objetivo de envíos
        tk.Label(frame, text="1. Generar Público Objetivo de Envíos", font=("Arial", 12, "bold"), wraplength=150).grid(row=2, column=0, rowspan=3, pady=5, padx=10, sticky='w')
        # Entrada para Definir las condición de compra
        tk.Label(frame, text="Condición de compra", font=("Arial", 10, "bold"), anchor='w').grid(row=2, column=1, pady=5, padx=5, sticky='w')
        entry_condicion = tk.Entry(frame)
        entry_condicion.grid(row=2, column=2, pady=5, padx=5, sticky='w')
        # Entrada para excluir listas de envío
        tk.Label(frame, text="Excluir listas de envío", font=("Arial", 10, "bold"), anchor='w').grid(row=3, column=1, pady=5, padx=5, sticky='w')
        entry_excluir = tk.Entry(frame)
        entry_excluir.grid(row=3, column=2, pady=5, padx=5, sticky='w')

        # Botón para generar listas de envío con monto de condición de compra y listas a excluir
        tk.Button(frame, text="Actualizar", command=lambda: self.submit_po_envios(entry_condicion, entry_excluir)).grid(row=4, column=2, pady=10)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=5, column=0, columnspan=3, pady=5, sticky="we")

        # Entrada para seleccionar los filtros de las listas: venta antes de campaña, venta en campaña, cumple condición antes de campaña, cumple condición en campaña
        tk.Label(frame, text="2. Filtros de la campaña", font=("Arial", 12, "bold"), wraplength=150).grid(row=6, column=0, rowspan=7, pady=5, padx=10)
        
        var_venta_antes = tk.StringVar()
        var_venta_actual = tk.StringVar()
        var_cond_antes = tk.StringVar()
        var_cond_actual = tk.StringVar()
        
        options = ["", "Si", "No"]
        
        # Label para Venta
        tk.Label(frame, text='Venta', font=("Arial", 10, "bold")).grid(row=6, column=1, columnspan=2, pady=5, padx=5)
        # Entrada para seleccionar si compra antes de campaña
        tk.Label(frame, text="Antes de campaña:", font=("Arial", 10), anchor='w').grid(row=7, column=1, pady=5, padx=5, sticky='w')
        ttk.Combobox(frame, textvariable=var_venta_antes, values=options, state='readonly').grid(row=7, column=2, pady=5, padx=5, sticky='w')
        # Entrada para seleccionar si compra en campaña
        tk.Label(frame, text="En campaña:", font=("Arial", 10), anchor='w').grid(row=8, column=1, pady=5, padx=5, sticky='w')
        ttk.Combobox(frame, textvariable=var_venta_actual, values=options, state='readonly').grid(row=8, column=2, pady=5, padx=5, sticky='w')
        
        # Label para Condición
        tk.Label(frame, text="Cumple condición", font=("Arial", 10, "bold")).grid(row=9, column=1, columnspan=2, pady=5, padx=5)
        # Entrada para seleccionar si cumple condición antes de campaña
        tk.Label(frame, text="Antes de campaña", font=("Arial", 10), anchor='w').grid(row=10, column=1, pady=5, padx=5, sticky='w')
        ttk.Combobox(frame, textvariable=var_cond_antes, values=options, state='readonly').grid(row=10, column=2, pady=5, padx=5, sticky='w')
        # Entrada para seleccionar si cumple condición en campaña
        tk.Label(frame, text="En campaña", font=("Arial", 10), anchor='w').grid(row=11, column=1, pady=5, padx=5, sticky='w')
        ttk.Combobox(frame, textvariable=var_cond_actual, values=options, state='readonly').grid(row=11, column=2, pady=5, padx=5, sticky='w')

        # Boton para ver conteo máximo de envíos con los filtros seleccionados
        tk.Button(frame, text="Ver Conteo", command=lambda: self.submit_po_filtros(var_venta_antes, var_venta_actual, var_cond_antes, var_cond_actual)).grid(row=12, column=2, pady=10)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=13, column=0, columnspan=3, pady=5, sticky="we")

        # Segundo frame para los botones
        frame_2 = tk.Frame(self.content_frame)
        frame_2.pack()

        # Label para Extraer Lista de envío
        tk.Label(frame_2, text="3. Generar Lista de Envío", font=("Arial", 12, "bold"), wraplength=150).grid(row=0, column=0, rowspan=10, pady=5, padx=10)
        
        # Diccionario para almacenar las entradas
        entries_canal = {}

        # Etiquetas de encabezado
        headers = ["Canal", "Fid", "Rec", "Cap"]
        for col, header in enumerate(headers, start=1):
            label = tk.Label(frame_2, text=header, font=("Arial", 10, "bold"), width=10)
            label.grid(row=1, column=col, padx=5, pady=10)

        # Filas de datos
        canales = ["SMS", "Mail", "SMS&Mail"]
        for row, canal in enumerate(canales, start=2):
            label = tk.Label(frame_2, text=canal, borderwidth=1, width=10)
            label.grid(row=row, column=1, padx=5, pady=5)
            for col, header in enumerate(headers[1:], start=2):
                entry_name = f"entry_{header.lower()}_{canal.lower()}"
                entry = tk.Entry(frame_2, width=10)
                entry.grid(row=row, column=col, padx=4, pady=5)
                entries_canal[entry_name] = entry
        
        # entries_canal

        # Entrada para seleccionar si se quiere Grupo Control
        var_grupo_control = tk.IntVar()
        tk.Checkbutton(frame_2, text="Grupo Control?", variable=var_grupo_control).grid(row=6, column=1, padx=10, pady=5)

        # Botón para generar listas de envío con canales seleccionados
        tk.Button(frame_2, text="Generar Listas", command=lambda: self.submit_canales(entries_canal, var_grupo_control)).grid(row=6, column=4, pady=5, padx=10)

        tk.Button(frame_2, text="Regresar al Menú", command=self.show_menu).grid(row=100, column=2, pady=10)

    def validate_entries_rad(self, inicio, termino, nombre):
        if not nombre:
            messagebox.showwarning("Advertencia", "Por favor ingrese un Nombre.")
            return False
    
        # Validar que las fechas de inicio y término sean correctas
        try:
            pd.to_datetime(inicio)
            pd.to_datetime(termino)
        except:
            messagebox.showwarning("Advertencia", "Por favor ingrese fechas en formato YYYY-MM-DD.")
            return False
        return True

    def submit_datos_rad(self, entry_inicio, entry_termino, entry_nombre):
        # Extraer los datos de las entradas
        inicio = entry_inicio.get().strip()
        termino = entry_termino.get().strip()
        nombre = entry_nombre.get().strip()

        if self.validate_entries_rad(inicio, termino, nombre):
            # Preguntar si ya existe la tabla PRODUCTOS
            if not self.mon.validate_if_table_exists('#PRODUCTOS'):
                messagebox.showwarning("Advertencia", "Por favor ingrese productos antes de generar Radiografía.")
                return

            # Preguntar si ya existe la radiografía con el mismo nombre y mes
            if self.mon.rad.validate_if_rad_exists(self.mon, inicio, termino, nombre):
                override = messagebox.askyesno("Advertencia", "Ya hay Radiografía generada, ¿Desea sobreescribirla?")
            else:
                override = None

            self.mon.generar_datos_rad(inicio=inicio, termino=termino, nombre=nombre, override=override)
            # Mostrar el mensaje de éxito
            messagebox.showinfo("Información", "Radiografía generada exitosamente.")
            # self.show_dataframe(self.mon.rad.df_rad, "Radiografía")

    def rad_existentes(self):
        def seleccion_radiografia(event):
            seleccion = combo.get()
            resultado_list = resultados.get(seleccion, ["No hay resultados para esta opción."])
            # Limpiar la Listbox
            list_box.delete(0, tk.END)
            
            # Insertar nuevos resultados
            for resultado in resultado_list:
                list_box.insert(tk.END, resultado)

        # Crear un Frame para los botones
        frame = tk.Tk()
        frame.title("Radiografías existentes")

        # Extraer datos
        resultados, proveedores = self.mon.rad.select_radiografias(self.mon)

        # Label radiografias creadas anteriormente
        tk.Label(frame, text="Provedor", font=("Arial", 10, "bold")).grid(row=0, column=0, pady=5, padx=10)
        
        # Crear y posicionar la lista desplegable (Combobox)
        combo = ttk.Combobox(frame, values=proveedores)
        combo.grid(row=1, column=0, pady=5, padx=10, sticky='n')
        combo.bind("<<ComboboxSelected>>", seleccion_radiografia)

        # Crear y posicionar la Listbox
        tk.Label(frame, text="Radiografías existentes", font=("Arial", 10, "bold")).grid(row=0, column=1, pady=5, padx=10, sticky='w')
        # Crear y posicionar la Listbox
        list_box = tk.Listbox(frame, width=50, height=10)
        list_box.grid(row=1, column=1, padx=10, pady=5)

    def generar_rad(self):
        # Crear layout para listas de envío
        self.menu_frame.pack_forget()
        self.clear_content_frame()

        # Crear un Frame para los botones
        frame = tk.Frame(self.content_frame)
        frame.pack(padx=10, pady=10)

        # Titulo de la sección
        tk.Label(frame, text="Radiografía", font=("Arial", 14, "bold")).grid(row=0, column=0, columnspan=2, pady=10)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=1, column=0, columnspan=2, pady=5, sticky="we")

        # Boton para ver radiografias existentes
        tk.Button(frame, text="Ver Radiografías existentes", command=self.rad_existentes).grid(row=3, column=0, columnspan=2, pady=10)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=4, column=0, columnspan=2, pady=5, sticky="we")

        # Label para Generar Radiografía
        tk.Label(frame, text="Generar Radiografía", font=("Arial", 12, "bold")).grid(row=5, column=0, columnspan=2, pady=5, padx=10)

        # Ingresar nombre de la Radiografía

        tk.Label(frame, text="Nombre", font=("Arial", 10, "bold")).grid(row=6, column=0, pady=5, padx=10, sticky='w')
        entry_nombre = tk.Entry(frame)
        entry_nombre.grid(row=6, column=1, pady=5, padx=10)

        # Seleccionar fechas para la radiografía
        tk.Label(frame, text="Periodo de la radiografía", font=("Arial", 10, "bold")).grid(row=7, column=0, columnspan=2, pady=5, padx=10)

        # Entrada para Definir las fechas de inicio y término
        # Periodo de la campaña
        tk.Label(frame, text="Inicio:").grid(row=8, column=0, pady=5, padx=5, sticky='w')
        entry_inicio = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_inicio.grid(row=8, column=1, pady=5, padx=5, sticky='w')
        
        tk.Label(frame, text="Termino:").grid(row=9, column=0, pady=5, padx=5, sticky='w')
        entry_termino = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_termino.grid(row=9, column=1, pady=5, padx=5, sticky='w')

        # Botón para generar listas de envío con canales seleccionados
        tk.Button(frame, text="Generar Radiografía", command=lambda: self.submit_datos_rad(entry_inicio, entry_termino, entry_nombre)).grid(row=10, column=0, columnspan=2, pady=5, padx=10)

        tk.Button(frame, text="Regresar al Menú", command=self.show_menu).grid(row=100, column=0, columnspan=2, pady=10)

    def generar_resultados(self):
        pass

# root = tk.Tk()
# app = App(root)
# root.mainloop()
