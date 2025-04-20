# Importar librerías necesarias
from tkinter import ttk, filedialog, messagebox
from tkinter.simpledialog import askstring
from tkcalendar import DateEntry
from datetime import datetime
from pandastable import Table
from PIL import ImageTk
from PIL import Image
import tkinter as tk
import pandas as pd
from util.functions.path import get_file_path, get_neighbor_path, icon_cogno_str, functions_str, data_str, logo_cogno_str
from util.constants.gui import title_gui_str
from util.functions.publicos_objetivo import *
from util.functions.monetizacion import Monetizacion

import warnings
# Ignore SQLAlchemy warnings
warnings.filterwarnings('ignore')


class App:
    def __init__(self, root):
        self.mon = Monetizacion()
        self.clases = ''
        self.subclases = ''
        self.prod_types = ''
        self.root = root
        self.root.title(title_gui_str)
        self.set_icon()
        self.create_main_layout()
        self.create_menu()
        self.root.resizable(0, 0)
        self.__set_bc_options()

    def __set_bc_options(self):
        self.bc_options_familia = ''
        self.bc_options_nse = ''
        self.bc_options_tiendas = ''

    # Query para obtener los datos entre comillas simples y separados por coma
    @staticmethod
    def add_quotes(text):
        lis = []
        for item in text.split(','):
            item = f"'{item}'"
            lis.append(item)
        return (',').join(lis) if text else ''

    def set_icon(self):

        path = get_file_path(
            icon_cogno_str,
            dir_path=get_neighbor_path(__file__, functions_str, data_str)
        )

        # Cargar imagen
        img = Image.open(path)
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
        
        path = get_file_path(
            logo_cogno_str,
            dir_path=get_neighbor_path(__file__, functions_str, data_str)
        )

        self.display_image(path)  # Display the placeholder image

    def end_program(self):
        # super().close()
        self.mon.close()
        self.root.quit()

    def create_menu(self):

        tk.Label(self.menu_frame, text="Menú Principal", font=("Arial", 14, "bold")).pack(pady=10)

        buttons = [
            ("1. Ingresar Productos", self.ingresar_productos),
            ("2. BusinessCase", self.analisis_bc),#lambda : print("BusinessCase")),
            ("3. Públicos Objetivos", self.generar_publicos_objetivos),
            ("4. Listas de envío", self.generar_listas),
            ("5. Radiografía", self.generar_rad),
            ("6. Radiografía Corta", self.generar_rad_corta),
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
        self.display_image(
                get_file_path(
                    logo_cogno_str,
                    dir_path=get_neighbor_path(__file__, functions_str, data_str)
            )
        )  # Display the placeholder image again

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
        # def generar_productos():
        #     self.mon.generar_productos(skus=skus, marcas=marcas, proveedores=proveedores, clases=self.clases, subclases=self.subclases, prod_type_desc=self.prod_types, override=override)
            # self.close_loading_window()

        if self.validate_entries_productos(entry_skus, entry_marcas, entry_proveedores):
            # Extraer los datos de los campos de productos
            skus = entry_skus.get().strip().replace(', ', ',')
            marcas = self.add_quotes(entry_marcas.get().strip().replace(', ', ','))
            proveedores = self.add_quotes(entry_proveedores.get().strip().replace(', ', ','))

            df = self.mon.get_df_categorias(skus=skus, marcas=marcas, proveedores=proveedores)

            # Preguntar si se desea filtrar los productos
            op = messagebox.askyesno("Advertencia", "¿Desea filtrar por Categorías?")

            if op:
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
            # self.show_dataframe(self.mon.get_productos_agg(), "Productos")

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

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=6, column=0, columnspan=2, pady=5, sticky="we")

        # Verificar si hay productos ingresados, si es así, botón para ver productos
        if not self.mon.df_productos.empty:
            # Buton para ver productos ya ingresados
            tk.Button(frame, text="Ver Productos", command=lambda: self.show_dataframe(self.mon.get_productos_agg(), 'Productos')).grid(row=7, column=0, columnspan=2, pady=10, sticky='we')
        
        # Ingresar productos
        tk.Button(frame, text="Ingresar", command=lambda: self.submit_productos(entry_skus, entry_marcas, entry_proveedores)).grid(row=8, column=0, pady=10)

        # Botón para ingresar productos
        tk.Button(frame, text="Regresar al Menú", command=self.show_menu).grid(row=8, column=1, pady=10)

    # Función para validar los campos de PO
    def validate_entries_po(self, entry_tiendas, entry_excluir, entry_is_online, entry_condicion, entry_inicio, entry_termino):

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
            
        # validar que excluir numerico separadas por coma
        if entry_excluir.get().strip() and not all(x.isdigit() for x in entry_excluir.get().strip().split(',')):
            messagebox.showwarning("Advertencia", "Por favor ingrese listas numéricas separadas por coma.")
            return False        

        return True
    
    # Funcion para limpiar los campos de PO
    def clear_entries_po(self, entry_tiendas, entry_excluir, entry_is_online, entry_condicion, entry_inicio, entry_termino):
        tiendas = self.add_quotes(entry_tiendas.get().replace(', ', ','))
        excluir = self.add_quotes(entry_excluir.get().replace(', ', ','))
        is_online = entry_is_online.get()
        condicion = entry_condicion.get()
        inicio = entry_inicio.get()
        termino = entry_termino.get()
        
        return tiendas, excluir, is_online, condicion, inicio, termino

    # Función para agregar POs a DB
    def submit_publicos(self, entry_tiendas, entry_excluir, var, entry_condicion, entry_inicio, entry_termino):
        if self.validate_entries_po(entry_tiendas=entry_tiendas, entry_excluir=entry_excluir, entry_is_online=var, entry_condicion=entry_condicion, entry_inicio=entry_inicio, entry_termino=entry_termino):
            # Limpiar los campos de PO
            tiendas, excluir, is_online, condicion, inicio, termino = self.clear_entries_po(entry_tiendas, entry_excluir, var, entry_condicion, entry_inicio, entry_termino)

            # Preguntar si ya existe la tabla PRODUCTOS
            if not self.mon.validate_if_table_exists('#PRODUCTOS'):
                messagebox.showwarning("Advertencia", "Por favor ingrese productos antes de generar Públicos Objetivos.")
                return
            
            # Preguntar si la tabla PO ya existe
            if self.mon.validate_if_table_exists('#PO'):
                override = messagebox.askyesno("Advertencia", "Ya hay Públicos Objetivos generados, ¿Desea sobreescribirlos?")
            else:
                override = None
            
            self.mon.generar_po(tiendas=tiendas, excluir=excluir, is_online=is_online, condicion=condicion, inicio=inicio, termino=termino, override=override)
            self.show_dataframe(self.mon.po.df_pos_agg, "Públicos Objetivos")

    def generar_publicos_objetivos(self):
        self.menu_frame.pack_forget()
        self.clear_content_frame()
        
        # Crear un frame
        frame = tk.Frame(self.content_frame)
        frame.pack(pady=10, padx=10)

        tk.Label(frame, text="Públicos Objetivos", font=("Arial", 14, "bold")).grid(row=0, column=0, columnspan=2, pady=10)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=1, column=0, columnspan=2, pady=5, sticky="we")

        # Periodo de la campaña
        tk.Label(frame, text="Periodo del PO", font=("Arial", 10, "bold")).grid(row=2, column=0, pady=10)
        tk.Label(frame, text="Inicio (mes):").grid(row=3, column=0, pady=5, padx=5)
        entry_inicio = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_inicio.grid(row=4, column=0, pady=5, padx=5)
        
        tk.Label(frame, text="Termino (mes):").grid(row=3, column=1, pady=5, padx=5)
        entry_termino = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_termino.grid(row=4, column=1, pady=5, padx=5)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=5, column=0, columnspan=2, pady=5, sticky="we")

        # Datos de la campaña
        tk.Label(frame, text="Filtros (opcional)", font=("Arial", 10, "bold")).grid(row=6, column=0, columnspan=2, pady=10)
        tk.Label(frame, text="Tiendas (store_code):").grid(row=7, column=0, pady=5, padx=5, sticky='e')
        entry_tiendas = tk.Entry(frame)
        entry_tiendas.grid(row=7, column=1, pady=5, padx=5)
        
        # Entrada para excluir listas de envío
        tk.Label(frame, text="Excluir listas de envío:").grid(row=8, column=0, pady=5, padx=5, sticky='e')
        entry_excluir = tk.Entry(frame)
        entry_excluir.grid(row=8, column=1, pady=5, padx=5, sticky='w')

        tk.Label(frame, text="Condición de Compra:").grid(row=9, column=0, pady=5, padx=5, sticky='e')
        entry_condicion = tk.Entry(frame)
        entry_condicion.grid(row=9, column=1, pady=5, padx=5)
        
        var = tk.IntVar()
        entry_is_online = tk.Checkbutton(frame, text="Venta Online?", variable=var)
        entry_is_online.grid(row=10, column=0, columnspan=2)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=11, column=0, columnspan=2, pady=5, sticky="we")
        
        tk.Button(frame, text="Calcular PO", command=lambda: self.submit_publicos(entry_tiendas, entry_excluir, var, entry_condicion, entry_inicio, entry_termino)).grid(row=12, column=0, pady=5)
        tk.Button(frame, text="Regresar al Menú", command=self.show_menu).grid(row=12, column=1, columnspan=2, pady=5)

    def ver_guardar_datos(self):
        self.menu_frame.pack_forget()
        self.clear_content_frame()

        # Crear un frame para los botones
        frame = tk.Frame(self.content_frame)
        frame.pack(pady=10, padx=10)
        
        # Label de Titulo
        tk.Label(frame, text="Ver/Guardar Datos generados", font=("Arial", 12, "bold")).grid(row=0, column=0, columnspan=2, pady=10)
        
        options = ['Productos', 'Públicos Objetivos', 'BusinessCase', 'Analisis de BC', 'Listas']
        for row, option in enumerate(options, start=1):
            tk.Button(frame, width=30, text=f"Ver {option}", command=lambda opt=option: self.get_dataframe(opt, type='show')).grid(row=row, column=0, pady=5, padx=5)
            tk.Button(frame, width=30, text=f"Guardar {option}", command=lambda opt=option: self.get_dataframe(opt, type='save')).grid(row=row, column=1, pady=5, padx=5)
        
        # Botón para regresar al menú
        tk.Button(frame, text="Regresar al Menú", command=self.show_menu).grid(row=100, column=0, columnspan=2, pady=10)

    def get_dataframe(self, title, type=None):
        # Extraer datos de listas
        if self.mon.po.dict_listas_envios:
            lis_df_listas = list(self.mon.po.dict_listas_envios.values())
            lis_title_listas = list(self.mon.po.dict_listas_envios.keys())
        else:
            lis_df_listas = pd.DataFrame()
            lis_title_listas = 'Listas de Envío'

        # Extraer datos de Analisis de Business Case
        if self.mon.po.dict_df_analisis_bc:
            lis_df_bc = list(self.mon.po.dict_df_analisis_bc.values())
            lis_title_bc = list(self.mon.po.dict_df_analisis_bc.keys())
        else:
            lis_df_bc = pd.DataFrame()
            lis_title_bc = 'Analisis de BC'
        
        # Dictionary to map the dataframe
        dic = {
            'Productos': (self.mon.df_productos, 'Productos'),
            'Públicos Objetivos': (self.mon.po.df_pos_agg, 'Públicos Objetivos'),
            'Analisis de BC': (self.mon.po.df_analisis_bc, 'Analisis de BC'),
            # 'Analisis de BC': (lis_df_bc, lis_title_bc),
            'BusinessCase': (self.mon.po.df_bc, 'BusinessCase'),
            'Listas': (lis_df_listas, lis_title_listas),
        }

        lis_dataframe = dic[title][0]
        lis_title = dic[title][1]

        if not isinstance(lis_dataframe, list):
            lis_dataframe = [lis_dataframe]
            lis_title = [lis_title]

        for dataframe, title in zip(lis_dataframe, lis_title):
            print(dataframe, title)
            if dataframe.empty:
                messagebox.showwarning("Advertencia", f"No hay {title} ingresados. Por favor genere los datos en la sección correspondiente.")
            else:
                if type == 'save':
                    self.save_dataframe(dataframe, title)
                elif type == 'show':
                    self.show_dataframe(dataframe, title)

    def show_dataframe(self, dataframe, title):
        top = tk.Toplevel(self.root)
        top.title(title)
        
        # Definir el tamaño de la ventana a 800x600
        top.geometry("800x600")

        frame = tk.Frame(top)
        frame.pack(fill='both', expand=True)

        table = Table(frame, dataframe=dataframe, showtoolbar=False, showstatusbar=False)
        table.show()

    def save_dataframe(self, dataframe, title):
        file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")], initialfile=title+'.csv')
        if file_path:
            dataframe.to_csv(file_path, index=False)
            messagebox.showinfo("Información", title + " guardado exitosamente.")

    def save_location(self, title):
        file_path = filedialog.asksaveasfilename(filetypes=[("All files", "*.*")], defaultextension=".html", initialfile=title)
        if file_path:
            return file_path

    def show_analisis_bc(self):
        # Verificar si ya se generó el análisis
        tabla = '#BC'
        if not self.mon.validate_if_table_exists(tabla):
            messagebox.showinfo("Información", f"Por favor genere el Análisis antes de visualizarlo.")
            return

        df_analisis_bc, dict_df_analisis_bc = self.mon.obtener_analisis_bc()

        # Preguntar al usuario la ubicación para guardar el archivo
        file_path = self.save_location('Analisis_BC')
        self.mon.guardar_reporte_analisis_bc(df_analisis_bc, file_path)

        # Mensaje de éxito
        messagebox.showinfo("Información", "Reporte de Análisis de Business Case guardado exitosamente.")

    def show_bc(self):
        # Verificar si ya se generó el análisis
        tabla = '#BC'
        if not self.mon.validate_if_table_exists(tabla):
            messagebox.showinfo("Información", f"Por favor genere el Business Case antes de visualizarlo.")
            return

        df_bc = self.mon.obtener_bc()

        print(df_bc)

    def validate_entries_bc(self, nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion):
        # Validar que las fechas sean en formato YYYY-MM-DD
        try:
            inicio_campana = pd.to_datetime(inicio_campana.strip())
            fin_campana = pd.to_datetime(fin_campana.strip())
            inicio_analisis = pd.to_datetime(inicio_analisis.strip())
            fin_analisis = pd.to_datetime(fin_analisis.strip())
        except:
            messagebox.showwarning("Advertencia", "Por favor ingrese fechas en formato YYYY-MM-DD.")
            return False
        
        # Validar que el nombre no esté vacío
        if not nombre.strip():
            messagebox.showwarning("Advertencia", "Por favor ingrese un nombre para el Análisis.")
            return False
        
        # Validar que la condición sea numérica
        if condicion.strip() and not condicion.strip().isdigit():
            messagebox.showwarning("Advertencia", "Por favor ingrese un valor numérico para Condición de Compra.")
            return False

        return True

    def select_analisis_agg(self, nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion, elegible, familia, nse, tiendas):
        # Botón para confirmar la selección
        def confirmar_seleccion():
            # obtener los agrupados seleccionados
            lis_tablas_seleccionadas = listbox_agrupados.get(0, tk.END)
            # print(lis_tablas_seleccionadas)
        
            # lis_tablas_seleccionadas = [tabla for tabla, var in var_tablas.items() if var.get()]
            frame.destroy()

            # Preguntar si desea guardar los cambios
            op = messagebox.askyesno("Advertencia", f"¿Está seguro que desea ejecutar el Analisis?")
            if op:
                # Actualizar los resultados de la campaña
                self.mon.generar_analisis_bc(nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion, elegible, familia, nse, tiendas, lis_tablas_seleccionadas)
                
                # Mensaje de éxito
                messagebox.showinfo("Información", "Analisis generado exitosamente.")

        # Botón para agregar el agrupado
        def agregar_agrupado():
            agrupado = entry_agrupado.get().strip()
            if agrupado:
                listbox_agrupados.insert(tk.END, agrupado)
                entry_agrupado.delete(0, tk.END)

        # Botón para quitar el agrupado seleccionado
        def quitar_agrupado():
            try:
                selected_items = listbox_agrupados.curselection()
                for index in reversed(selected_items):
                    listbox_agrupados.delete(index)
            except:
                pass

        lis_agg = self.mon.obtener_lista_opciones_agg_analisis()
            
        # Mostrar ventana emergente para introducir los agrupados especiales
        frame = tk.Toplevel(self.root)
        frame.resizable(0, 0)

        label_titulo = tk.Label(frame, text="Selección de Agrupados", font=("Arial", 12, "bold"))
        label_titulo.grid(row=0, column=0, pady=10, padx=10, columnspan=5)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=1, column=0, pady=5, padx=10, sticky="we", columnspan=5)

        # Linea vertical para separar las columnas 1 de 2 y 3
        separator_v = tk.Frame(frame, width=2, bd=1, relief="sunken")
        separator_v.grid(row=1, column=1, pady=5, padx=10, sticky="ns", rowspan=len(lis_agg) + 3)

        # Crear una columna con las opciones para el agrupado
        tk.Label(frame, text="Opciones de Agrupado", font=("Arial", 10, "bold")).grid(row=2, column=0, pady=1, padx=10, sticky='w')
        for i, tabla in enumerate(lis_agg, start=3):
            tk.Label(frame, text="• "+tabla).grid(row=i, column=0, pady=1, padx=5, sticky='w')

        # Segunda columna con el espacio para ingresar el agrupado y debajo el cuadro donde se agregarán y mostrarán los agrupados
        tk.Label(frame, text="Ingresar Agrupados:").grid(row=2, column=2, pady=1, padx=5, sticky='w')
        entry_agrupado = tk.Entry(frame, width=25)
        entry_agrupado.grid(row=2, column=3, pady=1, padx=5, sticky='w', columnspan=2)

        # Botón para agregar el agrupado
        tk.Button(frame, text="Agregar", command=agregar_agrupado, width=8).grid(row=3, column=3, pady=1, padx=5)
        tk.Button(frame, text="Quitar", command=quitar_agrupado, width=8).grid(row=3, column=4, pady=1, padx=5)

        # Listbox para mostrar los agrupados
        listbox_agrupados = tk.Listbox(frame, width=25, height=15, selectmode=tk.EXTENDED)
        listbox_agrupados.grid(row=4, column=2, pady=1, padx=5, sticky='we', columnspan=3, rowspan=len(lis_agg) - 1)

        # Linea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=len(lis_agg) + 3, column=0, pady=5, padx=10, sticky="we", columnspan=5)

        button_confirmar = tk.Button(frame, text="Confirmar", command=confirmar_seleccion)
        button_confirmar.grid(row=len(lis_agg) + 4, column=2, pady=10)

    def submit_datos_bc(self, entry_nombre_bc, entry_inicio_campana, entry_fin_campana, entry_inicio_analisis, entry_fin_analisis, entry_condicion, entry_elegible):
        # Extraer los campos de BC
        nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion, elegible = entry_nombre_bc.get(), entry_inicio_campana.get(), entry_fin_campana.get(), entry_inicio_analisis.get(), entry_fin_analisis.get(), entry_condicion.get(), entry_elegible.get()

        if self.validate_entries_bc(nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion):
            familia = self.add_quotes(self.bc_options_familia.replace(', ', ','))
            nse = self.add_quotes(self.bc_options_nse.replace(', ', ','))
            tiendas = self.add_quotes(self.bc_options_tiendas.replace(', ', ','))
            
            # Preguntar si ya existe la tabla PRODUCTOS
            if not self.mon.validate_if_table_exists('#PRODUCTOS'):
                messagebox.showwarning("Advertencia", "Por favor ingrese productos antes de generar el Analisis.")
                return
            
            # Verificar si se generó ya la tabla
            override = None
            tabla = '#ANALISIS_BC'
            if self.mon.validate_if_table_exists(tabla):
                override = messagebox.askyesno("Advertencia", f"Ya hay un analisis generado, ¿Desea sobreescribirlo?")

            # Preguntar si se desea generar un agrupado especial
            res = messagebox.askyesno("Advertencia", "¿Desea seleccionar un agrupado especial?")
            
            # Preguntar los agrupados que se desean ver
            # Extraer datos para el BC
            if override or override is None:
                if res:
                    self.select_analisis_agg(nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion, elegible, familia, nse, tiendas)
                else:
                    self.mon.generar_analisis_bc(nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion, elegible, familia, nse, tiendas, lis_agg=None)
                    # Mensaje de éxito
                    messagebox.showinfo("Información", "Analisis generado exitosamente.")
            else:
                return

    # Función para mostrar el analisis de BusinessCase
    def analisis_bc(self):
        # Crear layout para el Análisis de Business Case
        self.menu_frame.pack_forget()
        self.clear_content_frame()

        # Cear un frame para la sección
        frame = tk.Frame(self.content_frame)
        frame.pack(pady=10, padx=10)

        # Label
        tk.Label(frame, text="Análisis y Business Case", font=("Arial", 14, "bold")).grid(row=0, column=0, columnspan=4, pady=10)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=1, column=0, columnspan=4, pady=2, sticky="we")

        # Línea vertical
        separator = tk.Frame(frame, width=2, bd=1, relief="sunken")
        separator.grid(row=1, column=2, rowspan=10, pady=2, padx=5, sticky="ns")

        # Ingresar fecha de inicio y fecha termino de campaña
        tk.Label(frame, text="Datos para Analisis", font=("Arial", 10, "bold")).grid(row=2, column=0, columnspan=2, pady=2)
        # Ingresar Nombre del Analisis
        tk.Label(frame, text="Nombre Analisis:").grid(row=3, column=0, pady=2, sticky='e')
        entry_nombre_bc = tk.Entry(frame, width=25)
        entry_nombre_bc.grid(row=3, column=1, pady=2)
        # Fechas
        tk.Label(frame, text="Inicio de Campaña:").grid(row=4, column=0, pady=2, sticky='e')
        tk.Label(frame, text="Fin de Campaña:").grid(row=5, column=0, pady=2, sticky='e')
        entry_inicio_campana = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_fin_campana = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_inicio_campana.grid(row=4, column=1, pady=2)
        entry_fin_campana.grid(row=5, column=1, pady=2)

        # Ingresar fecha del inicio del análisis
        tk.Label(frame, text="Inicio del Análisis:").grid(row=6, column=0, pady=2, sticky='e')
        entry_inicio_analisis = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_inicio_analisis.grid(row=6, column=1, pady=2)
        
        # Ingresar fecha del fin del análisis
        tk.Label(frame, text="Fin del Análisis:").grid(row=7, column=0, pady=5, sticky='e')
        entry_fin_analisis = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_fin_analisis.grid(row=7, column=1, pady=2)

        # Ingresar Condición de Compra
        tk.Label(frame, text="Condición de Compra:").grid(row=8, column=0, pady=2)
        entry_condicion = tk.Entry(frame, width=15)
        entry_condicion.grid(row=8, column=1, pady=2)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=9, column=0, columnspan=3, pady=2, padx=5, sticky="we")

        # Botón para generar el análisis
        tk.Button(frame, width=14, text="Generar Analisis", command=lambda: self.submit_datos_bc(entry_nombre_bc, entry_inicio_campana, entry_fin_campana, entry_inicio_analisis, entry_fin_analisis, entry_condicion, var_solo_elegible)).grid(row=10, column=0, pady=2)

        # Botón para ver los resultados del Analisis
        tk.Button(frame, width=14, text="Guardar Analisis", command=self.show_analisis_bc).grid(row=10, column=1, pady=2)

        # Botón para generar el BC
        # tk.Button(frame, width=14, text="Generar BC", command=lambda: self.submit_datos_bc('bc', entry_nombre_bc, entry_inicio_campana, entry_fin_campana, entry_inicio_analisis, entry_fin_analisis, entry_condicion, var_solo_elegible)).grid(row=5, column=3, pady=5)

        # Boton para ver análisis de BC
        tk.Button(frame, width=14, text="Calcular BC", command=self.show_bc).grid(row=8, column=3, pady=2)

        # Bot
        tk.Button(frame, width=14, text="Regresar al Menú", command=self.show_menu).grid(row=10, column=3, pady=2)

        # Filtros
        tk.Label(frame, text="Filtros", font=("Arial", 10, "bold")).grid(row=2, column=3, pady=2)

        # Variable para solo cumple condición de compra
        var_solo_elegible = tk.IntVar()
        tk.Checkbutton(frame, text="Solo Elegible?", variable=var_solo_elegible).grid(row=3, column=3, pady=2)

        # Filtros de entrada para tiendas, NSE y Familia
        tk.Button(frame, text="Tiendas", command=self.select_bc_tiendas, width=10).grid(row=4, column=3, pady=2)
        tk.Button(frame, text="NSE", command=self.select_bc_nse, width=10).grid(row=5, column=3, pady=2)
        tk.Button(frame, text="Familia", command=self.select_bc_familia, width=10).grid(row=6, column=3, pady=2)

    # Validar los campos para el BusinessCase
    def validate_entries_rad_corta(self, nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion):
        # Si hay condicion, verificar que sea numérica
        if bool(condicion) and not condicion.isdigit():
            messagebox.showwarning("Advertencia", "Por favor ingrese un valor numérico para Condición de Compra.")
            return False
        # Validar que las fechas sean correctas
        try:
            inicio_campana = pd.to_datetime(inicio_campana)
            fin_campana = pd.to_datetime(fin_campana)
            inicio_analisis = pd.to_datetime(inicio_analisis)
            fin_analisis = pd.to_datetime(fin_analisis)
        except:
            messagebox.showwarning("Advertencia", "Por favor ingrese fechas en formato YYYY-MM-DD.")
            return False
        # Validar que nombre no esté vacío
        if not bool(nombre):
            messagebox.showwarning("Advertencia", "Por favor ingrese un nombre para la Radiografía.")
            return False
        return True
        
    
    def submit_rad_corta(self, entry_nombre_rad, entry_inicio_campana, entry_fin_campana, entry_inicio_analisis, entry_fin_analisis, entry_condicion, entry_online):
        # Extraer los campos de BC
        nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion, online = entry_nombre_rad.get(), entry_inicio_campana.get(), entry_fin_campana.get(), entry_inicio_analisis.get(), entry_fin_analisis.get(), entry_condicion.get(), entry_online.get()

        if self.validate_entries_rad_corta(nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion):
            # Preguntar si ya existe la tabla PRODUCTOS
            if not self.mon.validate_if_table_exists('#PRODUCTOS'):
                messagebox.showwarning("Advertencia", "Por favor ingrese productos antes de generar BusinessCase.")
                return
            
            # Verificar si se generó #BC
            override = None
            if self.mon.validate_if_table_exists('#RAD_CORTA'):
                override = messagebox.askyesno("Advertencia", "Ya hay datos de Radiografía Corta, ¿Desea sobreescribirlos?")
            
            # Extraer datos para el Radiografía Corta
            self.mon.generar_datos_rad_corta(nombre, inicio_campana, fin_campana, inicio_analisis, fin_analisis, condicion, online, override)

            # Mensaje de éxito
            messagebox.showinfo("Información", "Radiografía Corta generada exitosamente.")
            # self.get_dataframe('Radiografía Corta', type='show')

    def generar_rad_corta(self):
        # Crear layout para el Radiografía Corta
        self.menu_frame.pack_forget()
        self.clear_content_frame()

        # Cear un frame para la sección
        frame = tk.Frame(self.content_frame)
        frame.pack(pady=10, padx=10)

        # Label
        tk.Label(frame, text="Radiografía Corta", font=("Arial", 14, "bold")).grid(row=0, column=0, columnspan=4, pady=10)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=1, column=0, columnspan=4, pady=5, sticky="we")

        # Línea vertical
        separator = tk.Frame(frame, width=2, bd=1, relief="sunken")
        separator.grid(row=1, column=2, rowspan=9, pady=5, padx=10, sticky="ns")

        # Ingresar fecha de inicio y fecha termino de campaña
        tk.Label(frame, text="Datos para Radiografía", font=("Arial", 10, "bold")).grid(row=2, column=0, columnspan=2, pady=10)
        # Ingresar Nombre de la Campaña
        tk.Label(frame, text="Nombre Radiografía:").grid(row=3, column=0, pady=10, sticky='e')
        entry_nombre_rad = tk.Entry(frame, width=25)
        entry_nombre_rad.grid(row=3, column=1, pady=5)
        # Fechas
        tk.Label(frame, text="Mes Inicio de Campaña:").grid(row=4, column=0, pady=5, sticky='e')
        tk.Label(frame, text="Mes Fin de Campaña:").grid(row=5, column=0, pady=5, sticky='e')
        entry_inicio_campana = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_fin_campana = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_inicio_campana.grid(row=4, column=1, pady=5)
        entry_fin_campana.grid(row=5, column=1, pady=5)

        # Ingresar fecha del inicio del análisis
        tk.Label(frame, text="Mes Inicio del Análisis:").grid(row=6, column=0, pady=10, sticky='e')
        entry_inicio_analisis = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_inicio_analisis.grid(row=6, column=1, pady=5)
        
        # Ingresar fecha del fin del análisis
        tk.Label(frame, text="Mes Fin del Análisis:").grid(row=7, column=0, pady=10, sticky='e')
        entry_fin_analisis = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_fin_analisis.grid(row=7, column=1, pady=5)

        # Ingresar Condición de Compra
        tk.Label(frame, text="Condición de Compra:").grid(row=8, column=0, pady=10)
        entry_condicion = tk.Entry(frame, width=15)
        entry_condicion.grid(row=8, column=1, pady=5)

        # Ingresar si la campaña es online
        var_online = tk.IntVar()
        tk.Label(frame, text="Venta Online?").grid(row=4, column=3)
        tk.Checkbutton(frame, variable=var_online).grid(row=5, column=3)

        # Label para Presupuesto, valor numérico
        tk.Button(frame, width=14, text="Generar Datos", command=lambda: self.submit_rad_corta(entry_nombre_rad, entry_inicio_campana, entry_fin_campana, entry_inicio_analisis, entry_fin_analisis, entry_condicion, var_online)).grid(row=2, column=3, pady=5)

        # Botón para guardar el BusinessCase
        # tk.Button(frame, width=14, text="Ver BC", command=show_bc).grid(row=3, column=3, pady=5)

        # Boton para ver análisis de BC
        # tk.Button(frame, width=14, text="Ver Análisis", command=self.analisis_bc).grid(row=4, column=3, pady=5)

        # Bot
        tk.Button(frame, width=14, text="Regresar al Menú", command=self.show_menu).grid(row=9, column=3, pady=5)

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

    # def submit_po_envios(self, entry_condicion, entry_excluir):
    #     # Validar las entradas
    #     if self.validate_entries_po_envios(entry_condicion, entry_excluir):
    #         # Limpiar los campos de listas
    #         condicion = entry_condicion.get().strip()
    #         excluir = self.add_quotes(entry_excluir.get().replace(', ', ','))

    #         # Preguntar si ya existe la tabla PRODUCTOS
    #         if not self.mon.validate_if_table_exists('#PRODUCTOS'):
    #             messagebox.showwarning("Advertencia", "Por favor ingrese productos antes de generar Listas de Envío.")
    #             return
            
    #         # Preguntar si ya existe la tabla PO
    #         if not self.mon.validate_if_table_exists('#PO'):
    #             messagebox.showwarning("Advertencia", "Por favor Genere los Públicos Objetivos antes de generar Listas de Envío.")
    #             return

    #         # Preguntar si ya existe la tabla PO envíos
    #         if self.mon.validate_if_table_exists('#PO_ENVIOS'):
    #             override = messagebox.askyesno("Advertencia", "Ya se ha generado un Público Objetivo para Envíos, ¿Desea sobreescribirlo?")
    #         else:
    #             override = None

    #         self.mon.generar_po_envios(condicion=condicion, excluir=excluir, override=override)
    #         messagebox.showinfo("Información", "Públicos Objetivos de Envíos generados exitosamente.")

    def submit_filtros_listas(self, var_venta_antes, var_venta_camp, var_cond_antes, var_cond_camp, var_online):
        # Todas las entradas son opcionales
        venta_antes = var_venta_antes.get()
        venta_camp = var_venta_camp.get()
        cond_antes = var_cond_antes.get()
        cond_camp = var_cond_camp.get()
        online = var_online.get()

        # Preguntar si ya existe la tabla PO
        if not self.mon.validate_if_table_exists('#PO'):
            messagebox.showwarning("Advertencia", "Por favor Genere los Públicos Objetivos antes de generar Listas de Envío.")
            return
        
        self.mon.generar_po_envios_conteo(venta_antes=venta_antes, venta_camp=venta_camp, cond_antes=cond_antes, cond_camp=cond_camp, online=online)
        messagebox.showinfo("Información", f"Públicos Objetivos de Envíos generados exitosamente:\n\nFiltros aplicados:\nVenta antes: {venta_antes}\nVenta en: {venta_camp}\nCondición antes: {cond_antes}\nCondición en: {cond_camp}\nOnline: {online}")
        self.show_dataframe(self.mon.po.df_po_conteo, 'Conteo para Listas')

    def submit_canales(self, entries_canales:dict, var_grupo_control, var_prioridad_online):
        # Validar las entradas de canales
        if self.validate_entries_filtros(entries_canales):
            # Limpiar los campos de canales
            canales = {canal: int(entry.get().strip()) if entry.get().strip() else 0 for canal, entry in entries_canales.items()}
            grupo_control = var_grupo_control.get()
            prioridad_online = var_prioridad_online.get()
            
            # Preguntar si ya existe la tabla PO
            if not self.mon.validate_if_table_exists('#PO'):
                messagebox.showwarning("Advertencia", "Por favor Genere los Públicos Objetivos antes de generar Listas de Envío.")
                return
            
            self.mon.generar_listas_envio(canales=canales, grupo_control=grupo_control, prioridad_online=prioridad_online)
            
            # Preguntar si desea separar las listas de envío
            op = messagebox.askyesno("Información", "Listas de Envío generadas exitosamente.\n¿Desea separarlas por segmento?")
            if op:
                self.mon.separar_listas_envio()

            self.get_dataframe('Listas', type='show')

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

        # Entrada para seleccionar los filtros de las listas: venta antes de campaña, venta en campaña, cumple condición antes de campaña, cumple condición en campaña
        tk.Label(frame, text="1. Filtros de la Lista", font=("Arial", 12, "bold"), wraplength=150).grid(row=6, column=0, rowspan=7, pady=5, padx=10)
        
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

        # Casilla para filtrar solo Online
        var_online = tk.IntVar()
        tk.Checkbutton(frame, text="Solo Online?", variable=var_online).grid(row=12, column=1, padx=10, pady=5)

        # Boton para ver conteo máximo de envíos con los filtros seleccionados
        tk.Button(frame, text="Actualizar Conteo", command=lambda: self.submit_filtros_listas(var_venta_antes, var_venta_actual, var_cond_antes, var_cond_actual, var_online)).grid(row=12, column=2, pady=10)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=13, column=0, columnspan=3, pady=5, sticky="we")

        # Segundo frame para los botones
        frame_2 = tk.Frame(self.content_frame)
        frame_2.pack()

        # Label para Extraer Lista de envío
        tk.Label(frame_2, text="2. Generar Lista de Envío", font=("Arial", 12, "bold"), wraplength=150).grid(row=0, column=0, rowspan=10, pady=5, padx=10)
        
        # Diccionario para almacenar las entradas
        entries_canal = {}

        # Etiquetas de encabezado
        headers = ["Canal", "Fid", "Rec", "Cap"]
        for col, header in enumerate(headers, start=1):
            label = tk.Label(frame_2, text=header, font=("Arial", 10, "bold"))
            label.grid(row=1, column=col, padx=0, pady=10)

        # Filas de datos
        canales = ["01 SMS", "02 MAIL", "03 MAIL & SMS"]
        for row, canal in enumerate(canales, start=2):
            label = tk.Label(frame_2, text=canal)
            label.grid(row=row, column=1, padx=5, pady=5)
            for col, header in enumerate(headers[1:], start=2):
                entry_name = f"entry_{header.lower()}_{canal.lower()}"
                entry = tk.Entry(frame_2, width=14)
                entry.grid(row=row, column=col, padx=5, pady=5)
                entries_canal[entry_name] = entry
        
        # entries_canal

        # Entrada para seleccionar si se quiere Grupo Control
        var_grupo_control = tk.IntVar()
        tk.Checkbutton(frame_2, text="Grupo Control", variable=var_grupo_control).grid(row=6, column=1, padx=10, pady=5)

        # Casilla para dar prioridad a Online
        var_prioridad_online = tk.IntVar()
        tk.Checkbutton(frame_2, text="Prioridad Online", variable=var_prioridad_online).grid(row=6, column=2, padx=10, pady=5)

        # Botón para generar listas de envío con canales seleccionados
        tk.Button(frame_2, text="Generar Listas", command=lambda: self.submit_canales(entries_canal, var_grupo_control, var_prioridad_online)).grid(row=6, column=3, columnspan=2, pady=5, padx=10)

        # Línea horizontal
        separator = tk.Frame(frame_2, height=2, bd=1, relief="sunken")
        separator.grid(row=7, column=0, columnspan=5, pady=5, sticky="we")

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

    def submit_datos_rad(self, entry_inicio, entry_termino, entry_nombre, entry_online):
        # 1. Función para confirmar selección
        # 2. Validar las entradas
        # 3. Obtener lista de opciones de tablas a confirmar
        # 4. Generar la ventana de selección
        # 5. Ejecutar la función de confirmar_seleccion

        # 1. Función para confirmar la selección
        def confirmar_seleccion():
            lis_tablas_seleccionadas = [tabla for tabla, var in var_tablas.items() if var.get()]
            frame.destroy()

            # Preguntar si desea guardar los cambios
            op = messagebox.askyesno("Advertencia", f"¿Está seguro que desea generar la Radiografía seleccionada?")
            if op:
                
                # Preguntar si ya existe la radiografía con el mismo nombre y mes
                if self.mon.rad.validate_if_rad_exists(self.mon, inicio, termino, nombre):
                    override = messagebox.askyesno("Advertencia", "Ya hay Radiografía generada, ¿Desea sobreescribirla?")
                else:
                    override = None

                self.mon.generar_datos_rad(inicio=inicio, termino=termino, nombre=nombre, online=online, override=override, lis_seleccion=lis_tablas_seleccionadas)
                # Mostrar el mensaje de éxito
                messagebox.showinfo("Información", "Radiografía generada exitosamente.")
                # self.show_dataframe(self.mon.rad.df_rad, "Radiografía")

        # 2. Validar las entradas del usuario
        # Extraer los datos de las entradas
        inicio, termino, nombre, online = entry_inicio.get().strip(), entry_termino.get().strip(), entry_nombre.get().strip(), entry_online.get()

        if self.validate_entries_rad(inicio, termino, nombre):
            # Preguntar si ya existe la tabla PRODUCTOS
            if not self.mon.validate_if_table_exists('#PRODUCTOS'):
                messagebox.showwarning("Advertencia", "Por favor ingrese productos antes de generar Radiografía.")
                return
        else:
            return
        
        # 3. Obtener la lista de opciones de tablas a confirmar
        lis_tablas = self.mon.obtener_lista_opciones('Radiografia Completa')

        # 4. Generar la ventana de selección
        # Mostrar ventana emergente para seleccionar las tablas de resultados
        frame = tk.Toplevel(self.root)
        frame.resizable(0, 0)

        label_titulo = tk.Label(frame, text="Selección de Tablas", font=("Arial", 12, "bold"))
        label_titulo.grid(row=0, column=0, pady=10, padx=10)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=1, column=0, pady=5, padx=10, sticky="we")

        # Crear una casilla de verificación por cada tabla de resultados
        var_tablas = {}
        for i, tabla in enumerate(lis_tablas, start=2):
            var_tablas[tabla] = tk.IntVar()
            check = tk.Checkbutton(frame, text=tabla, variable=var_tablas[tabla])
            check.grid(row=i, column=0, pady=5, padx=5, sticky='w')
        
        # 5. Ejecutar la función de confirmar_seleccion
        button_confirmar = tk.Button(frame, text="Confirmar", command=confirmar_seleccion)
        button_confirmar.grid(row=len(lis_tablas) + 2, column=0, pady=10)

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

        # Label para Generar Radiografía
        tk.Label(frame, text="Generar Radiografía", font=("Arial", 12, "bold")).grid(row=2, column=0, columnspan=2, pady=5, padx=10)

        # Ingresar nombre de la Radiografía
        tk.Label(frame, text="Nombre", font=("Arial", 10, "bold")).grid(row=3, column=0, pady=5, padx=10, sticky='w')
        entry_nombre = tk.Entry(frame)
        entry_nombre.grid(row=3, column=1, pady=5, padx=10)

        # Seleccionar fechas para la radiografía
        tk.Label(frame, text="Periodo de la radiografía", font=("Arial", 10, "bold")).grid(row=4, column=0, columnspan=2, pady=5, padx=10)

        # Entrada para Definir las fechas de inicio y término
        # Periodo de la campaña
        tk.Label(frame, text="Inicio:").grid(row=5, column=0, pady=5, padx=5, sticky='w')
        entry_inicio = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_inicio.grid(row=5, column=1, pady=5, padx=5, sticky='w')
        
        tk.Label(frame, text="Termino:").grid(row=6, column=0, pady=5, padx=5, sticky='w')
        entry_termino = DateEntry(frame, date_pattern='yyyy-mm-dd')
        entry_termino.grid(row=6, column=1, pady=5, padx=5, sticky='w')

        # Preguntar solo venta online?
        entry_online = tk.IntVar()
        tk.Checkbutton(frame, text="Venta Online?", variable=entry_online).grid(row=7, column=1, columnspan=2, pady=5, padx=10, sticky='w')

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=8, column=0, columnspan=2, pady=5, sticky="we")

        # Boton para ver radiografias existentes
        tk.Button(frame, text="Ver Radiografías existentes", command=self.rad_existentes).grid(row=9, column=0, columnspan=2, pady=10, sticky='we')

        # Botón para generar listas de envío con canales seleccionados
        tk.Button(frame, text="Generar Radiografía", command=lambda: self.submit_datos_rad(entry_inicio, entry_termino, entry_nombre, entry_online)).grid(row=10, column=0, pady=5, padx=10)

        tk.Button(frame, text="Regresar al Menú", command=self.show_menu).grid(row=10, column=1, pady=10)

    def validate_entry_campana(self, list_box):
        try:
            seleccion = list_box.get(list_box.curselection())
            return seleccion
        except:
            messagebox.showwarning("Advertencia", "Por favor seleccione una campaña.")
            return None

    def show_edit_campaign(self, list_box, type='show_edit'):
        def edit_row(tree, row_id, headers):
            current_values = tree.item(row_id, "values")
            
            edit_window = tk.Toplevel()
            edit_window.title("Editar Fila")
            
            entries = []
            
            for i, value in enumerate(current_values):
                tk.Label(edit_window, text=headers[i]).grid(row=i, column=0, padx=5, pady=5)
                entry = tk.Entry(edit_window)
                # Dehabilitar la edicion para el primer campo
                entry.grid(row=i, column=1, padx=5, pady=5)
                entry.insert(0, value)
                
                # Deshabilitar la edición para los campos de codigo_campana y nombre
                if headers[i] in ('codigo_campana', 'nombre'):
                    entry.config(state='disabled')
                
                # Deshabilitar la edición para los campos de las tablas producto y tienda
                if headers[i] in ('region', 'state', 'formato_tienda', 'tienda', 'store_key', 'proveedor', 'marca', 'class_code', 'class_desc', 'subclass_code', 'subclass_desc', 'prod_type_desc', 'product_description'):
                    entry.config(state='disabled')

                entries.append(entry)
            
            def save_changes():
                new_values = [entry.get() for entry in entries]
                tree.item(row_id, values=new_values)
                edit_window.destroy()
            
            save_button = tk.Button(edit_window, text="Guardar Cambios", command=save_changes)
            save_button.grid(row=len(entries), column=0, columnspan=2, pady=10)

        def delete_row(tree):
            selected_item = tree.selection()
            if selected_item:
                tree.delete(selected_item)

        def add_row(tree, headers):
            add_window = tk.Toplevel()
            add_window.title("Agregar Nueva Fila")
            
            entries = []
            
            for i, header in enumerate(headers):
                tk.Label(add_window, text=header).grid(row=i, column=0, padx=5, pady=5)
                entry = tk.Entry(add_window)
                entry.grid(row=i, column=1, padx=5, pady=5)
                
                if header == 'codigo_campana':
                    # Crear un nuevo codigo de campaña. Si es de tipo 'add', el codigo de campaña es el nombre de la campaña sustituyendo los espacios por guiones bajos
                    codigo_campana = nombre_campana.replace(' ', '_').upper()
                    entry.insert(0, codigo_campana)
                    entry.config(state='disabled')
                
                if header == 'nombre':
                    # Si es de tipo 'add', el nombre de la campaña es el nombre ingresado por el usuario
                    entry.insert(0, nombre_campana)
                    entry.config(state='disabled')

                if header in ('region', 'state', 'formato_tienda', 'tienda', 'store_key', 'proveedor', 'marca', 'class_code', 'class_desc', 'subclass_code', 'subclass_desc', 'prod_type_desc', 'product_description'):
                    entry.config(state='disabled')
                
                entries.append(entry)

            def save_new_row():
                new_values = [entry.get() for entry in entries]
                tree.insert("", "end", values=new_values)
                add_window.destroy()
            
            save_button = tk.Button(add_window, text="Agregar Fila", command=save_new_row)
            save_button.grid(row=len(entries), column=0, columnspan=2, pady=10)

        def on_double_click(event, headers):
            item = event.widget.identify('item', event.x, event.y)
            if item:
                edit_row(event.widget, item, headers)

        def create_table(frame, df):
            headers = df.columns
            tree = ttk.Treeview(frame, columns=list(range(1, len(headers) + 1)), show="headings", height=8)
            
            for i, header in enumerate(headers, 1):
                tree.heading(i, text=header)
                tree.column(i, width=100)
            
            for row in df.itertuples(index=False):
                tree.insert("", "end", values=row)
            
            vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
            hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
            tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
            
            tree.pack(side="left", fill="both", expand=True)
            vsb.pack(side="right", fill="y")
            hsb.pack(side="bottom", fill="x")
            
            tree.bind("<Double-1>", lambda event: on_double_click(event, headers))
            
            return tree

        def extract_tree_data(tree, headers):
            rows = tree.get_children()
            data = []
            for row in rows:
                data.append(tree.item(row)['values'])
            return pd.DataFrame(data, columns=headers)

        def compare_dataframes(df_original, df_updated):
            # Return True if there are differences, False otherwise
            # Revisar cuál df es más largo
            if len(df_original) < len(df_updated):
                df_original, df_updated = df_updated, df_original
            
            # Reindexar df_updated para que coincida con df_original
            df_updated = df_updated.reindex_like(df_original)

            comparison = df_original.compare(df_updated)
            if comparison.empty:
                print("No hay diferencias.")
            else:
                print("Cambios detectados:")
                print(comparison)
            
            return comparison

        def cargar_lista(tree, df, title, tipo):
            print(f'Entramos a cargar lista {title}')
            # Obtener el nombre de la campaña, si es de tipo 'add', el nombre de la campaña es el nombre ingresado por el usuario reemplazando los espacios por guiones bajos
            codigo_campana = nombre_campana.replace(' ', '_').upper() if df['codigo_campana'].isnull().all() else df['codigo_campana'].iloc[0]
            print(f"Cargando lista de {title} para la campaña: {nombre_campana}")
            
            lista = None

            # Si tipo es 'total_cadena_tiendas', cargar todas las tiendas
            if tipo == 'total_cadena_tiendas':
                # Obtener todas las tiendas en formato string de 4 digitos con ceros a la izquierda separadas por coma
                lista = self.mon.obtener_total_cadena_tiendas()
            # Si tipo es 'productos_ingresados', cargar los productos ingresados por el usuario
            if tipo == 'productos_ingresados':
                # Obtener los productos del df_productos en formato string separadas por coma
                lista = self.mon.obtener_lista_productos()
            if tipo == 'cargar_lista':
                # Habilitar entrada para ingresar lista de tiendas o productos
                lista = askstring(f"Lista de {title}", f"Ingrese la lista de {title} separada por coma.")
            
            # Validar que la lista no esté vacía y que este separada por coma o si se dio cancelar
            if not lista or not all([x.strip() for x in lista.split(',')]):
                if tipo == 'productos_ingresados':
                    messagebox.showwarning("Advertencia", "Para esta opción es necesario primero definir productos en el apartado de Productos.")
                return
            
            # Insertar la lista en el tree, solo en las columnas codigo_campana y store_code o product_code. Dejar vacias las columnas distintas
            for item in lista.split(','):
                item = item.strip()
                # Obtener columnas del df
                cols = df.columns
                # Identificar index de las columnas codigo_campana y store_code o product_code
                index_codigo_campana = cols.get_loc('codigo_campana')
                index_value = cols.get_loc('store_code') if 'store_code' in cols else cols.get_loc('product_code')
                # Crear la variable de valores a insertar en el tree, dejar vacias las columnas que no sean codigo_campana y store_code o product_code
                values = [""] * len(cols)
                values[index_codigo_campana] = codigo_campana
                values[index_value] = str(item).zfill(4)
                # Insertar en el tree solo las columnas codigo_campana y store_code
                tree.insert("", "end", values=values)

        # Validar si se seleccionó una campaña
        if type in ['show_edit']:
            nombre_campana = self.validate_entry_campana(list_box)
            if not nombre_campana:
                return
        elif type in ['add']:
            # Obtener la ultima campaña en la lista
            nombre_ultima_campana = self.mon.get_campanas().iloc[-1]['nombre']
            # Obtener el año y mes actual en formato YYMM y concatenar con espacio
            nombre_campana = f"{datetime.now().strftime('%y%m')} "
            nombre_campana = askstring("Nueva Campaña", "Ingrese el nombre de la nueva campaña", initialvalue=nombre_campana)
            if not nombre_campana:
                return

        # Crear la ventana principal
        frame = tk.Toplevel(self.root)
        # frame.state("zoomed")

        # Crear un Notebook para organizar las tablas
        notebook = ttk.Notebook(frame)
        notebook.pack(fill="both", expand=True)

        # Configurar las tablas, títulos y datos
        lis_titles = self.mon.obtener_nombres_tablas_campanas()
        # Obtener los datos de la campaña seleccionada si es de tipo 'show', si no, crear DataFrames vacíos
        lis_df = self.mon.obtener_info_campana(nombre_campana) if type in ['show_edit'] else [pd.DataFrame(columns=df.columns) for df in self.mon.obtener_info_campana(nombre_ultima_campana)]
        lis_tree = []

        # Crear un tab por cada tabla
        for title, df in zip(lis_titles, lis_df):
            # Crear un tab por cada tabla
            tab = tk.Frame(notebook)
            notebook.add(tab, text=title)
            tree = create_table(tab, df)
            # Guardar el tree y el df en una lista
            lis_tree.append(tree)
            # Sustituir el dataframe con la info del tree para mantener el mismo formato y comparar cambios
            lis_df[lis_titles.index(title)] = extract_tree_data(tree, df.columns)

            # Botones para agregar y eliminar filas
            add_button = tk.Button(tab, width=15, text="Agregar Fila", command=lambda t=tree, h=df.columns: add_row(t, h))
            add_button.pack(side="top", pady=5, padx=5)

            delete_button = tk.Button(tab, width=15, text="Eliminar Fila", command=lambda t=tree: delete_row(t))
            delete_button.pack(side="top", pady=5, padx=5)

            # Agregar botones para cargar una lista completa de tiendas o productos
            if title in ('Tiendas'):
                button_listas_tiendas = tk.Button(tab, width=15, text="Cargar Lista", command=lambda t=tree, d=df, title=title: cargar_lista(t, d, title, tipo='cargar_lista'))
                button_listas_tiendas.pack(side="top", pady=5, padx=5)

                button_total_cadena = tk.Button(tab, width=15, text="Total Cadena", command=lambda t=tree, d=df, title=title: cargar_lista(t, d, title, tipo='total_cadena_tiendas'))
                button_total_cadena.pack(side="top", pady=5, padx=5)
            
            if title in ('Productos'):
                button_listas_productos = tk.Button(tab, width=15, text="Cargar Lista", command=lambda t=tree, d=df, title=title: cargar_lista(t, d, title, tipo='cargar_lista'))
                button_listas_productos.pack(side="top", pady=5, padx=5)

                button_productos_ingresados = tk.Button(tab, wraplength=60, width=15, text="Usar Productos Ingresados", command=lambda t=tree, d=df, title=title: cargar_lista(t, d, title, tipo='productos_ingresados'))
                button_productos_ingresados.pack(side="top", pady=5, padx=5)

        # Función para extraer y comparar los datos de todas las tablas
        def on_save_and_compare_all():
            for i, (tree, df) in enumerate(zip(lis_tree, lis_df)):
                table_name = lis_titles[i]
                df_new = extract_tree_data(tree, df.columns)
                # Comparar los DataFrames
                print(f"Comparando: {table_name}")
                df_comparison = compare_dataframes(df, df_new)

                if not df_comparison.empty: # and validate_df_types(df_new, table_name):
                    # Si las tablas son producto o tienda, dejar en el dataframe solo las columnas codigo_campanaa en ambas y product_code y store_code respectivamente
                    if table_name in ('Tiendas'):
                        df_new = df_new[['codigo_campana', 'store_code']]
                        # Convertir store_code a string de 4 digitos con ceros a la izquierda
                        df_new['store_code'] = df_new['store_code'].apply(lambda x: str(x).zfill(4))
                    if table_name in ('Productos'):
                        df_new = df_new[['codigo_campana', 'product_code']]
                        # Convertir product_code a string de 18 digitos con ceros a la izquierda
                        df_new['product_code'] = df_new['product_code'].apply(lambda x: str(x).zfill(18))

                    # Guardar los cambios en la campaña si hay diferencias
                    error = self.mon.guardar_info_campana(nombre_campana, table_name, df_new)
                    if bool(error):
                        # Obtener las columnas con errores a partir del df.compare() obtenido
                        cols_error = ','.join(df_comparison.loc[:, (df_comparison != "").any()].columns.get_level_values(0).unique().tolist())
                        messagebox.showwarning("Advertencia", f"Error en los datos ingresados.\n\nTabla: {table_name}\nColumnas: {cols_error}\nError: {repr(error)}")
                        return
                    else:
                        # Mostrar mensaje de éxito
                        messagebox.showinfo("Información", f"Cambios en la tabla {table_name} guardados exitosamente.")
            # Salir de la ventana
            frame.destroy()

        # Botón para guardar los cambios y comparar todas las tablas
        save_button = tk.Button(frame, text="Guardar y Salir", command=on_save_and_compare_all)
        save_button.pack(pady=10)

    def delete_campaign(self, list_box):
        nombre_campana = self.validate_entry_campana(list_box)
        if not nombre_campana:
            return

        # Preguntar si desea eliminar la campaña
        op = messagebox.askyesno("Advertencia", f"¿Está seguro que desea eliminar la campaña {nombre_campana}?")
        if op:
            self.mon.eliminar_info_campana(nombre_campana)
            messagebox.showinfo("Información", f"La campaña {nombre_campana} ha sido eliminada exitosamente.")
    
    def update_campaign(self, list_box):
        # Botón para confirmar la selección
        def confirmar_seleccion():
            lis_tablas_seleccionadas = [tabla for tabla, var in var_tablas.items() if var.get()]
            frame.destroy()

            # Preguntar si desea guardar los cambios
            op = messagebox.askyesno("Advertencia", f"¿Está seguro que desea actualizar los resultados de la campaña {nombre_campana}?")
            if op:
                # Validar que la campaña seleccionada tiene productos en la tabla MON_CAMP_PRODUCTOS
                if not campaign_products_exists(nombre_campana):
                    messagebox.showwarning("Advertencia", "No hay productos definidos para esta campaña.\nPor favor ingrese productos antes de actualizar los resultados.")
                    return
                # Actualizar los resultados de la campaña
                self.mon.actualizar_resultados_campana(nombre_campana, lis_tablas_seleccionadas)
                messagebox.showinfo("Información", f"Los resultados de la campaña {nombre_campana} han sido actualizados exitosamente.")

        def campaign_products_exists(nombre_campana):
            return self.mon.validate_campaign_products(nombre_campana)

        nombre_campana = self.validate_entry_campana(list_box)
        if not nombre_campana:
            return
        
        lis_tablas = self.mon.obtener_lista_opciones('Campañas') # REVISAR
            
        # Mostrar ventana emergente para seleccionar las tablas de resultados
        frame = tk.Toplevel(self.root)
        frame.resizable(0, 0)

        label_titulo = tk.Label(frame, text="Selección de Tablas", font=("Arial", 12, "bold"))
        label_titulo.grid(row=0, column=0, pady=10, padx=10)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=1, column=0, pady=5, padx=10, sticky="we")

        # Crear una casilla de verificación por cada tabla de resultados
        var_tablas = {}
        for i, tabla in enumerate(lis_tablas, start=2):
            var_tablas[tabla] = tk.IntVar()
            check = tk.Checkbutton(frame, text=tabla, variable=var_tablas[tabla])
            check.grid(row=i, column=0, pady=5, padx=5, sticky='w')
        
        button_confirmar = tk.Button(frame, text="Confirmar", command=confirmar_seleccion)
        button_confirmar.grid(row=len(lis_tablas) + 2, column=0, pady=10)

    def show_results(self, list_box):
        nombre_campana = self.validate_entry_campana(list_box)
        # self.mon.mostrar_resultados_campana(nombre_campana)

    def generar_resultados(self):
        # Crear layout para ventana de resultados
        self.menu_frame.pack_forget()
        self.clear_content_frame()

        campanas = self.mon.get_campanas()

        # Crear un Frame para los botones
        frame = tk.Frame(self.content_frame)
        frame.pack(padx=10, pady=10)

        # Titulo de la sección
        tk.Label(frame, text="Campañas Monetización", font=("Arial", 14, "bold")).grid(row=0, column=0, columnspan=2, pady=10)

        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=1, column=0, columnspan=2, pady=5, sticky="we")

        # Label Campañas Monetización
        tk.Label(frame, text="Campañas", font=("Arial", 10, "bold")).grid(row=2, column=0, pady=5, padx=10, sticky='w')

        # Listbox para mostrar las campañas
        list_box = tk.Listbox(frame, width=70, height=15, selectmode='single')
        list_box.grid(row=3, rowspan=6, column=0, padx=10, pady=5)
        list_box.insert(tk.END, *campanas.nombre)

        # Botones laterales para Ver, Editar, Eliminar, Crear, espacio en blanco, Actualizar, Ver Resultados y Regresar al Menú
        tk.Button(frame, text="Ver/Editar", width=12, command=lambda: self.show_edit_campaign(list_box, 'show_edit')).grid(row=3, column=1, pady=2, padx=10)
        tk.Button(frame, text="Agregar", width=12, command=lambda: self.show_edit_campaign(list_box, 'add')).grid(row=4, column=1, pady=2, padx=10)
        tk.Button(frame, text="Eliminar", width=12, command=lambda: self.delete_campaign(list_box)).grid(row=5, column=1, pady=2, padx=10)
        tk.Label(frame, text="").grid(row=6, column=1, pady=2, padx=10)
        tk.Button(frame, text="Actualizar", width=12, command=lambda: self.update_campaign(list_box), bg="navy", fg="white").grid(row=7, column=1, pady=2, padx=10)
        tk.Button(frame, text="Ver Resultados", width=12, command=lambda: self.show_results(list_box), bg="green", fg="white").grid(row=8, column=1, pady=2, padx=10)
        
        # Línea horizontal
        separator = tk.Frame(frame, height=2, bd=1, relief="sunken")
        separator.grid(row=9, column=0, columnspan=2, pady=5, sticky="we")
        
        tk.Button(frame, text="Regresar al Menú", command=self.show_menu).grid(row=10, column=0, columnspan=2, pady=5)

    def select_bc_familia(self):
        # Opciones
        options = self.mon.get_bc_options_familia()

        # Crear el dropdown personalizado
        multi_select = MultiSelectDropdown(self.root, options)
        self.root.wait_window(multi_select.top)

        # Concatenar las opciones seleccionadas separando por comas
        self.bc_options_familia = ', '.join(multi_select.selected_items)

    def select_bc_nse(self):
        # Opciones
        options = self.mon.get_bc_options_nse()

        # Crear el dropdown personalizado
        multi_select = MultiSelectDropdown(self.root, options)
        self.root.wait_window(multi_select.top)
        
        # Concatenar las opciones seleccionadas separando por comas
        self.bc_options_nse = ', '.join(multi_select.selected_items)

    def select_bc_tiendas(self):
        top = tk.Toplevel(self.root)
        # Definir el tamaño de la ventana a 800x600
        top.geometry("250x150")
        top.title("Selección de Tiendas")
        top.resizable(0, 0)

        # Crear un Frame para los botones
        frame = tk.Frame(top)
        frame.pack(fill='both', expand=True)

        # Titulo "Filtro tiendas"
        tk.Label(frame, text="Filtro Tiendas", font=("Arial", 12, "bold")).grid(row=0, column=0, columnspan=2, pady=10)
        tk.Label(frame, text="codigos de 4 dígitos separadas por coma").grid(row=1, column=0, columnspan=2, pady=5)

        # Label y Entry para filtrar tiendas
        tk.Label(frame, text="Tiendas:").grid(row=2, column=0, pady=10, padx=10, sticky='w')
        entry_tiendas = tk.Entry(frame, width=25)
        entry_tiendas.grid(row=2, column=1, pady=10, padx=10, sticky='e')

        # Botón para confirmar la selección
        def confirmar_seleccion():
            # Validar entry_tiendas son solo numeros de 4 digitos separados por coma
            tiendas = entry_tiendas.get().strip()
            if not all([x.strip().isdigit() and len(x.strip()) == 4 for x in tiendas.split(',')]):
                messagebox.showwarning("Advertencia", "Por favor ingrese tiendas en formato correcto.")
            else:
                self.bc_options_tiendas = tiendas
                top.destroy()

        # Botón para confirmar la selección
        tk.Button(frame, text="Confirmar", command=confirmar_seleccion).grid(row=3, column=0, columnspan=2, pady=10)

class MultiSelectDropdown:
    def __init__(self, root, options):
        self.root = root
        self.options = options
        self.selected_items = []

        # Seleccionar opciones
        """Abre una ventana emergente con opciones de selección múltiple."""
        self.top = tk.Toplevel(self.root)
        self.top.title("Seleccionar opciones")
        self.top.geometry("150x200")

        # Listbox con selección múltiple
        self.listbox = tk.Listbox(self.top, selectmode=tk.EXTENDED, height=len(self.options))
        for option in self.options:
            self.listbox.insert(tk.END, option)
        self.listbox.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

        # Botón para confirmar selección
        confirm_button = ttk.Button(self.top, text="Aplicar", command=self.apply_selection)
        confirm_button.pack(pady=5)

    def apply_selection(self):
        """Guarda y muestra la selección del usuario."""
        selected_indices = self.listbox.curselection()
        self.selected_items = [self.options[i] for i in selected_indices]
        self.top.destroy()  # Cierra la ventana emergente
        

