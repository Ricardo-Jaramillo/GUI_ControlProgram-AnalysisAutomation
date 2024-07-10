import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
from PIL import Image
from PIL import ImageTk
# Import Libraries
from publicos_objetivo import *
import warnings

# Ignore SQLAlchemy warnings
warnings.filterwarnings('ignore')


class App:
    def __init__(self, root, name='App'):
        self.root = root
        self.root.title("Cognodata Monetización - Data Science")
        self.productos = pd.DataFrame()
        self.publicos_objetivos = pd.DataFrame()
        self.conn = PublicosObjetivo(name)

        self.set_icon("Data Science - Públicos Objetivos\images\icono_cogno_resized.png")
        self.create_main_layout()
        self.create_menu()

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
        
        self.display_image("Data Science - Públicos Objetivos\images\logo_cogno.png")

    def end_program(self):
        self.root.quit()
        self.conn.close()

    def create_menu(self):

        tk.Label(self.menu_frame, text="Menú Principal", font=("Arial", 14, "bold")).pack(pady=10)

        buttons = [
            ("1. Ingresar Productos", self.ingresar_productos),
            ("2. Generar Públicos Objetivos", self.generar_publicos_objetivos),
            ("3. Ver/Guardar Productos ingresados", self.ver_guardar_productos),
            ("4. Ver/Guardar Públicos Objetivos", self.ver_guardar_publicos),
            ("Salir", self.end_program)
        ]

        for (text, command) in buttons:
            button = tk.Button(self.menu_frame, text=text, command=command, width=30)
            button.pack(pady=5)

    def show_menu(self):
        self.clear_content_frame()
        self.menu_frame.pack(side=tk.LEFT, fill=tk.Y, padx=20, pady=20)
        self.image_label.pack(expand=True)  # Ensure the image label is packed again
        self.display_image("Data Science - Públicos Objetivos\images\logo_cogno.png")  # Display the placeholder image again

    def clear_content_frame(self):
        for widget in self.content_frame.winfo_children():
            widget.pack_forget()

    def ingresar_productos(self):
        self.menu_frame.pack_forget()
        self.clear_content_frame()
        self.content_frame.pack(padx=100)

        def validate_entries():
            skus = entry_skus.get().strip()
            marcas = entry_marcas.get().strip()
            proveedores = entry_proveedores.get().strip()
            
            if sum(bool(x) for x in [skus, marcas, proveedores]) > 1:
                messagebox.showwarning("Advertencia", "Por favor ingrese solo un campo.")
                return False
            return True
        
        # Función para agregar productos
        def submit_productos():
            if validate_entries():
                skus = entry_skus.get().replace(', ', '')
                marcas = PublicosObjetivo.add_quotes(entry_marcas.get().replace(', ', ''))
                proveedores = PublicosObjetivo.add_quotes(entry_proveedores.get().replace(', ', ''))

                # Setear variables
                self.conn.initialize_variables_products()
                self.conn.skus = skus
                self.conn.marcas = marcas
                self.conn.proveedores = proveedores
                
                # Create temporal table Productos, marca y competencia
                # print(self.conn.get_query_create_productos_temporal())
                if not self.conn.validate_table_exists('#PRODUCTOS'):
                    self.conn.create_table_productos_temporal()
                    self.conn.df_productos = self.conn.select(query=self.conn.get_query_select_productos_temporal())
                
                self.show_dataframe(self.conn.df_productos)
        
        # Crear los campos para ingresar productos
        tk.Label(self.content_frame, text="Ingresar Productos").pack(pady=10)
        tk.Label(self.content_frame, text="SKUs:").pack()
        entry_skus = tk.Entry(self.content_frame)
        entry_skus.pack()
        
        tk.Label(self.content_frame, text="Marcas:").pack()
        entry_marcas = tk.Entry(self.content_frame)
        entry_marcas.pack()
        
        tk.Label(self.content_frame, text="Proveedor(es):").pack()
        entry_proveedores = tk.Entry(self.content_frame)
        entry_proveedores.pack()
        
        # tk.Label(self.content_frame, text="¿Desea filtrar por categorías?").pack(pady=10)

        tk.Button(self.content_frame, text="Agregar", command=submit_productos).pack(pady=10)
        tk.Button(self.content_frame, text="Regresar al Menú", command=self.show_menu).pack()

    def generar_publicos_objetivos(self):
        self.menu_frame.pack_forget()
        self.clear_content_frame()

        # Función para validar los campos. VALIDAR
        def validate_entries():
            tiendas = entry_tiendas.get().strip()
            is_online = entry_is_online.get().strip()
            condicion = entry_condicion.get().strip()
            inicio = entry_inicio.get().strip()
            termino = entry_termino.get().strip()
            
            # Validar que solo se haya ingresado un campo
            # if type(is_online) == bool and type(condicion) == int:
            #     return True
            # else:
            #     messagebox.showwarning("Advertencia", "Por favor ingrese solo un campo.")
            #     return False
            return True
        
        # Función para agregar productos
        def submit_publicos():
            if validate_entries():
                tiendas = PublicosObjetivo.add_quotes(entry_tiendas.get().replace(', ', ''))
                is_online = entry_is_online.get()
                condicion = entry_condicion.get()
                inicio = entry_inicio.get()
                termino = entry_termino.get()

                # Setear variables
                self.conn.initialize_variables_pos()
                self.conn.tiendas = tiendas
                self.conn.is_online = is_online
                self.conn.condicion = condicion
                self.conn.inicio = inicio
                self.conn.termino = termino
                
                # Crear tabla temporal POs
                if not self.conn.validate_table_exists('#PO_AGG'):
                    # print(self.conn.get_query_create_pos_temporal())
                    self.conn.create_table_pos_temporal()
                    self.conn.df_pos_agg = self.conn.select(query=self.conn.get_query_select_pos_agg_temporal())
                
                self.show_dataframe(self.conn.df_pos_agg)

        tk.Label(self.content_frame, text="Públicos Objetivos", font=("Arial", 14, "bold")).pack(pady=10)
        # Periodo de la campaña
        tk.Label(self.content_frame, text="Periodo de la campaña", font=("Arial", 12, "bold")).pack(pady=10)
        tk.Label(self.content_frame, text="Inicio:").pack()
        entry_inicio = tk.Entry(self.content_frame)
        entry_inicio.pack()
        
        tk.Label(self.content_frame, text="Termino:").pack()
        entry_termino = tk.Entry(self.content_frame)
        entry_termino.pack()

        # Datos de la campaña
        tk.Label(self.content_frame, text="Filtros de la campaña", font=("Arial", 12, "bold")).pack(pady=10)
        tk.Label(self.content_frame, text="Tiendas (store_code):").pack()
        entry_tiendas = tk.Entry(self.content_frame)
        entry_tiendas.pack()
        
        tk.Label(self.content_frame, text="Venta Online:").pack()
        entry_is_online = tk.Entry(self.content_frame)
        entry_is_online.pack()
        
        tk.Label(self.content_frame, text="Condición de Compra:").pack()
        entry_condicion = tk.Entry(self.content_frame)
        entry_condicion.pack()
        
        tk.Button(self.content_frame, text="Calcular Públicos Objetivo", command=submit_publicos).pack(pady=10)
        tk.Button(self.content_frame, text="Regresar al Menú", command=self.show_menu).pack()



    def ver_guardar_productos(self):
        self.menu_frame.pack_forget()
        self.clear_content_frame()
        
        tk.Label(self.content_frame, text="Productos Ingresados").pack(pady=10)
        
        if not self.conn.df_productos.empty:
            self.show_dataframe(self.conn.df_productos)
        
        tk.Button(self.content_frame, text="Guardar archivo csv", command=self.save_productos).pack(pady=5)
        tk.Button(self.content_frame, text="Regresar al Menú", command=self.show_menu).pack()

    def ver_guardar_publicos(self):
        self.menu_frame.pack_forget()
        self.clear_content_frame()
        
        tk.Label(self.content_frame, text="Públicos Objetivos").pack(pady=10)
        
        if not self.conn.df_pos_agg.empty:
            self.show_dataframe(self.conn.df_pos_agg)
        
        tk.Button(self.content_frame, text="Guardar archivo csv", command=self.save_publicos).pack(pady=5)
        tk.Button(self.content_frame, text="Regresar al Menú", command=self.show_menu).pack()

    def show_dataframe(self, dataframe):
        top = tk.Toplevel(self.root)
        top.title("DataFrame")
        
        frame = tk.Frame(top)
        frame.pack(fill='both', expand=True)
        
        pt = ttk.Treeview(frame, show='headings')
        pt.pack(side='left', fill='both', expand=True)
        
        vsb = ttk.Scrollbar(frame, orient="vertical", command=pt.yview)
        vsb.pack(side='right', fill='y')
        pt.configure(yscrollcommand=vsb.set)
        
        pt["column"] = list(dataframe.columns)
        pt["show"] = "headings"
        
        for column in pt["columns"]:
            pt.heading(column, text=column)
            pt.column(column, width=100)
        
        rows = dataframe.to_numpy().tolist()
        for row in rows:
            pt.insert("", "end", values=row)
        
    def save_productos(self):
        if not self.conn.df_productos.empty:
            file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
            if file_path:
                self.conn.df_productos.to_csv(file_path, index=False)
                messagebox.showinfo("Información", "Productos guardados exitosamente.")
        else:
            messagebox.showwarning("Advertencia", "No hay productos para guardar.")

    def save_publicos(self):
        if not self.conn.df_pos_agg.empty:
            file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
            if file_path:
                self.conn.df_pos_agg.to_csv(file_path, index=False)
                messagebox.showinfo("Información", "Públicos objetivos guardados exitosamente.")
        else:
            messagebox.showwarning("Advertencia", "No hay públicos objetivos para guardar.")


# root = tk.Tk()
# app = App(root)
# root.mainloop()
