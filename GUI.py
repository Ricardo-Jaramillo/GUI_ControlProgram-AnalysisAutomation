import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkcalendar import DateEntry
import pandas as pd
from PIL import Image
from PIL import ImageTk
# Import Libraries
from publicos_objetivo import *
import warnings
from connection import Conn
from monetizacion import Monetizacion

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

    # Función para agregar productos
    def submit_productos(self, entry_skus, entry_marcas, entry_proveedores):
        if self.validate_entries_productos(entry_marcas, entry_proveedores, entry_skus):
            skus = entry_skus.get().strip().replace(', ', ',')
            marcas = self.add_quotes(entry_marcas.get().strip().replace(', ', ','))
            proveedores = self.add_quotes(entry_proveedores.get().strip().replace(', ', ','))
            
            # Crear tabla temporal Productos y mostrar
            self.mon.generar_productos(skus, marcas, proveedores)
            self.show_dataframe(self.mon.df_productos)

    def ingresar_productos(self):
        self.menu_frame.pack_forget()
        self.clear_content_frame()
        self.content_frame.pack(padx=100)
        
        # Crear los campos para ingresar productos
        tk.Label(self.content_frame, text="Ingresar Productos separados por coma", font=('Arial', 10, 'bold')).pack(pady=5)
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

        tk.Button(self.content_frame, text="Agregar", command=lambda: self.submit_productos(entry_skus,entry_marcas, entry_proveedores)).pack(pady=10)
        tk.Button(self.content_frame, text="Regresar al Menú", command=self.show_menu).pack()

    def generar_publicos_objetivos(self):
        self.menu_frame.pack_forget()
        self.clear_content_frame()

        # Función para validar los campos. VALIDAR
        def validate_entries_po(entry_tiendas='', entry_is_online=0, entry_condicion=0, entry_inicio='', entry_termino=''):

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
        
        # Función para agregar productos
        def submit_publicos():
            if validate_entries_po(entry_tiendas=entry_tiendas, entry_is_online=var, entry_condicion=entry_condicion, entry_inicio=entry_inicio, entry_termino=entry_termino):
                tiendas = self.add_quotes(entry_tiendas.get().replace(', ', ','))
                is_online = var.get()
                condicion = entry_condicion.get()
                inicio = entry_inicio.get()
                termino = entry_termino.get()

                # Generar Públicos Objetivos
                self.mon.generar_po(tiendas=tiendas, is_online=is_online, condicion=condicion, inicio=inicio, termino=termino)
                self.show_dataframe(self.mon.po.df_pos_agg)

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
        
        tk.Button(self.content_frame, text="Calcular Públicos Objetivo", command=submit_publicos).pack(pady=10)
        tk.Button(self.content_frame, text="Regresar al Menú", command=self.show_menu).pack()



    def ver_guardar_productos(self):
        self.menu_frame.pack_forget()
        self.clear_content_frame()
        
        tk.Label(self.content_frame, text="Productos Ingresados").pack(pady=10)
        
        if not self.mon.df_productos.empty:
            self.show_dataframe(self.mon.df_productos)
        
        tk.Button(self.content_frame, text="Guardar archivo csv", command=self.save_productos).pack(pady=5)
        tk.Button(self.content_frame, text="Regresar al Menú", command=self.show_menu).pack()

    def ver_guardar_publicos(self):
        self.menu_frame.pack_forget()
        self.clear_content_frame()
        
        tk.Label(self.content_frame, text="Públicos Objetivos").pack(pady=10)
        
        if not self.mon.po.df_pos_agg.empty:
            self.show_dataframe(self.mon.po.df_pos_agg)
        
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
        if not self.mon.df_productos.empty:
            file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
            if file_path:
                self.mon.df_productos.to_csv(file_path, index=False)
                messagebox.showinfo("Información", "Productos guardados exitosamente.")
        else:
            messagebox.showwarning("Advertencia", "No hay productos para guardar.")

    def save_publicos(self):
        if not self.mon.po.df_pos_agg.empty:
            file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
            if file_path:
                self.mon.po.df_pos_agg.to_csv(file_path, index=False)
                messagebox.showinfo("Información", "Públicos objetivos guardados exitosamente.")
        else:
            messagebox.showwarning("Advertencia", "No hay públicos objetivos para guardar.")


# root = tk.Tk()
# app = App(root)
# root.mainloop()
