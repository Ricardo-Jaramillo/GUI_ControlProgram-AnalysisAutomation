import tkinter as tk
from tkinter import ttk
import pandas as pd
from tkinter import messagebox

# Datos de ejemplo
data = {
    'Categoria': ['Electrónica', 'Electrónica', 'Electrónica', 'Ropa', 'Ropa', 'Ropa', 'Hogar', 'Hogar', 'Hogar'],
    'Subcategoria': ['Teléfonos', 'Computadoras', 'Televisores', 'Hombres', 'Mujeres', 'Niños', 'Cocina', 'Baño', 'Dormitorio'],
    'Prod Type': ['iPhone', 'Laptop', 'LED', 'Camisas', 'Vestidos', 'Ropa', 'Utensilios', 'Toallas', 'Camas']
}

df = pd.DataFrame(data)
categorias = df['Categoria'].unique()
subcategorias = df['Subcategoria'].unique()
prod_types = df['Prod Type'].unique()

categoria_global = []
subcategoria_global = []
prod_type_global = []

def actualizar_categoria(event):
    categoria_seleccionada = [listbox_categoria.get(i) for i in listbox_categoria.curselection()]
    subcategoria_seleccionada = [listbox_subcategoria.get(i) for i in listbox_subcategoria.curselection()]
    prod_type_seleccionado = [listbox_prod_type.get(i) for i in listbox_prod_type.curselection()]
    print(categoria_seleccionada, subcategoria_seleccionada, prod_type_seleccionado)
    print(categoria_global, subcategoria_global, prod_type_global)

    if categoria_seleccionada:
        if not subcategoria_global:
            subcategoria_seleccionada = subcategorias
        else:
            subcategoria_seleccionada = subcategoria_global
        if not prod_type_global:
            prod_type_seleccionado = prod_types
        else:
            prod_type_seleccionado = prod_type_global

        listbox_subcategoria.delete(0, tk.END)
        listbox_subcategoria.insert(tk.END, *df[df['Categoria'].isin(categoria_seleccionada) & df['Subcategoria'].isin(subcategoria_seleccionada) & df['Prod Type'].isin(prod_type_seleccionado)]['Subcategoria'].unique())

        listbox_prod_type.delete(0, tk.END)
        listbox_prod_type.insert(tk.END, *df[df['Categoria'].isin(categoria_seleccionada) & df['Subcategoria'].isin(subcategoria_seleccionada) & df['Prod Type'].isin(prod_type_seleccionado)]['Prod Type'].unique())
    

def actualizar_subcategoria(event):
    categoria_seleccionada = [listbox_categoria.get(i) for i in listbox_categoria.curselection()]
    subcategoria_seleccionada = [listbox_subcategoria.get(i) for i in listbox_subcategoria.curselection()]
    prod_type_seleccionado = [listbox_prod_type.get(i) for i in listbox_prod_type.curselection()]
    print(categoria_seleccionada, subcategoria_seleccionada, prod_type_seleccionado)
    print(categoria_global, subcategoria_global, prod_type_global)

    if subcategoria_seleccionada:
        if not categoria_global:
            categoria_seleccionada = categorias
        else:
            categoria_seleccionada = categoria_global
        if not prod_type_global:
            prod_type_seleccionado = prod_types
        else:
            prod_type_seleccionado = prod_type_global

        listbox_categoria.delete(0, tk.END)
        listbox_categoria.insert(tk.END, *df[df['Subcategoria'].isin(subcategoria_seleccionada) & df['Categoria'].isin(categoria_seleccionada) & df['Prod Type'].isin(prod_type_seleccionado)]['Categoria'].unique())
        
        listbox_prod_type.delete(0, tk.END)
        listbox_prod_type.insert(tk.END, *df[df['Subcategoria'].isin(subcategoria_seleccionada) & df['Categoria'].isin(categoria_seleccionada) & df['Prod Type'].isin(prod_type_seleccionado)]['Prod Type'].unique())

def actualizar_prod_type(event):
    categoria_seleccionada = [listbox_categoria.get(i) for i in listbox_categoria.curselection()]
    subcategoria_seleccionada = [listbox_subcategoria.get(i) for i in listbox_subcategoria.curselection()]
    prod_type_seleccionado = [listbox_prod_type.get(i) for i in listbox_prod_type.curselection()]
    print(categoria_seleccionada, subcategoria_seleccionada, prod_type_seleccionado)
    print(categoria_global, subcategoria_global, prod_type_global)

    if prod_type_seleccionado:
        if not categoria_global:
            categoria_seleccionada = categorias
        else:
            categoria_seleccionada = categoria_global
        if not subcategoria_global:
            subcategoria_seleccionada = subcategorias
        else:
            subcategoria_seleccionada = subcategoria_global

        listbox_categoria.delete(0, tk.END)
        listbox_categoria.insert(tk.END, *df[df['Prod Type'].isin(prod_type_seleccionado) & df['Categoria'].isin(categoria_seleccionada) & df['Subcategoria'].isin(subcategoria_seleccionada)]['Categoria'].unique())
        
        listbox_subcategoria.delete(0, tk.END)
        listbox_subcategoria.insert(tk.END, *df[df['Prod Type'].isin(prod_type_seleccionado) & df['Categoria'].isin(categoria_seleccionada) & df['Subcategoria'].isin(subcategoria_seleccionada)]['Subcategoria'].unique())
        
    else:
        print("Selecciona un prod_type")

def seleccionar_categoria():
    global categoria_global
    categoria_seleccionada = [listbox_categoria.get(i) for i in listbox_categoria.curselection()]
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
        categoria_global = categoria_seleccionada

def seleccionar_subcategoria():
    global subcategoria_global
    subcategoria_seleccionada = [listbox_subcategoria.get(i) for i in listbox_subcategoria.curselection()]
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
        subcategoria_global = subcategoria_seleccionada

def seleccionar_prod_type():
    global prod_type_global
    prod_type_seleccionado = [listbox_prod_type.get(i) for i in listbox_prod_type.curselection()]
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
        prod_type_global = prod_type_seleccionado

def reiniciar_selecciones():
    global categoria_global
    global subcategoria_global
    global prod_type_global
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
    categoria_global = []
    subcategoria_global = []
    prod_type_global = []

def aplicar_filtros():
    categoria_seleccionada = [item for item in listbox_categoria.get(0, tk.END)]
    subcategoria_seleccionada = [item for item in listbox_subcategoria.get(0, tk.END)]
    prod_type_seleccionado = [item for item in listbox_prod_type.get(0, tk.END)]
    
    print(f"Categoría: {categoria_seleccionada}")
    print(f"Subcategoría: {subcategoria_seleccionada}")
    print(f"Prod Type: {prod_type_seleccionado}")

    # Mostrar en un messagebox que los filtros se aplicaron correctamente
    messagebox.showinfo("Filtros Aplicados", f"Categoría: {(', ').join(categoria_seleccionada)}\nSubcategoría: {(', ').join(subcategoria_seleccionada)}\nProd Type: {(', ').join(prod_type_seleccionado)}")

# Crear la ventana principal
ventana = tk.Tk()
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

# Iniciar el bucle de eventos
ventana.mainloop()
