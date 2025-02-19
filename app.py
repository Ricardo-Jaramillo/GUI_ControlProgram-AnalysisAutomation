import streamlit as st
import pandas as pd
from datetime import datetime
from monetizacion import Monetizacion

# Configuración de la página
st.set_page_config(page_title="Cognodata Monetización", layout="wide")
st.markdown("""<style>
    body {
        background-color: #121212;
        color: #ffffff;
    }
    .stButton>button {
        background-color: #1E88E5;
        color: white;
    }
</style>""", unsafe_allow_html=True)

# Inicialización
mon = Monetizacion()

# Función para ingresar productos
def ingresar_productos():
    st.subheader("Ingresar Productos")
    skus = st.text_input("SKUs (separados por coma)")
    marcas = st.text_input("Marcas")
    proveedores = st.text_input("Proveedores")
    
    if st.button("Ingresar"):
        df = mon.get_df_categorias(skus=skus, marcas=marcas, proveedores=proveedores)
        st.write("Datos extraídos:", df.head())

# Función para generar Business Case
def analisis_bc():
    st.subheader("Análisis de Business Case")
    nombre = st.text_input("Nombre del Análisis")
    inicio_campana = st.date_input("Inicio de Campaña")
    fin_campana = st.date_input("Fin de Campaña")
    
    if st.button("Generar Análisis"):
        st.success("Análisis generado correctamente")

# Menú lateral
menu = st.sidebar.radio("Menú", ["Ingresar Productos", "Business Case"])
if menu == "Ingresar Productos":
    ingresar_productos()
elif menu == "Business Case":
    analisis_bc()
