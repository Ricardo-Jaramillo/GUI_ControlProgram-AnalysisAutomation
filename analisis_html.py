# Importar librerias
import numpy as np
import matplotlib.pyplot as plt
from pandas.plotting import table
import pandas as pd
import seaborn as sns
from connection import Conn
import geopandas as gpd
from fuzzywuzzy import process
from matplotlib.ticker import FuncFormatter
from pathlib import Path
import os

class Analisis:
    def __init__(self, df=None):
        self.df = df
        self.__set_foldername('Analisis BC')
        self.__set_df_mexico()
        self.__set_theme()
    
    def __set_foldername(self, foldername):
        self.foldername = Path(foldername)

    # Función para mapear los nombres de las columnas
    def __map_col_agg(self, df):
        dict_map = {
            'mes': 'Mes',
            'region': 'Region',
            'state': 'Estado',
            'formato_tienda': 'Formato',
            'tienda': 'Tienda',
            'store_description': 'Tienda',
            'nse': 'NSE',
            'tipo_familia': 'Familia',
            'class_desc': 'Class',
            'subclass_desc': 'Subclass',
            'prod_type_desc': 'Prodtype',
            'product_description': 'Producto'
        }
        df_copy = df.copy()
        return df_copy.rename(columns=dict_map)

    # Funcion para mapear las salidas de cada variable en list_agg
    @staticmethod
    def __map_agg(df):
        dict_map = {
            'region': {
                '10 REGIÓN METROPOLITANA': 'Metropolitana',
                '20 REGIÓN ORIENTE': 'Oriente',
                '30 REGIÓN SUR': 'Sur',
                '40 REGIÓN PACÍFICO': 'Pacifico',
                '45 REGIÓN BAJÍO': 'Bajio',
                '48 REGIÓN NORESTE': 'Noreste',
                '50 REGIÓN COMERCIO ELECTRÓNICO': 'Comercio Electronico',
                '60 REGIÓN VENTAS CENTRAL': 'Ventas Central',
                '70 REGIÓN BODEGAS': 'Bodegas'
            },
            'state': {
                'Aguascalientes': 'Aguascalientes',
                'Baja California': 'Baja California',
                'Baja California Sur': 'Baja California Sur',
                'Campeche': 'Campeche',
                'Chiapas': 'Chiapas',
                'Ciudad de México': 'Ciudad de Mexico',
                'Distrito Federal': 'Ciudad de Mexico',
                'Durango': 'Durango',
                'Estado de México': 'Mexico',
                'Guanajuato': 'Guanajuato',
                'Guerrero': 'Guerrero',
                'Hidalgo': 'Hidalgo',
                'Jalisco': 'Jalisco',
                'Michoacán': 'Michoacan',
                'Morelos': 'Morelos',
                'Nayarit': 'Nayarit',
                'Nuevo León': 'Nuevo Leon',
                'Oaxaca': 'Oaxaca',
                'Puebla': 'Puebla',
                'Querétaro': 'Queretaro',
                'Quintana Roo': 'Quintana Roo',
                'San Luis Potosí': 'San Luis Potosi',
                'Sinaloa': 'Sinaloa',
                'Tabasco': 'Tabasco',
                'Tamaulipas': 'Tamaulipas',
                'Tlaxcala': 'Tlaxcala',
                'Veracruz': 'Veracruz',
                'Yucatán': 'Yucatan',
                'Zacatecas': 'Zacatecas'
            },
            'formato_tienda': {
                '01 SELECTO': 'Selecto',
                '02 AB': 'AB',
                '03 CD': 'CD',
                '04 WEB': 'Web',
                '05 SUPERCITO': 'Supercito',
            },
            'tipo_familia': {
                'FAMILIA_BEBES': '1 Bebes',
                'FAMILIA_NINOS': '2 Ninos',
                'FAMILIA_JOVENES': '3 Jovenes',
                'JOVEN/VIVO_SOLO': '4 Joven/Vive Solo',
                'PAREJA_MADURA': '5 Pareja Madura',
                'NO SEGMENTADO': '6 No Segmentado',
            },
            'nse': {
                'Alto': '1 Alto',
                'Bajo': '3 Bajo',
                'Medio': '2 Medio',
                'NO SEGMENTADO': '4 No Segmentado',
            }
        }

        df_copy = df.copy()
        
        for col in df_copy.columns:
            if col in dict_map:
                df_copy[col] = df_copy[col].map(dict_map[col]).fillna(df_copy[col])
        return df_copy

    def set_df(self, _df):
        # Mapear nombres de TABLA
        dict_tabla = {
            'MES': 'MES',
            'FORMATO': 'FORMATO',
            'ESTADO_REGION': 'ESTADO',
            'ESTADO_FORMATO_REGION_TIENDA': 'TIENDA',
            'FAMILIA': 'FAMILIA',
            'NSE': 'NSE',
            'CLASS': 'CLASS',
            'CLASS_SUBCLASS': 'SUBCLASS',
            'CLASS_PRODTYPE_SUBCLASS': 'PROD_TYPE',
            'CLASS_PRODTYPE_PRODUCTO_SUBCLASS': 'PRODUCTO'
        }
        # Mapear los valores de TABLA
        df = _df.copy().fillna(0)
        df['tabla'] = df['tabla'].map(dict_tabla).fillna(df['tabla'])

        # Mapear valores de cada columna
        df = self.__map_agg(df)

        # Mapear los nombres de las columnas
        df = self.__map_col_agg(df)

        self.df = df

    def __set_df_mexico(self):
        self.df_mexico = gpd.read_file('Analisis BC/mexicoHigh.json')

    def __split_df(self, df, columns=['MES', 'FORMATO', 'ESTADO', 'TIENDA', 'FAMILIA', 'NSE', 'CLASS', 'SUBCLASS', 'PROD_TYPE', 'PRODUCTO', 'TOTAL']):
        
        # KPIS
        columns_marca = ['venta_actual', 'clientes_actual', 'unidades_actual', 'tx_actual', 'share', '%venta_online', 'frecuencia_tx', 'tx_medio', 'precio_medio'] #10
        columns_crecimiento_marca = ['crecimiento_venta', 'crecimiento_venta_anual', 'crecimiento_share', 'crecimiento_frecuencia_tx', 'crecimiento_tx_medio', 'crecimiento_precio_medio'] #5
        columns_categoria = ['cat_venta_actual', 'cat_clientes_actual', 'cat_unidades_actual', 'cat_tx_actual', '%cat_venta_online', 'cat_frecuencia_tx', 'cat_tx_medio', 'cat_precio_medio'] #8
        columns_crecimiento_categoria = ['cat_crecimiento_venta', 'cat_crecimiento_venta_anual', 'cat_crecimiento_frecuencia_tx', 'cat_crecimiento_tx_medio', 'cat_crecimiento_precio_medio'] #5
        columns_condiciones_compra = ['%clientes_condicion_1', '%clientes_condicion_2', '%clientes_condicion_3', '%clientes_condicion_4', '%clientes_condicion_5', '%clientes_condicion_6'] #6
        columns_condiciones_tx_medio = ['tx_medio_condicion_1', 'tx_medio_condicion_2', 'tx_medio_condicion_3', 'tx_medio_condicion_4', 'tx_medio_condicion_5', 'tx_medio_condicion_6'] #6

        dict_kpis = {
            'marca': columns_marca,
            'crecimiento_marca': columns_crecimiento_marca,
            'categoria': columns_categoria,
            'crecimiento_categoria': columns_crecimiento_categoria,
            'condiciones_compra': columns_condiciones_compra,
            'condiciones_compra_tx_medio': columns_condiciones_tx_medio
        }

        kpi_columns = [col for sublist in dict_kpis.values() for col in sublist]

        # Inicializar el diccionario de DataFrames
        df_dict = {}
        
        # Dataframe de KPIs
        print(columns)
        if 'TOTAL' in columns:
            print('TOTAL en columns')
            print(df[df['tabla'] == 'TOTAL'][kpi_columns].reset_index(drop=True).T) #CONTINUAR
            df_kpis = df[df['tabla'] == 'TOTAL'][kpi_columns].reset_index(drop=True).T
            df_kpis.rename(columns={0: 'valor'}, inplace=True)
            df_kpis = df_kpis[df_kpis['valor'].notnull() & pd.to_numeric(df_kpis['valor'], errors='coerce').notnull()]
            df_dict['KPIS'] = df_kpis
            print('Dataframe de KPIs creado correctamente.')
        
        # Dataframes restantes
        for col in columns:
            if col != 'TOTAL':
                df_dict[col] = df[df['tabla'] == col]

        print('Dataframes separados correctamente.')

        return df_dict, dict_kpis

    def __set_theme(self, palette='Paired'):
        self.palette = palette
        # Paired, coolwarm, viridis, deep, muted, bright, pastel, dark, colorblind
        sns.set_theme(style="whitegrid", palette=self.palette)

    # Función para formatear etiquetas de miles y millones
    def label_K_M(self, number, col_var):
        # Si no es numerico deten la función
        if not isinstance(number, (int, float)):
            return number
        if number >= 1e6:
            label = f'{number/1e6:,.2f}M'
        elif number >= 1e3:
            label = f'{number/1e3:,.1f}K'
        elif 'share' in col_var or 'crec' in col_var or '%' in col_var:
            label = f'{number:.1%}'
        else:
            label = f'{number:,.1f}'
        
        if 'share' in col_var or 'crec' in col_var or '%' in col_var:
            return label
        elif 'venta' in col_var or 'precio' in col_var or 'tx_medio' in col_var:
            label = f'${label}'
            return label
        else:
            return label
        
    # Función para formatear el eje Y
    def format_y_axis(self, ax, col_var):
        ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: self.label_K_M(x, col_var)))

    # Función para formatear el eje X
    def format_x_axis(self, ax, col_var):
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: self.label_K_M(x, col_var)))

    # Función para graficar con estilo Seaborn
    def plot_mes(self, _df, col_var, figsize=(16, 4), y2_var=None, ax_type=['bar', None], twinx=False, show=False):
        df = _df.copy().sort_values('Mes')
        fig, ax1 = plt.subplots(figsize=figsize)

        # Ajustar el desplazamiento horizontal de las barras
        bar_width = 0.4
        x_positions = np.arange(len(df['Mes']))

        # Configuración para manejar gráficos de barras dobles
        if ax_type == ['bar', 'bar']:
            x_positions1 = x_positions - bar_width / 2
            x_positions2 = x_positions + bar_width / 2
        else:
            x_positions1 = x_positions
            x_positions2 = x_positions

        ax2 = ax1.twinx() if twinx else ax1

        # Asignar colores desde la paleta asignada
        colors = sns.color_palette(self.palette)

        # Gráfico para la primera variable
        if ax_type[0] == 'bar':
            ax1.bar(x=x_positions1, height=df[col_var], width=bar_width, label=col_var, color=colors[0])
        elif ax_type[0] == 'line':
            ax1.plot(x_positions1, df[col_var], label=col_var, marker='o', color=colors[0])
        else:
            raise ValueError("ax_type[0] must be 'bar' or 'line'")

        ax1.set_ylabel(col_var, color=colors[0])
        self.format_y_axis(ax1, col_var)

        # Etiquetas de datos en las barras
        for i, v in enumerate(df[col_var]):
            ax1.text(x_positions1[i], v, self.label_K_M(v, col_var), color='black', ha='center', va='bottom', fontsize=8)

        # Gráfico para la segunda variable
        if y2_var is not None:
            if ax_type[1] == 'bar':
                ax2.bar(x=x_positions2, height=df[y2_var], width=bar_width, label=y2_var, color=colors[1])
                for i, v in enumerate(df[y2_var]):
                    ax2.text(x_positions2[i], v, self.label_K_M(v, y2_var), ha='center', va='bottom', fontsize=8, color=colors[1])

            elif ax_type[1] == 'line':
                ax2.plot(x_positions2, df[y2_var], label=y2_var, marker='o', color=colors[1])
            else:
                raise ValueError("ax_type[1] must be 'bar' or 'line'")

            if twinx:
                ax2.set_ylabel(y2_var, color=colors[1])
                ax2.tick_params(axis='y', colors=colors[1])
            
            self.format_y_axis(ax2, y2_var)

            ax2.grid(False)

        # Ajustar etiquetas del eje X
        ax1.set_xticks(x_positions)
        ax1.set_xticklabels(df['Mes'], rotation=45)

        title = f"{col_var} por Mes" if not y2_var else f"{col_var} y {y2_var} por Mes"

        plt.title(title, fontsize=14, color='black', weight='bold')
        plt.tight_layout()
        
        if show:
            plt.show()
        else:
            plt.close(fig)

        return fig

    # ESTADO -------------------------------
    def merge_df_mapa(self, _df, df_mapa):
        df = _df.copy()
        estados = df_mapa.name.unique()
        for val_state in df['Estado'].unique():
            # Hacer el match de cada estado con los nombres del Geo, con una tolerancia de 90%
            match = process.extractOne(val_state, estados, score_cutoff=90)
            if match:
                # print(f'{state} -> {match[0]}')
                df.loc[df['Estado'] == val_state, 'Estado'] = match[0]
            else:
                print(f'No se encontró match para {val_state}')

        # Paso 3: Asegúrate de que los nombres de los estados coincidan entre el GeoJSON y tu DataFrame
        # Limpia los nombres en ambos DataFrames para evitar problemas (mayúsculas, espacios, acentos)
        df_mapa['name'] = df_mapa['name'].str.strip().str.lower()
        df['Estado'] = df['Estado'].str.strip().str.lower()

        # Paso 4: Haz un merge entre el GeoJSON y tus datos
        # El GeoJSON usa 'name' para los nombres de los estados, y tu DataFrame usa 'state'
        df_plot = df_mapa.merge(df, left_on='name', right_on='Estado', how='inner')

        return df_plot

    def plot_estado(self, df_plot, column, title, cmap='OrRd', figsize=(12, 8), show=False):
        # Paso 1: Crear la figura y los ejes
        fig, ax = plt.subplots(figsize=figsize)

        # Paso 2: Graficar el mapa coloreando por 'venta_actual'
        # Guardamos el resultado de `mexico.plot` para obtener el objeto de la barra de color
        plot = df_plot.plot(
            column=column,
            ax=ax,
            cmap=cmap,  # Escala de colores
            legend=False
        )

        # Paso 3: Personalizar la barra de color
        # Obtener la barra de color creada automáticamente
        cbar = plot.get_figure().colorbar(ax.collections[0], ax=ax, orientation="horizontal")
        cbar.set_label(title)  # Agregar etiqueta a la barra de color

        # Personalizar los valores de la barra de color como formato de moneda
        cbar.ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: self.label_K_M(x, column)))
        # format_y_axis(cbar.ax, column)

        # Ocultar las etiquetas de los ejes
        ax.set_xticklabels([])
        ax.set_yticklabels([])

        # Paso 4: Agregar un título
        plt.title(title, fontsize=16, weight='bold')

        # Paso 5: Mostrar el gráfico
        if show:
            plt.show()
        else:
            plt.close(fig)

        # Cambiar la columna a formato de moneda
        df_aux = df_plot[['Estado', column]].sort_values(column, ascending=False).reset_index(drop=True)
        df_aux[column] = df_aux[column].map('${:,.0f}'.format)

        return fig, df_aux

    def top_low_n(self, df, ref_column, sortby, n=5):
        # Filtrar las tiendas con mayor y menor valor en 'ref_column'
        top = df.sort_values(sortby, ascending=False).head(10)[ref_column].values.tolist()
        lowest = df.sort_values(sortby, ascending=True).head(10)[ref_column].values.tolist()
        filter = top + lowest

        df_filtered = df[df[ref_column].isin(filter)].sort_values(sortby, ascending=False)
        
        # Agrupar por estado y sumar la venta
        df_agg = df_filtered.groupby('Estado').agg(
            share=('share', 'mean'),
            venta_actual=('venta_actual', 'sum'),
            cat_venta_actual=('cat_venta_actual', 'sum'),
            clientes_actual=('clientes_actual', 'sum'),
            cat_clientes_actual=('cat_clientes_actual', 'sum'),
            tx_medio=('tx_medio', 'mean'),
            cat_tx_medio=('cat_tx_medio', 'mean'),
            precio_medio=('precio_medio', 'mean'),
            cat_precio_medio=('cat_precio_medio', 'mean'),
            frecuencia_tx=('frecuencia_tx', 'mean'),
            cat_frecuencia_tx=('cat_frecuencia_tx', 'mean'),
            crecimiento_venta=('crecimiento_venta', 'mean'),
            cat_crecimiento_venta=('cat_crecimiento_venta', 'mean'),
            crecimiento_venta_anual=('crecimiento_venta_anual', 'mean'),
            cat_crecimiento_venta_anual=('cat_crecimiento_venta_anual', 'mean'),
            perc_venta_online=('%venta_online', 'mean'),
            cat_perc_venta_online=('%cat_venta_online', 'mean'),
            promedio_dias_recompra=('promedio_dias_recompra', 'mean'),
            cat_promedio_dias_recompra=('cat_promedio_dias_recompra', 'mean'),

            perc_clientes_condicion_1=('%clientes_condicion_1', 'mean'),
            perc_clientes_condicion_2=('%clientes_condicion_2', 'mean'),
            perc_clientes_condicion_3=('%clientes_condicion_3', 'mean'),
            perc_clientes_condicion_4=('%clientes_condicion_4', 'mean'),
            perc_clientes_condicion_5=('%clientes_condicion_5', 'mean'),
            perc_clientes_condicion_6=('%clientes_condicion_6', 'mean'),
            perc_recompra=('%recompra', 'mean'),
            perc_recompra_2m=('%recompra_2m', 'mean'),
            perc_recompra_3m=('%recompra_3m', 'mean'),
            perc_nuevos=('%nuevos', 'mean'),
            perc_fid=('%fid', 'mean'),
            perc_rec=('%rec', 'mean'),    
        ).reset_index()
        # df_agg

        df_filtered[sortby] = df_filtered[sortby].map('${:,.0f}'.format if 'venta' in sortby else '{:,.0f}'.format)
        # df_agg[sortby] = df_agg[sortby].map('${:,.0f}'.format if 'venta' in sortby else '{:,.0f}'.format)

        return df_filtered, df_agg

    def plot_table(self, df, variable, title='Tabla de datos', figsize=(10, 4), title_off=1, show=False):
        # Crear una figura y un eje
        fig, ax = plt.subplots(figsize=figsize)

        # Ajustar el margen superior para dar espacio al título
        plt.subplots_adjust(top=title_off)

        # Ocultar el eje
        ax.xaxis.set_visible(False)
        ax.yaxis.set_visible(False)
        ax.set_frame_on(False)

        # Crear una tabla a partir del DataFrame
        tabla = table(ax, df, loc='center', cellLoc='center')

        # Ajustar el tamaño de la fuente
        tabla.auto_set_font_size(False)
        tabla.set_fontsize(10)

        # Ajustar automáticamente el tamaño de las columnas
        max_char_per_col = [max(df[col].astype(str).apply(len).max(), len(col)) for col in df.columns]
        total_chars = sum(max_char_per_col)
        col_widths = [char / total_chars for char in max_char_per_col]
        tabla.scale(1.2, 1.2)  # Escalar para mejorar la visualización
        for i, col_width in enumerate(col_widths):
            tabla.auto_set_column_width(i)
            for key, cell in tabla.get_celld().items():
                if key[1] == i:  # Columna específica
                    cell.set_width(col_width)

        # Colorear el fondo de los títulos de las columnas
        for key, cell in tabla.get_celld().items():
            if key[0] == 0:  # Encabezados
                cell.set_fontsize(12)
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor('#000080')  # Azul marino

        # Resaltar la mayor venta a la menor en la columna 'variable'
        ventas = df[variable].str.replace('$', '').str.replace(',', '').astype(float)
        max_venta = ventas.max()
        min_venta = ventas.min()

        for i, venta in enumerate(ventas):
            color_intensity = (venta - min_venta) / (max_venta - min_venta)
            color = plt.cm.Greens(color_intensity)
            tabla[(i + 1, df.columns.get_loc(variable))].set_facecolor(color)

            # Cambiar el color del texto a blanco si el fondo es muy oscuro
            text_color = 'white' if color_intensity > 0.5 else 'black'
            tabla[(i + 1, df.columns.get_loc(variable))].set_text_props(color=text_color)

        # Mostrar el título en la parte superior
        plt.title(title, fontsize=16, weight='bold')
        
        if show:
            plt.show()
        else:
            plt.close(fig)

        return fig

    # FORMATO
    # sns.set_theme(style='whitegrid', palette=paleta)
    # Graficar la venta por variable categorica
    def plot_cat_column(self, _df, cat_column, variable, title, figsize=(12, 6), estimator='sum', show=False):
        df = _df.copy().sort_values(variable, ascending=False)

        # plt.figure(figsize=figsize)
        fig, ax = plt.subplots(figsize=figsize)
        sns.barplot(x=cat_column, y=variable, data=df, errorbar=None, estimator=estimator, hue=cat_column)
        plt.title(title, weight='bold', fontsize=16)

        # Agregar etiquetas de datos en cada columna
        for i, v in enumerate(df.groupby(cat_column)[variable].agg(estimator).sort_values(ascending=False)):
            label = self.label_K_M(v, variable) 
            plt.text(i, v, label, color='black', ha='center', va='bottom', fontsize=8)

        # Cambiar el formato de los valores en el eje y
        # plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'${x:,.0f}'))
        self.format_y_axis(ax, variable)

        if show:
            plt.show()
        else:
            plt.close(fig)

        return fig

    def plot_pie(self, _df, cat_column, var_column, figsize=(8, 8), show=False):
        df = _df.copy().sort_values(var_column, ascending=False)
        
        # Quitar vacíos
        df = df[df[var_column] > 0]

        # Configurar los datos para el gráfico
        sizes = df[var_column]
        labels = df[cat_column]
        # colors = sns.color_palette('colorblind', len(labels))

        # Crear el gráfico de pastel
        fig, ax = plt.subplots(figsize=figsize)
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, textprops={'fontsize': 12})
        plt.title(f'{var_column} por {cat_column}', fontsize=16, weight='bold')
        
        if show:
            plt.show()
        else:
            plt.close(fig)

        return fig

    # CLASS
    def rank_column(self, _df, columns=['venta_actual', 'cat_venta_actual']):
        df = _df.copy()

        # Agregar la posición en la marca y en la categoría en el ranking
        for column in columns:
            if f'rank_{column}' not in df.columns:
                df.loc[:, f'rank_{column}'] = df[column].rank(ascending=False, method='min').fillna(0).astype(int)

        return df

    # Graficar la venta por variable categorica
    def plot_cat_column_h(self, _df, cat_column, variable, title, figsize=(12, 6), estimator='sum', frac_off_x=0.01, frac_off_y=0.01, hue=None, top=None, show=False):
        df = _df.copy()

        df = self.rank_column(df)
        df = df[df[variable] > 0].sort_values(variable, ascending=False)

        if not hue:
            hue = cat_column

        if top:
            if top < len(df):
                title = f'{title} (Top {top})'
            df = df.head(top)

        fig, ax = plt.subplots(figsize=figsize)
        sns.barplot(x=variable, y=cat_column, data=df, errorbar=None, estimator=estimator, hue=hue, orient='h', width=0.8)
        plt.title(title, weight='bold', fontsize=16)

        # Calcular el offset para las etiquetas de datos
        off_x = frac_off_x * df[variable].max()
        off_y = frac_off_y * df[cat_column].nunique()
        
        # Agregar etiquetas de datos en cada barra
        df_agg = df.groupby(cat_column).agg(estimator).sort_values(by=variable, ascending=False)

        for i in range(len(df_agg)):
            valor = df_agg.iloc[i][variable]
            label = self.label_K_M(valor, variable)
            
            # Agregar al final de la label entre parentesis el ranking de categoría
            rank = df[df[cat_column] == df_agg.index[i]]['rank_cat_' + variable].values[0]
            label = f'{label} ({rank})'
            plt.text(valor + off_x, i + off_y, label, color='black', ha='center', va='bottom', fontsize=8)  # Agregar offset del 1%

        # # Cambiar el formato de los valores en el eje x
        self.format_x_axis(ax, variable)

        if show:
            plt.show()
        else:
            plt.close(fig)

        return fig

    # Muestra en una figura el valor de venta_actual
    def plot_value(self, df, row, title, column='valor', figsize=(6, 4), show=False):
        fig, ax = plt.subplots(figsize=figsize)
        
        # Obtener el valor de la columna
        value = df.loc[row, column]
        value = self.label_K_M(value, row)

        # Mostrar el valor
        ax.text(0.5, 0.5, value, color='black', ha='center', va='center', fontsize=20, weight='bold')

        # Ocultar los ejes
        ax.axis('off')

        # Agregar un título
        plt.title(title if title else row, fontsize=16, weight='bold')

        if show:
            plt.show()
        else:
            plt.close(fig)

        return fig

    # Guardar todas las metricas en una imagen
    def plot_kpis(self, df_kpis, dict_kpis, show=False):

        # Calcular las filas y columnas dinámicamente según el diccionario de KPIs
        nrows = len(dict_kpis)  # Una fila por cada categoría en dict_kpis
        ncols = max(len(kpi_list) for kpi_list in dict_kpis.values())  # Número máximo de KPIs por categoría

        # Crear el subplot principal con tamaño adecuado
        final_fig, axes = plt.subplots(nrows, ncols, figsize=(ncols*2.5, nrows*0.6))  # Ajustar el tamaño según sea necesario

        # Asegurarse de que axes sea una matriz unidimensional si solo hay una fila
        if nrows == 1:
            axes = np.expand_dims(axes, axis=0)

        # Insertar las figuras creadas en la matriz de subplots
        kpi_idx = 0  # Índice para recorrer las métricas del dataframe df_kpis
        for row_idx, (category, kpi_columns) in enumerate(dict_kpis.items()):
            # Iterar sobre los KPIs de la categoría
            for col_idx, kpi_column in enumerate(kpi_columns):
                if kpi_idx < len(df_kpis.index):  # Verificar que haya un valor para este KPI en el dataframe
                    # fig = df_kpis.iloc[kpi_idx]['fig']
                    fig = self.plot_value(df_kpis, kpi_column, title=kpi_column, column='valor', figsize=(2, 1), show=False)
                    source_ax = fig.axes[0]  # Obtener el eje de la figura original
                    
                    # Copiar el texto del eje original al subplot
                    for text in source_ax.texts:  # Copiar textos (si los hay)
                        axes[row_idx, col_idx].text(
                            text.get_position()[0], text.get_position()[1], text.get_text(),
                            color=text.get_color(), ha=text.get_ha(), va=text.get_va(),
                            fontsize=text.get_fontsize(), fontweight=text.get_fontweight()
                        )
                    
                    # Copiar otros elementos si los hay (ej. título, etiquetas)
                    axes[row_idx, col_idx].set_title(source_ax.get_title().replace('_', ' ').capitalize(), weight='bold')
                    axes[row_idx, col_idx].set_xlabel(source_ax.get_xlabel())
                    axes[row_idx, col_idx].set_ylabel(source_ax.get_ylabel())

                    # Si el eje original tiene leyenda, copiarla
                    if source_ax.get_legend():
                        axes[row_idx, col_idx].legend(loc=source_ax.get_legend_loc())
                    
                    plt.close(fig)  # Cerrar la figura original (ya está movida al subplot)

                    # Desactivar los ejes que no se usan
                    axes[row_idx, col_idx].axis('off')

                kpi_idx += 1  # Incrementar el índice de KPI para el siguiente gráfico

            # Después de llenar una fila, los subgráficos sobrantes deben ser desactivados
            for col_idx in range(len(kpi_columns), ncols):
                axes[row_idx, col_idx].axis('off')  # Desactivar ejes vacíos

        # Ajustar el diseño final
        plt.tight_layout()

        if show:
            plt.show()
        else:
            plt.close(final_fig)

        return final_fig

    def get_figs(self, df_dict, dict_kpis, show=False):
        
        df_mes = df_dict['MES']
        df_estado = df_dict['ESTADO']
        df_tienda = df_dict['TIENDA']
        df_formato = df_dict['FORMATO']
        df_familia = df_dict['FAMILIA']
        df_nse = df_dict['NSE']
        df_class = df_dict['CLASS']
        df_subclass = df_dict['SUBCLASS']
        df_prod_type = df_dict['PROD_TYPE']
        df_producto = df_dict['PRODUCTO']
        df_kpis = df_dict['KPIS']
        df_mexico = self.df_mexico

        # Figuras
        fig_mes = self.plot_mes(df_mes, 'venta_actual', y2_var='share', figsize=(16, 4), ax_type=['bar', 'line'], twinx=True, show=show)

        df_plot = self.merge_df_mapa(df_estado, df_mexico)
        fig_estados, df_table_estados = self.plot_estado(df_plot, 'venta_actual', 'Venta Actual por Estado', cmap='OrRd', figsize=(12, 8), show=show)

        df_table_filtered_tiendas, df_filtered_tiendas_agg = self.top_low_n(df_tienda, 'Tienda', 'venta_actual', n=5)
        df_plot = self.merge_df_mapa(df_filtered_tiendas_agg, df_mexico)
        fig_top_tiendas, df_table_filtered_estados = self.plot_estado(df_plot, 'venta_actual', 'Top y Lowest 10 tiendas por Estado', cmap='OrRd', figsize=(12, 8), show=show)

        fig_table_estados = self.plot_table(df_table_estados, 'venta_actual', 'Venta por Estados', title_off=2, figsize=(6, 4), show=show)
        fig_table_top_tiendas = self.plot_table(df_table_filtered_tiendas, 'venta_actual', 'Top y Lowest 10 Tiendas por Venta Actual', title_off=1.2, figsize=(12, 4), show=show)

        fig_formato = self.plot_cat_column(df_formato, 'Formato', 'venta_actual', 'Venta Actual por Formato de Tienda', figsize=(12, 4), show=show)
        fig_familia = self.plot_cat_column(df_familia, 'Familia', 'venta_actual', 'Venta Actual por Tipo de Familia', figsize=(12, 4), show=show)
        fig_nse = self.plot_cat_column(df_nse, 'NSE', 'venta_actual', 'Venta Actual por NSE', figsize=(12, 4), show=show)

        fig_formato_pie = self.plot_pie(df_formato, 'Formato', 'venta_actual', figsize=(8, 4), show=show)
        fig_familia_pie = self.plot_pie(df_familia, 'Familia', 'venta_actual', figsize=(8, 4), show=show)
        fig_nse_pie = self.plot_pie(df_nse, 'NSE', 'venta_actual', figsize=(8, 4), show=show)

        fig_class = self.plot_cat_column_h(df_class, 'Class', 'venta_actual', 'Venta Actual por Clase', figsize=(8, 2), frac_off_x=0.06, top=5, show=show)
        fig_subclass = self.plot_cat_column_h(df_subclass, 'Subclass', 'venta_actual', 'Venta Actual por Subclase', figsize=(8, 2), frac_off_x=0.06, top=5, show=show)
        fig_prod_type = self.plot_cat_column_h(df_prod_type, 'Prodtype', 'venta_actual', 'Venta Actual por Tipo de Producto', figsize=(8, 4), frac_off_x=0.06, hue='Subclass', top=10, show=show)
        fig_product = self.plot_cat_column_h(df_producto, 'Producto', 'venta_actual', 'Venta Actual por Producto', figsize=(8, 8), frac_off_x=0.06, frac_off_y=0.005, hue='Prodtype', top=20, show=show)

        fig_kpis = self.plot_kpis(df_kpis, dict_kpis, show=False)

        # Guardar las figuras
        fig_dict = {
            'fig_kpis': fig_kpis,
            'fig_mes': fig_mes,
            'fig_estados': fig_estados,
            'fig_top_tiendas': fig_top_tiendas,
            'fig_table_estados': fig_table_estados,
            'fig_table_top_tiendas': fig_table_top_tiendas,
            'fig_formato': fig_formato,
            'fig_familia': fig_familia,
            'fig_nse': fig_nse,
            'fig_formato_pie': fig_formato_pie,
            'fig_familia_pie': fig_familia_pie,
            'fig_nse_pie': fig_nse_pie,
            'fig_class': fig_class,
            'fig_subclass': fig_subclass,
            'fig_prod_type': fig_prod_type,
            'fig_product': fig_product
        }

        df_fig = pd.DataFrame(fig_dict, index=[0]).T.rename(columns={0: 'fig'})
        
        print('Guardando las figuras.')

        # Validar que existe la carpeta 'folder_name', si no existe, crearla. Después validar que dentro exista la carpeta 'images', si no existe, crearla.
        if not os.path.exists(self.foldername):
            os.makedirs(self.foldername)
        if not os.path.exists(f'{self.foldername}/images'):
            os.makedirs(f'{self.foldername}/images')

        for row in df_fig.index:
            df_fig.loc[row, 'fig'].savefig(f'{self.foldername}/images/{row}.svg', bbox_inches='tight', dpi=300)

    def validate_fig(self, image_base_path, fig_dict):
        for key, filename in fig_dict.items():
            image_path = image_base_path / filename
            if not image_path.exists():
                print(f"Error: La imagen {image_path} no existe.")

    def __html_content(self):
        # Define the base path for images
        image_base_path = self.foldername / 'images'
        
        # Diccionario con los nombres de las imágenes
        fig_dict = {
            'fig_kpis': 'fig_kpis.svg',
            'fig_mes': 'fig_mes.svg',
            'fig_estados': 'fig_estados.svg',
            'fig_top_tiendas': 'fig_top_tiendas.svg',
            'fig_table_estados': 'fig_table_estados.svg',
            'fig_table_top_tiendas': 'fig_table_top_tiendas.svg',
            'fig_formato': 'fig_formato.svg',
            'fig_familia': 'fig_familia.svg',
            'fig_nse': 'fig_nse.svg',
            'fig_formato_pie': 'fig_formato_pie.svg',
            'fig_familia_pie': 'fig_familia_pie.svg',
            'fig_nse_pie': 'fig_nse_pie.svg',
            'fig_class': 'fig_class.svg',
            'fig_subclass': 'fig_subclass.svg',
            'fig_prod_type': 'fig_prod_type.svg',
            'fig_product': 'fig_product.svg'
        }
        
        # Create the HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html lang=\"en\">
        <head>
            <meta charset=\"UTF-8\">
            <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
            <title>Analisis BC</title>
            <style>
                body {{
                    background-color: white;
                    margin: 0;
                    padding: 0;
                    overflow-y: auto;
                }}
                .container {{
                    display: grid;
                    grid-template-columns: repeat(12, 1fr);
                    grid-template-rows: auto;
                    gap: 10px;
                    width: 100vw;
                }}
                .container img {{
                    width: 100%;
                    height: auto;
                }}
                .title {{ grid-column: span 12; grid-row: 1; }}
                .fig_mes {{ grid-column: span 12; grid-row: 2; }}
                .fig_estados {{ grid-column: span 5; grid-row: 3; }}
                .fig_top_tiendas {{ grid-column: span 5; grid-row: 3; }}
                .fig_table_estados {{ grid-column: span 2; grid-row: 3; transform: scale(1.1); }}
                .fig_formato {{ grid-column: span 4; grid-row: 4; }}
                .fig_nse {{ grid-column: span 4; grid-row: 4; }}
                .fig_familia {{ grid-column: span 4; grid-row: 4; }}
                .fig_formato_pie {{ grid-column: span 4; grid-row: 5; }}
                .fig_nse_pie {{ grid-column: span 4; grid-row: 5; }}
                .fig_familia_pie {{ grid-column: span 4; grid-row: 5; }}
                .fig_class {{ grid-column: span 5; grid-row: 6; }}
                .fig_subclass {{ grid-column: span 5; grid-row: 7; }}
                .fig_prod_type {{ grid-column: span 5; grid-row: 8; }}
                .fig_product {{ grid-column: span 7; grid-row: 6 / span 3; }}
            </style>
        </head>
        <body>
            <div class=\"container\">
                <div class=\"title\"><img src=\"{image_base_path / fig_dict['fig_kpis']}\" alt=\"KPIs\"></div>
                <div class=\"fig_mes\"><img src=\"{image_base_path / fig_dict['fig_mes']}\" alt=\"Mes\"></div>
                <div class=\"fig_estados\"><img src=\"{image_base_path / fig_dict['fig_estados']}\" alt=\"Estados\"></div>
                <div class=\"fig_top_tiendas\"><img src=\"{image_base_path / fig_dict['fig_top_tiendas']}\" alt=\"Top Tiendas\"></div>
                <div class=\"fig_table_estados\"><img src=\"{image_base_path / fig_dict['fig_table_estados']}\" alt=\"Tabla Estados\"></div>
                <div class=\"fig_formato\"><img src=\"{image_base_path / fig_dict['fig_formato']}\" alt=\"Formato\"></div>
                <div class=\"fig_nse\"><img src=\"{image_base_path / fig_dict['fig_nse']}\" alt=\"NSE\"></div>
                <div class=\"fig_familia\"><img src=\"{image_base_path / fig_dict['fig_familia']}\" alt=\"Familia\"></div>
                <div class=\"fig_formato_pie\"><img src=\"{image_base_path / fig_dict['fig_formato_pie']}\" alt=\"Formato Pie\"></div>
                <div class=\"fig_nse_pie\"><img src=\"{image_base_path / fig_dict['fig_nse_pie']}\" alt=\"NSE Pie\"></div>
                <div class=\"fig_familia_pie\"><img src=\"{image_base_path / fig_dict['fig_familia_pie']}\" alt=\"Familia Pie\"></div>
                <div class=\"fig_class\"><img src=\"{image_base_path / fig_dict['fig_class']}\" alt=\"Clase\"></div>
                <div class=\"fig_subclass\"><img src=\"{image_base_path / fig_dict['fig_subclass']}\" alt=\"Subclase\"></div>
                <div class=\"fig_prod_type\"><img src=\"{image_base_path / fig_dict['fig_prod_type']}\" alt=\"Tipo de Producto\"></div>
                <div class=\"fig_product\"><img src=\"{image_base_path / fig_dict['fig_product']}\" alt=\"Producto\"></div>
            </div>
        </body>
        </html>
        """
        
        return html_content

    def save_html(self, periodo='total', show_figs=False, foldername=None, filename='Analisis_BC'):

        if foldername:
            self.__set_foldername(foldername)

        if periodo == 'total':
            df = self.df[self.df['Mes'] != 'CAMPANA']
        elif periodo == 'campana':
            df = self.df[self.df['Mes'] == 'CAMPANA']

        df_dict, dict_kpis = self.__split_df(df)
        self.get_figs(df_dict, dict_kpis, show=show_figs)

        # Save to a file
        if 'html' not in filename:
            filename += '.html'
        self.html_analisis = Path(f'{self.foldername}/{filename}')
        
        self.html_analisis.write_text(self.__html_content(), encoding='utf-8')

        print('Reporte de Analisis generado.')

# df_raw = pd.read_csv('Analisis BC/data.csv')
# analisis = Analisis()
# analisis.set_df(df_raw)
# analisis.save_html(periodo='total', show_figs=False, foldername='Analisis BC', filename='Analisis_BC')
