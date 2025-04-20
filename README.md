# DataScience Públicos Objetivos

## Explicación General y Objetivo

El proyecto **DataScience Públicos Objetivos** tiene como objetivo principal analizar y procesar datos relacionados con campañas de monetización, segmentación de públicos y análisis de datos geográficos y demográficos. Este sistema permite gestionar campañas, realizar análisis de datos, y generar reportes visuales e interactivos para la toma de decisiones estratégicas.

El proyecto está diseñado para ser modular y escalable, integrando herramientas de análisis de datos, procesamiento de información en formatos como HTML y JSON, y una interfaz gráfica de usuario (GUI) para facilitar la interacción con los usuarios.

## Contenido y Módulos

### 1. **main_gui.py**
   - Archivo principal que ejecuta la interfaz gráfica de usuario (GUI). Permite a los usuarios interactuar con las funcionalidades del proyecto, como la gestión de campañas, visualización de resultados y configuración de filtros.

### 2. **util/**
   - **config/**
     - `credentials.yaml`: Almacena credenciales necesarias para conexiones externas (por ejemplo, bases de datos o APIs).
   - **constants/**
     - `bc.py`: Define constantes y mapeos utilizados en el análisis, como regiones, estados, formatos de tienda, y segmentaciones (NSE, familias, etc.).
     - `gui.py`: Contiene configuraciones específicas para la interfaz gráfica.
   - **data/**
     - Contiene recursos estáticos como imágenes (icono_cogno.png, logo_cogno.png) y archivos JSON (México.json, mexicoHigh.json) utilizados para visualizaciones geográficas.
   - **functions/**
     - `analisis_html.py`: Procesa y analiza archivos HTML para extraer información relevante.
     - `campana.py`: Gestiona la lógica relacionada con las campañas de monetización.
     - `connection.py`: Maneja las conexiones a bases de datos o APIs externas.
     - `GUI.py`: Implementa la lógica de la interfaz gráfica, incluyendo la creación de ventanas, menús y componentes interactivos.
     - `monetizacion.py`: Contiene funciones relacionadas con el análisis y generación de resultados de monetización.
     - `path.py`: Gestiona rutas de archivos y directorios.
     - `productos.py`: Procesa información relacionada con productos y categorías.
### 3. **pages/**
   - Contiene páginas adicionales que pueden ser cargadas en la GUI, como reportes o configuraciones específicas.

### 4. **sql_queries/**
   - Almacena consultas SQL utilizadas para extraer y procesar datos desde bases de datos.

## Estructura

La estructura del proyecto es modular, organizada por carpetas y archivos que representan diferentes funcionalidades y componentes del sistema. A continuación se describe la organización principal:

- **`main_gui.py`**: Controla la interfaz gráfica de usuario.
- **`util/`**: Contiene recursos y funciones auxiliares como configuraciones, constantes, y funciones de análisis.
- **`pages/`**: Páginas adicionales cargadas en la GUI.
- **`sql_queries/`**: Contiene las consultas SQL necesarias para interactuar con bases de datos.

## Funcionamiento

Aquí puedes agregar imágenes y descripciones de cómo funciona cada parte del sistema. Algunos ejemplos incluyen:

- **Interfaz Gráfica de Usuario (GUI)**: Imagen que muestra cómo se visualiza la GUI y cómo interactúa el usuario con las diferentes funciones.
- **Análisis de Datos**: Imagen de los reportes generados, gráficos y visualizaciones.
- **Gestión de Campañas**: Imagen de cómo se gestionan las campañas y los datos asociados.

## Resultado del Proyecto y Conclusiones

El proyecto proporciona una herramienta integral para la gestión de campañas de monetización y análisis de datos geográficos, demográficos y de productos. Se han logrado los siguientes resultados:

- **Automatización de procesos**: Reducción de tareas manuales mediante la automatización de la generación de reportes y análisis.
- **Mejor toma de decisiones**: Los usuarios ahora pueden visualizar de manera clara y accesible los resultados de sus campañas y los datos relacionados con el mercado.
- **Modularidad y escalabilidad**: El sistema está diseñado de manera que puede adaptarse fácilmente a nuevas necesidades o integrarse con otros sistemas.

## Contacto

Para cualquier duda o sugerencia, por favor contacta a:

**Ricardo Jaramillo**  
Email: [ricardo.jaramillo@example.com](mailto:ricardo.jaramillo@example.com)
