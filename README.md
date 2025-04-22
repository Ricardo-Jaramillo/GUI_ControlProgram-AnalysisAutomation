# Control Program – Analysis and Reporting System

## General Overview and Objective

The **Control Program** project aims to analyze and process data related to sales campaigns, audience segmentation, and geographic-demographic analysis. This system supports campaign management, data analysis, and the generation of visual and interactive reports for strategic decision-making.

Designed to be **modular** and **scalable**, the project integrates tools for data analysis, processing through formats like PowerBI, Excel, Python, HTML/JSON, and an intuitive graphical user interface (GUI) for easy interaction.

---

## Folder Structure

### 1. [main_gui.py](./main_gui.py)
Main script to launch the graphical user interface. It enables users to interact with project functionalities such as campaign management, result visualization, and filtering options.

### 2. [util/](./src/util)
Utility folder containing configuration files and constant definitions.

- [config/](./src/util/config)
  - [`credentials.yaml`](./src/util/config/credentials.yaml): Stores external access credentials (e.g., database or API keys).

- [constants/](./src/util/constants)
  - [`bc.py`](./src/util/constants/bc.py): Defines constants and mappings used throughout the analysis, such as regions, store formats, socioeconomic levels, and segments.
  - [`gui.py`](./src/util/constants/gui.py): GUI-specific configurations.

- [data/](./src/util/data)
  - Static resources like images and geographic JSON files.
    - Icons: `icono_cogno.png`, `logo_cogno.png`, etc.
    - Maps: `México.json`, `mexicoHigh.json`

### 3. [functions/](./src/functions)
Core logic and processing modules.

- [`analisis_html.py`](./src/functions/analisis_html.py): Parses and processes HTML data for extracting campaign or product insights.
- [`campana.py`](./src/functions/campana.py): Handles logic related to monetization campaigns.
- [`connection.py`](./src/functions/connection.py): Manages connections to databases and APIs.
- [`GUI.py`](./src/functions/GUI.py): Implements the GUI structure and user interaction flows.
- [`monetizacion.py`](./src/functions/monetizacion.py): Performs calculations and generates monetization analysis.
- [`path.py`](./src/functions/path.py): Path management and file routing.
- [`productos.py`](./src/functions/productos.py): Handles product and category information processing.
- [`publicos_objetivo.py`](./src/functions/publicos_objetivo.py): Segments customers into strategic audiences.
- [`query_builder.py`](./src/functions/query_builder.py): Dynamically builds SQL queries based on filters.
- [`radiografia.py`](./src/functions/radiografia.py): Generates brand-level analytical summaries (for PowerBI reports).

### 4. [pages/](./src/pages)
Additional GUI pages, such as advanced reports or configuration panels.

### 5. [sql_queries/](./src/sql_queries)
(SQL folder – coming soon): For modular SQL query files, reusable and cleaner.

### 6. [test/](./test)
Testing and debugging notebooks/scripts.
- [`test_analisis_bc.ipynb`](./test/test_analisis_bc.ipynb): Example notebook for testing Business Case logic.

---

## How It Works

Each section in the GUI corresponds to a specific stage of the analysis workflow:

📸 _Main menu interface._

![Main Menu](./data/images/0%20Menu%20principal.png)

### 1. **Products**
Users select the **Products**, **Brands**, **Suppliers**, and **Categories** to be analyzed.

📸 _Product selection interface._

![Products](./data/images/1.1%20Productos.png)

📸 _Categories selection._

![Products](./data/images/1.2%20Categorías.png)

---

### 2. **Business Case**
This module allows analysis of selected products by filters such as **Socioeconomic Level**, **Store Type**, and **Product Families**. Users can define date ranges and purchase conditions (e.g., minimum ticket).

📸 _Business Case Analysis._

![BC Analisis](./data/images/2.1%20Analisis%20y%20BC.png)

📸 _Business Case - Report Example._

![BC Analisis](./data/images/2.2%20Analisis%20y%20BC%20reporte.png.jpg)

---

### 3. **Target Audiences**
Classifies the customer base into **Loyal**, **Acquired**, or **Recovered** groups based on behavioral patterns. This section quantifies the potential audience for a campaign.

📸 _POs menu._

![PO](./data/images/3%20Publicos%20Objetivo.png)

---

### 4. **Contact Lists**
Based on the audience segments, this section shows how many users can be contacted via **SMS, Email, or WhatsApp**. It helps generate prioritized communication lists per channel.

📸 _Contactable audience by channel._

![Listas de envio - Total](./data/images/4.1%20Listas%20de%20envío.png)

📸 _Contactable audience by channel - Segment by Condition._

![Listas de envio - Cumple condición](./data/images/4.2%20Listas%20de%20envío.png)

---

### 5. **Short and Long Radiography**
Users define an analysis period and ticket condition. This section performs a deep analysis of the selected brand, generating insights for PowerBI reports (auto-exportable).

📸 _Long Radiography selection._

![Radiografía Larga](./data/images/5.1%20Radiografia%20larga.png)

An example of the [Long Radiography](./data/PDF%20reports/Radiografia%20corta.pdf) report can be accesed in the [PDF folder](./data/PDF%20reports)

📸 _Short Radiography selection._

![Radiografía corta](./data/images/5.2%20Radiografia%20corta.png)

An example of the [Short Radiography](./data/PDF%20reports/Radiografia%20corta.pdf) report can be accesed in the [PDF folder](./data/PDF%20reports)

<!-- 📸 _Preview of PowerBI dashboard generated._ -->

---

### 6. **Campaign Results**
Allows users to see, add, update or delete campaign results. All results are stored in the SQL Database and feed the PowerBI Reports.

📸 _Campaign Results interface._

![Resultados de Campaña](./data/images/6%20Resultados%20de%20Campaña.png)

An example of the [Campaign Results](./data/PDF%20reports/Resultados%20de%20Campaña.pdf) report can be accesed in the [PDF folder](./data/PDF%20reports)

---

### 7. **View/Download Data**
Allows users to revisit and download previously generated tables and datasets.

📸 _Export generated datasets._

![Guardar datos](./data/images/7%20Ver%20o%20Guardar%20datos.png)

---

## Project Results and Conclusions

This project provides a comprehensive tool for managing and analyzing sales campaigns, customer behavior, product performance, and generating strategic audiences for communication.

### Achievements:

- ✅ **Process Automation** – Eliminates repetitive manual tasks in report generation.
- ✅ **Enhanced Decision-Making** – Data visualizations provide clear campaign insights.
- ✅ **Modular & Scalable Design** – Easy to expand and integrate with new features or systems.
- ✅ **Time Savings** – Cuts analysis time by over 80%, allowing focus on higher-impact data science.
- ✅ **Improved Information Control** – Simple generation, viewing, and export of reports.
- ✅ **Centralized Platform** – Everything needed for campaign planning in one place.

---

## Contact

For questions or suggestions, please contact:

**Ricardo Jaramillo**  
📧 [r.jaramillohernandez@outlook.com](mailto:r.jaramillohernandez@outlook.com)

---

## Extras

- ✅ Easy to adapt to other products or market segments.
- 🚀 Plans for future integration with Streamlit or cloud APIs.
- 🔐 Secure credentials stored separately.
- 🧠 Built with modular Python components for clarity and maintainability.

---

## Requirements

Install dependencies via:

```bash
pip install -r requirements.txt
