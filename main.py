# Crear un programa que permita ingresar varaibles por usuario y retorne un archivo csv con los POs

# Import Libraries
from publicos_objetivo import *
import warnings

# Ignore SQLAlchemy warnings
warnings.filterwarnings('ignore')

# Create a conn object. Automatically connects to DB
conn = PublicosObjetivo('test')

# Iniciar programa
conn.start_program()

# conn.close()
