from flask import Flask, render_template
from flask import Blueprint
import pyodbc
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from itertools import product
from collections import defaultdict
from itertools import combinations
import ipywidgets as widgets
import datetime
from sqlalchemy import create_engine
import datetime
from datetime import datetime, timedelta


# Parámetros de conexión
server = '74.208.51.229'
database = 'DWH_SPLGNYC'
database2 = 'DWH_SPLPRO'
username = 'mhernandez'
password = '7xkDa7j7ejT5qi!n'
CartasPorte = 'ReporteCartasPorte'

Km = "DimRentabilidadLiquidacion"

# Cadena de conexión SQLAlchemy
engine = create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=SQL+Server")

# Ejecutar una consulta SQL y obtener el resultado como un dataframe
ConsultaCartas = f"SELECT * FROM {CartasPorte} WHERE FechaSalida > '2024-01-01'"

# Convertir a dataframe
Cartas = pd.read_sql(ConsultaCartas, engine)
engine.dispose()


CP= Cartas.copy()

# 30 dias atras
fecha_actual = datetime.now()
fecha_30_dias_atras = fecha_actual - timedelta(days=30)

# Dividir la columna 'Ruta' por '||' y luego por '-' para obtener origen
CP[['ID1', 'Ciudad_Origen', 'Ciudad_Destino']] = CP['Ruta'].str.split(' \|\| | - ', expand=True)

# Filtro Mes actual, UO, ColumnasConservadas, Ciudad Origen
CP = CP[CP['FechaSalida'] >= fecha_30_dias_atras]
CP = CP[CP['UnidadOperativa'].isin(['U.O. 01 ACERO', 'U.O. 02 ACERO', 'U.O. 03 ACERO', 'U.O. 04 ACERO', 'U.O. 39 ACERO', 'U.O. 07 ACERO'])]
CP = CP[CP['Ciudad_Origen'].isin(['MONTERREY'])]
CP = CP[CP['Cliente'].isin(['TERNIUM MEXICO'])]
CP= CP[['Operador', 'Remolque1', 'Remolque2', 'SubtotalMXN', 'FechaSalida', 'Ciudad_Destino']]
CP.sort_values(by=['FechaSalida'], ascending=False, inplace=True)


CP


# Convertir el DataFrame a HTML
datos_renderizados = CP.to_html()


historicoAsignado = Blueprint('historicoAsignado', __name__)


@historicoAsignado .route('/')
def index():
    # Retornar el DataFrame como HTML
    return render_template('historicoAsignado.html', datos_html=datos_renderizados)