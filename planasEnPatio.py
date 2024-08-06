from flask import render_template, Blueprint
from datetime import datetime
from db_manager import fetch_data, fetch_data_DIA
import gdown
import io
import pandas as pd

planasEnPatio = Blueprint('planasEnPatio', __name__)
@planasEnPatio.route('/')
def index():
    planas, _, DataDIA = cargar_datos()
    planasSAC = planas_sac()
    planasPatio = planas_en_patio(planas, DataDIA, planasSAC)
    
    html_empates_dobles = planasPatio.to_html()
    return render_template('planasEnPatio.html', datos_html= html_empates_dobles)

def cargar_datos():
    consulta_planas = """
        SELECT *
        FROM DimTableroControlRemolque_CPatio
        WHERE PosicionActual = 'NYC'
        AND Estatus = 'CARGADO EN PATIO'
        AND Ruta IS NOT NULL
        AND CiudadDestino != 'MONTERREY'
        AND CiudadDestino != 'GUADALUPE'
        AND CiudadDestino != 'APODACA'
    """
    consulta_operadores = """
        SELECT * 
        FROM DimTableroControl_Disponibles
        """
    ConsultaDBDIA= "SELECT * FROM DIA_NYC"
    
    planas = fetch_data(consulta_planas)
    Operadores = fetch_data(consulta_operadores)  
    DataDIA= fetch_data_DIA(ConsultaDBDIA)
    return planas, Operadores, DataDIA

def planas_en_patio(planas, DataDIA, planasSAC):
    planas['Horas en patio_Sistema'] = ((datetime.now() - planas['FechaEstatus']).dt.total_seconds() / 3600.0).round(1)
    planas = pd.merge(planas, planasSAC, on='Remolque', how='left')
    planas['Horas en patio'] = ((datetime.now() - planas['fecha de salida']).dt.total_seconds() / 3600.0).round(1)
    planas= planas[~planas['Remolque'].isin(DataDIA['Plana'])]#Excluye unidades ya asigndos
    planas['ValorViaje'] = planas['ValorViaje'].apply(lambda x: "${:,.0f}".format(x))
    planas.sort_values(by=['Horas en patio'], ascending=False, inplace=True)
    planas = planas[['Remolque', 'CiudadDestino', 'Horas en patio', 'Horas en patio_Sistema']]
    planas.reset_index(drop=True, inplace=True)
    planas.index += 1
    return planas

def procesar_operadores(Operadores, DataDIA):
    Operadores = Operadores[(Operadores['Estatus'] == 'Disponible') & (Operadores['Destino'] == 'NYC')]
    Operadores  = Operadores [Operadores ['UOperativa'].isin(['U.O. 01 ACERO', 'U.O. 02 ACERO', 'U.O. 03 ACERO', 'U.O. 04 ACERO', 'U.O. 06 ACERO (TENIGAL)', 'U.O. 07 ACERO','U.O. 39 ACERO'])]
    Operadores['Tiempo Disponible'] = ((datetime.now() - Operadores['FechaEstatus']).dt.total_seconds() / 3600).round(1)
    #Operadores =Operadores[Operadores['Tiempo Disponible'] > 6]
    Operadores= Operadores[~Operadores['Operador'].isin(DataDIA['Operador'])]#Excluye operadores ya asigndos
    Operadores = Operadores[['Operador', 'Tractor', 'UOperativa', 'Tiempo Disponible']]
    Operadores.sort_values(by='Tiempo Disponible', ascending=False, inplace=True)
    Operadores.reset_index(drop=True, inplace=True)
    Operadores.index += 1 
    return Operadores

def planas_sac():
    url = 'https://drive.google.com/uc?id=1h3oynOXp11tKAkNmq4SkjBR8q_ZyJa2b'
    path = gdown.download(url, output=None, quiet=False)  # Guarda el archivo temporalmente

    # Abrir el archivo temporal en modo binario
    with open(path, 'rb') as f:
        data = io.BytesIO(f.read())  # Leer los datos del archivo y pasarlos a BytesIO

    # Leer el archivo desde el buffer directamente en un DataFrame
    df = pd.read_excel(data)
    
    # Ordenar el DataFrame por la columna 'Cita de descarga' en orden descendente
    df = df.sort_values(by='fecha de salida', ascending=False, na_position='last')
    df = df.groupby('Remolque')['fecha de salida'].max().reset_index()
    
    return df