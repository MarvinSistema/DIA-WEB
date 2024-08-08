from flask import render_template, Blueprint
from datetime import datetime
from db_manager import fetch_data, fetch_data_DIA
import gdown
import io
import pandas as pd
import warnings
import cProfile
import pstats
import os
from datetime import datetime, timedelta
from cachetools import TTLCache, cached
from concurrent.futures import ThreadPoolExecutor

planasEnPatio = Blueprint('planasEnPatio', __name__)
@planasEnPatio.route('/')
def index():
    # Inicia el perfilado
    profiler = cProfile.Profile()

    profiler.enable()
    planas, _, DataDIA = cargar_datos()
    planasSAC = planas_sac()
    planasPatio = planas_en_patio(planas, DataDIA, planasSAC)
    
    html_empates_dobles = planasPatio.to_html()

    # Detiene el perfilado
    profiler.disable()

    # Guarda y muestra los resultados del perfilado
    profiler.dump_stats('profiling_results_index')
    p = pstats.Stats('profiling_results_index')
    p.sort_stats('cumulative').print_stats(10)


    return render_template('planasEnPatio.html', datos_html= html_empates_dobles)

def cargar_datos():
    consultas = [
        ("fetch_data", """
            SELECT *
            FROM DimTableroControlRemolque_CPatio
            WHERE PosicionActual = 'NYC'
            AND Estatus = 'CARGADO EN PATIO'
            AND Ruta IS NOT NULL
            AND CiudadDestino != 'MONTERREY'
            AND CiudadDestino != 'GUADALUPE'
            AND CiudadDestino != 'APODACA'
        """),
        ("fetch_data", "SELECT * FROM DimTableroControl_Disponibles"),
        ("fetch_data_DIA", "SELECT * FROM DIA_NYC")
    ]

    with ThreadPoolExecutor() as executor:
        # Lanzar cada consulta en un hilo separado
        futures = [executor.submit(globals()[func], sql) for func, sql in consultas]
        # Esperar a que todas las consultas se completen y recoger los resultados
        results = [future.result() for future in futures]

    return tuple(results)

def planas_en_patio(planas, DataDIA, planasSAC):
    planas['Horas en patio_Sistema'] = ((datetime.now() - planas['FechaEstatus']).dt.total_seconds() / 3600.0).round(1)
    planas = pd.merge(planas, planasSAC, on='Remolque', how='left')
    planas['Horas en patio'] = ((datetime.now() - planas['fecha de salida']).dt.total_seconds() / 3600.0).round(1)
    planas= planas[~planas['Remolque'].isin(DataDIA['Plana'])]#Excluye unidades ya asigndos
    planas['ValorViaje'] = planas['ValorViaje'].apply(lambda x: "${:,.0f}".format(x))
    planas.sort_values(by=['Horas en patio'], ascending=False, inplace=True)
    planas = planas[['Remolque', 'CiudadDestino', 'Horas en patio', 'Horas en patio_Sistema']]
    planas.loc[:, 'CiudadDestino'] = planas['CiudadDestino'].str.replace('JALISCO', 'GUADALAJARA')   
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

dataframe_cache = TTLCache(maxsize=100, ttl=1800)  # Tamaño máximo 1 elemento, TTL de 10 minutos

def get_cached_dataframe():
    url = 'https://drive.google.com/uc?id=1h3oynOXp11tKAkNmq4SkjBR8q_ZyJa2b'
    cache_file = 'seguimiento_ternium.xlsx'
    cache_time_limit = timedelta(minutes=30)  # Duración de la caché, ajustar según sea necesario

    # Verificar si el archivo ya está en caché y es reciente
    if os.path.exists(cache_file):
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        if datetime.now() - file_mod_time < cache_time_limit:
            print("Usando archivo cacheado.")
            path = cache_file
        else:
            print("Descargando archivo nuevo.")
            path = gdown.download(url, output=cache_file, quiet=False)
    else:
        print("Descargando archivo nuevo.")
        path = gdown.download(url, output=cache_file, quiet=False)

    # Abrir el archivo en modo binario
    with open(path, 'rb') as f:
        data = io.BytesIO(f.read())  # Leer los datos del archivo y pasarlos a BytesIO

    # Ignorar las advertencias de openpyxl
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

        # Leer el archivo desde el buffer directamente en un DataFrame
        df = pd.read_excel(data)

    # Ordenar el DataFrame por la columna 'fecha de salida' en orden descendente
    df = df.sort_values(by='fecha de salida', ascending=False, na_position='last')
    df = df.groupby('Remolque')['fecha de salida'].max().reset_index()

    return df

@cached(dataframe_cache)
def planas_sac():
    return get_cached_dataframe()