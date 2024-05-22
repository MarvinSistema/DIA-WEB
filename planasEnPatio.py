from flask import Flask, render_template, Blueprint
from datetime import datetime
from db_manager import fetch_data

planasEnPatio = Blueprint('planasEnPatio', __name__)
@planasEnPatio.route('/')

def index():
    planas, Operadores = cargar_datos()
    planas = procesar_planas(planas)
    operadores_sin_asignacion = procesar_operadores(Operadores)
    datos_html_empates_dobles = planas.to_html()
    datos_html_operadores = operadores_sin_asignacion.to_html()
    return render_template('planasEnPatio.html', datos_html= datos_html_empates_dobles, datos_html_operadores=datos_html_operadores)

def cargar_datos():
    consulta_planas = consulta_planas = "SELECT * FROM DimTableroControlRemolque WHERE PosicionActual = 'NYC' AND Estatus = 'CARGADO EN PATIO' AND Ruta IS NOT NULL AND CiudadDestino != 'MONTERREY'AND CiudadDestino != 'GUADALUPE'"
    consulta_operadores = "SELECT * FROM DimTableroControl"
    planas = fetch_data(consulta_planas)
    Operadores = fetch_data(consulta_operadores)
    return planas, Operadores

def procesar_planas(planas):
    Planas_df = planas.copy()
    
    Planas_df['Horas en patio'] = ((datetime.now() - Planas_df['FechaEstatus']).dt.total_seconds() / 3600.0).round(1)
    Planas_df['FechaEstatus'] = Planas_df['FechaEstatus'].dt.strftime('%Y-%m-%d %H:%M')
    Planas_df['ValorViaje'] = Planas_df['ValorViaje'].apply(lambda x: "${:,.0f}".format(x))
    Planas_df.sort_values(by=['FechaEstatus'], ascending=True, inplace=True)
    Planas_df = Planas_df[['Remolque', 'CiudadDestino', 'ValorViaje', 'Horas en patio']]
    Planas_df.reset_index(drop=True, inplace=True)
    Planas_df.index += 1
    return Planas_df

def procesar_operadores(Operadores):
    Operadores = Operadores[(Operadores['Estatus'] == 'Disponible') & (Operadores['Destino'] == 'NYC')]
    Operadores  = Operadores [Operadores ['UOperativa'].isin(['U.O. 01 ACERO', 'U.O. 02 ACERO', 'U.O. 03 ACERO', 'U.O. 04 ACERO', 'U.O. 07 ACERO','U.O. 39 ACERO', 'U.O. 15 ACERO (ENCORTINADOS)', 'U.O. 41 ACERO LOCAL (BIG COIL)', 'U.O. 52 ACERO (ENCORTINADOS SCANIA)'])]
    Operadores['Tiempo Disponible'] = ((datetime.now() - Operadores['FechaEstatus']).dt.total_seconds() / 3600).round(1)
    Operadores = Operadores[Operadores['ObservOperaciones'].isna() | Operadores['ObservOperaciones'].eq('')]
    Operadores = Operadores[['Operador', 'Tractor', 'UOperativa', 'Tiempo Disponible']]
    Operadores.sort_values(by='Tiempo Disponible', ascending=False, inplace=True)
    Operadores.reset_index(drop=True, inplace=True)
    Operadores.index += 1
    return Operadores