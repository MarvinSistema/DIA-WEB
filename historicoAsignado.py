from planasPorAsignar import asignacionesPasadasOp
from db_manager import fetch_data
from flask import Flask, render_template, Blueprint

historicoAsignado= Blueprint('historicoAsignado', __name__)
@historicoAsignado.route('/')
def index():
    Cartas  = cargar_datos()
    asignacionesPasadasOperadores=  asignacionesPasadasOp(Cartas)
    ajuste = ajusteTablaAsignacion(asignacionesPasadasOperadores)
    datos_html_operadores = ajuste.to_html()
    return render_template('historicoAsignado.html', datos_html=datos_html_operadores)

def cargar_datos():
    ConsultaCartas = f"SELECT * FROM ReporteCartasPorte WHERE FechaSalida > '2024-04-01'"
    Cartas = fetch_data(ConsultaCartas)
    return Cartas

def ajusteTablaAsignacion(asignacionesPasadasOperadores):
    asignacionesPasadasOperadores= asignacionesPasadasOperadores[['Operador', 'Bueno', 'Malo', 'Regular', 'CalificacionVianjesAnteiores']]
    asignacionesPasadasOperadores.rename(columns={'CalificacionVianjesAnteiores': 'Puntaje'}, inplace=True)
    return asignacionesPasadasOperadores