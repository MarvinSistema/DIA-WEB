import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from flask import render_template, Blueprint
from sklearn.metrics import DistanceMetric
from db_manager import fetch_data, fetch_data_PRO, fetch_data_DIA

from concurrent.futures import ThreadPoolExecutor
from planasPorAsignar import asignacionesPasadasOp, siniestralidad, eta

calOperador = Blueprint('calOperador', __name__)
@calOperador.route('/')
def index():
    operadores, Cartas, Gasto, Km, Bloqueo, ETAs, Permisos,\
    DataDIA, MttoPrev, CheckTaller, OrAbierta  =  \
        cargar_datos()
    asignacionesPasadasOperadores=  asignacionesPasadasOp(Cartas)
    siniestroKm= siniestralidad(Gasto, Km)
    ETAi= eta(ETAs)

    operadorDis = calOperadores(
        operadores,
        asignacionesPasadasOperadores, 
        siniestroKm, 
        ETAi, 
    )
  
    operadoresFull = operadorDis.to_html()
    return render_template('calOperador.html',  operadoresFull=operadoresFull)

def cargar_datos():
    consultas = [
        ("fetch_data", "SELECT * FROM DimTableroControl"),
        ("fetch_data", """
            SELECT IdViaje, FechaSalida, Operador, Tractor, UnidadOperativa, Cliente, SubtotalMXN, Ruta, IdConvenio 
            FROM ReporteCartasPorte 
            WHERE FechaSalida > DATEADD(day, -90, GETDATE())
        """),
        ("fetch_data_PRO", """
            SELECT Reporte, Empresa, Tractor, FechaSiniestro, TotalFinal, ResponsableAfectado As NombreOperador
            FROM DimReporteUnificado
            WHERE FechaSiniestro > DATEADD(day, -90, GETDATE())
        """),
        ("fetch_data", """
            SELECT NombreOperador, FechaPago, Tractor, KmsReseteo  
            FROM DimRentabilidadLiquidacion 
            WHERE FechaPago > DATEADD(day, -90, GETDATE())
        """),
        ("fetch_data", """
            SELECT NombreOperador, Activo, OperadorBloqueado
            FROM DimOperadores
            WHERE Activo = 'Si'
        """),
        ("fetch_data", """
            SELECT NombreOperador, FechaFinalizacion, CumpleETA 
            FROM DimIndicadoresOperaciones 
            WHERE FechaLlegada > DATEADD(day, -90, GETDATE()) 
            AND FechaLlegada IS NOT NULL
        """),
        ("fetch_data", """
            SELECT NoOperador, Nombre, Activo, FechaBloqueo
            FROM DimBloqueosTrafico
        """),
        ("fetch_data_DIA", "SELECT * FROM DIA_NYC"),
        ("fetch_data", """
            SELECT ClaveTractor, UltimoMantto, Descripcion,
                CASE 
                    WHEN VencimientoD > VencimientoK THEN VencimientoD 
                    ELSE VencimientoK 
                END AS VencimientoD
            FROM DimPreventivoFlotillas
            WHERE VencimientoD > 0.97 OR VencimientoK > 0.97
            ORDER BY VencimientoD ASC
        """),
        ("fetch_data", """
            SELECT Tractor, UnidadOperativa, Estatus, FechaEstatus
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY Tractor ORDER BY FechaEstatus DESC) as rn
                FROM DimDashboardHistorico
                WHERE Tractor != ''
            ) t
            WHERE rn <= 5
            AND (Estatus = 'Check Express' OR Estatus = 'En Taller')
            ORDER BY Tractor ASC, FechaEstatus DESC
        """),
        ("fetch_data", """
            SELECT IdDimOrdenReparacion, IdOR, TipoEquipo, ClaveEquipo, FechaCreacion, FechaFinalizacion
            FROM DimOrdenesReparacion
            WHERE FechaFinalizacion IS NULL AND TipoEquipo = 'Tractor'
        """)
    ]

    with ThreadPoolExecutor() as executor:
        # Lanzar cada consulta en un hilo separado
        futures = [executor.submit(globals()[func], sql) for func, sql in consultas]
        # Esperar a que todas las consultas se completen y recoger los resultados
        results = [future.result() for future in futures]

    return tuple(results)

def calOperadores(operadores, asignacionesPasadasOp, siniestroKm, ETAi):
    calOperador= operadores.copy()
    calOperador= pd.merge(calOperador, asignacionesPasadasOp, left_on='Operador', right_on='Operador', how='left')
    calOperador= pd.merge(calOperador, siniestroKm, left_on='Operador', right_on='NombreOperador', how='left')
    calOperador= pd.merge(calOperador, ETAi, left_on='Operador', right_on='NombreOperador', how='left')
    
    #Control Valores FAltantes    
    if 'Calificacion SAC' not in calOperador.columns:
        calOperador['Calificacion SAC'] = 0  
    calOperador['ViajeCancelado']= 20
    # Generar números aleatorios entre 25 y 50 para op nuevos 
    random_values = np.random.randint(25, 51, size=len(calOperador))
    # Convertir el ndarray en una serie de pandas
    random_series = pd.Series(random_values, index=calOperador.index)
    # Reemplazar los valores nulos con los valores aleatorios generados
    calOperador['CalificacionVianjesAnteiores'] = calOperador['CalificacionVianjesAnteiores'].fillna(random_series)
    calOperador= calOperador[(calOperador['Operador'].notna()) & (calOperador['Bueno'].notna())]
    calOperador['PuntosSiniestros'] = calOperador['PuntosSiniestros'].fillna(20)
    calOperador['Calificacion SAC'] = calOperador['Calificacion SAC'].fillna(10)
    
    calOperador['CalFinal'] = (
    calOperador['CalificacionVianjesAnteiores'] +
    calOperador['PuntosSiniestros'] +
    calOperador['Calificacion SAC'] +
    calOperador['ViajeCancelado'] 
    )
    

    calOperador = calOperador[['Operador', 'Bueno',	'Malo',	'Regular', 'CalificacionVianjesAnteiores',
                               'PuntosSiniestros', 'Calificacion SAC', 'ViajeCancelado', 'CalFinal']]
    


    calOperador = calOperador.reset_index(drop=True)
    calOperador.index = calOperador.index + 1
    
    return calOperador
