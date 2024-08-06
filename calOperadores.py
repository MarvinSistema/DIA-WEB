import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from flask import render_template, Blueprint
from sklearn.metrics import DistanceMetric
import networkx as nx
from db_manager import fetch_data, fetch_data_PRO, fetch_data_DIA
from itertools import combinations
import requests
import os
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


    # Intentar crear una tabla pivote para contar la cantidad de 'Bueno', 'Malo' y 'Regular' para cada operador
    #try:
    ETAi = ETAi.pivot_table(index='NombreOperador', columns='CumpleETA', aggfunc='size', fill_value=0).reset_index()

    # Asegurar que las columnas 'Cumple' y 'No Cumple' están presentes antes de realizar cálculos
    if 'Cumple' in ETAi.columns and 'No Cumple' in ETAi.columns:
        ETAi['Cumple'] = ETAi['Cumple'].fillna(0)
        ETAi['No Cumple'] = ETAi['No Cumple'].fillna(0)
        ETAi['Calificacion SAC'] = ((ETAi['Cumple'] / (ETAi['Cumple'] + ETAi['No Cumple'])) * 10).round(0).astype(int)
    else:
        print("Las columnas necesarias 'Cumple' o 'No Cumple' no están presentes.")
        return ETAi  # Retorna el DataFrame sin la columna 'Calificacion SAC'

    # Cambiar los nombres de las columnas para reflejar los datos correctamente
    ETAi.rename(columns={
        'Cumple': 'Cumple ETA',
        'No Cumple': 'No Cumple ETA'
    }, inplace=True)

    
    return ETAi


    
    planas= planas[~planas['Remolque'].isin(DataDIA['Plana'])]
    planas = pd.merge(planas, planasSAC, on='Remolque', how='left')
    planas['Horas en patio'] = ((datetime.now() - planas['fecha de salida']).dt.total_seconds() / 3600.0).round(1)
    planas = planas.sort_values(by='Horas en patio', ascending=False)
    planas = planas[['IdSolicitud', 'Remolque', 'CiudadDestino', 'Horas en patio', 'ValorViaje']]
    planas = planas[planas['CiudadDestino'] != 'ALLENDE'] 
    
    def emparejar_misma_ciudad_mayor23(planas):
        # Filtrar las planas con más de 23 horas en el patio
        df_filtrado = planas[planas['Horas en patio'] > 23]
        
        # Listas para almacenar las parejas formadas
        parejas = []
        
        # Agrupar por 'CiudadDestino'
        destinos_agrupados = df_filtrado.groupby('CiudadDestino')
        
        for destino, grupo in destinos_agrupados:
            # Si hay más de una plana en el mismo destino
            if len(grupo) > 1:
                ids = grupo['IdSolicitud'].tolist()
                remolques = grupo['Remolque'].tolist()
                horas = grupo['Horas en patio'].tolist()
                valores = grupo['ValorViaje'].tolist()
                for i in range(0, len(remolques) - 1, 2):
                    if i + 1 < len(remolques):
                        parejas.append({
                            'Destino': destino,
                            'IdSolicitud1': ids[i],
                            'IdSolicitud2': ids[i + 1],
                            'Remolque1': remolques[i],
                            'Remolque2': remolques[i + 1],
                            'Horas en patio1': horas[i],
                            'Horas en patio2': horas[i + 1],
                            'ValorViaje1': valores[i],
                            'ValorViaje2': valores[i + 1]
                        })
 
        
        # Convertir las parejas a un DataFrame para mostrar el resultado
        parejas_df = pd.DataFrame(parejas)
        
        
        if not parejas_df.empty:
            ids_emparejados = pd.concat([parejas_df['IdSolicitud1'], parejas_df['IdSolicitud2']])
            planas_restante = planas[~planas['IdSolicitud'].isin(ids_emparejados)]
        else:
            planas_restante = planas.copy()
        
        return parejas_df, planas_restante
    
    def emparejar_destinos_cercanos_mayor23(planas, distancia_maxima):
        
        _, data_restante = emparejar_misma_ciudad_mayor23(planas)
        
       
        Ubicaciones = pd.DataFrame({
            'City': ['TEZOYUCA', 'CUAUTLA', 'CDVALLES', 'ACAPULCO', 'XALAPA,VER','AMATLANDELOSREYES', 'CUAUTLA,MORELOS','QUERETARO', 'GUADALAJARA', 'PUERTOVALLARTA', 'MAZATLAN', 'CULIACAN', 'LEON', 'MEXICO', 'SANLUISPOTOSI', 'VERACRUZ', 'TULTITLAN', 'JIUTEPEC', 'VILLAHERMOSA', 'PACHUCADESOTO', 'COLON', 'MERIDA', 'SALTILLO', 'CHIHUAHUA', 'TUXTLAGTZ', 'CORDOBA',
                        'TOLUCA', 'CIUDADHIDALGOCHP', 'CAMPECHE', 'ATITALAQUIA', 'MATAMOROS', 'ZAPOPAN', 'CIUDADCUAHUTEMOCCHH', 'MORELIA', 'TLAXCALA', 'GUADALUPE', 'SANTACRUZSON', 'LASVARAS', 'PACHUCA', 'CIUDADJUAREZ', 'TLAJOMULCO', 'PIEDRASNEGRAS', 'RAMOSARIZPE', 'ORIZABA', 'TAPACHULA', 'TEPATITLAN', 'TLAQUEPAQUE', 'TEAPEPULCO', 'LABARCA', 'ELMARQUEZ', 'CIUDADVICTORIA', 'NUEVOLAREDO', 'TIZAYUCA,HIDALGO', 'ELSALTO', 'OCOTLANJAL', 'TEZONTEPEC', 'ZAPOTILTIC', 'PASEOELGRANDE', 'POZARICA', 'JACONA', 'FRESNILLO', 'PUEBLA', 'TUXTLAGUTIERREZ', 'PLAYADELCARMEN', 'REYNOSA', 'MEXICALI', 'TEPEJIDELORODEOCAMPO',
                        'LEON', 'CUERNAVACA', 'CHETUMAL', 'CHIHUAHUA', 'SILAO', 'ACAPULCODEJUAREZ', 'AGUASCALIENTES', 'TIJUANA', 'OCOSINGO', 'MONCLOVA', 'OAXACA', 'SOLIDARIDAROO', 'JIUTEPEC', 'ELPRIETO', 'TORREON', 'HERMOSILLO', 'CELAYA', 'CANCUN', 'URUAPAN', 'ALTAMIRA', 'COATZACUALCOS', 'IRAPUATO', 'CASTAÑOS', 'DURANGO', 'COLON', 'CIUDADVALLLES', 'MANZANILLA', 'TAMPICO', 'GOMEZPALACIO', 'ZACATECAS', 'SALAMANCA', 'COMITANDEDOMINGUEZ', 'UMAN', 'TUXTEPEC', 'ZAMORA', 'CORDOBA', 'MONTERREY', 'PENJAMO', 'NOGALES', 'RIOBRAVO', 'CABORCA', 'FRONTERACOAHUILA', 'LOSMOCHIS', 'KANASIN', 'ARRIAGACHIAPAS', 'VALLEHERMOSA', 'SANJOSEITURBIDE', 'MAZATLAN', 'TEHUACAN', 'CHILTEPEC', 'CHILPANCINGODELOSBRAVO'],
            'Latitude': [19.595099, 18.831580, 22.998189, 16.889844, 19.533927, 18.846950, 18.836561, 20.592275, 20.74031, 20.655893, 23.255931, 24.800964, 21.133941, 19.440265, 22.158710, 19.19002, 19.647433, 18.891529, 17.992561, 20.106154, 20.781414, 20.984380, 25.427049, 28.643361, 16.761753, 18.890666,
                            19.271311, 14.679697, 18.833447, 20.054095, 25.845915, 20.76705, 28.431062, 19.736983, 19.500336, 25.717427, 31.239198, 28.165034, 20.13492, 31.785672, 20.488792, 28.721685, 25.594781, 18.88138, 14.950696, 20.842635, 20.646152, 19.799357, 20.313766, 20.958186, 23.786371, 27.541875, 19.863533, 20.531878, 20.380148, 19.891505, 19.641563, 20.566394, 20.576162, 19.971759, 23.215653, 19.132065, 16.801565, 20.707474, 26.128212, 32.6718, 19.943972,
                            21.188758, 18.998997, 18.561445, 31.542897, 20.968175, 16.923231, 21.942294, 32.550529, 16.922181, 26.965938, 17.128621, 20774439, 18.932162, 22.22124, 25.622625, 29.098203, 20.581304, 21.208637, 19.432413, 22.430696, 22.430608, 20.725167, 20.828685, 24.077945, 22.027654, 20.025186, 19.127328, 22.323528, 25.629602, 22.782732, 20.604713, 16.2059, 20.914188, 18.108973, 20.018848, 18.911559, 25.79573, 20.444102, 31.331515, 26.007962, 30.751014, 26.976145, 25.831174, 20.979043, 16.251855, 25.690649, 21.020823, 23.316277, 18.504335, 18.908622, 17.592174],
            'Longitude': [-98.910005, -98.943625, -99.010334, -99.830687, -96.909218, -96.914283, -98.944068, -100.394273, -103.31312, -105.221967, -106.412165, -107.390388, -101.661519, -99.206780, -100.970141, -96.196430, -99.164822, -99.181056, -92.942980, -98.759106, -100.047289, -89.620138, -100.985244, -106.056315, -93.108217, -96.932524,
                            -99.667407, -92.151656, -90.286039, -99.222389, -97.503895, -103.351047, -106.83201, -101.204422, -98.158429, -100.181515, -110.59637, -105.340582, -98.772788, -106.566775, -103.445088, -100.547409, -100.900214, -97.104977, -92.254966, -102.79309, -103.317318, -98.555426, -102.541315, -100.2477, -99.16679, -99.565339, -98.976743, -103.181408, -102.777496, -98.814611, -103.449286, -100.679298, -97.430099, -102.298419, -102.850368, -98.222853, -93.116207, -87.07644, -98.343761, -115.385465, -99.339322,
                            -101.768658, -99.257945, -88.27958, -107.90993, -101.415423, -99.825972, -102.298616, -116.875228, -92.093952, -101.400616, -97.76784, -86.986023, -99.181586, -97.917121, -103.387956, -110.978133, -100.812923, -86.837061, -102.021193, -97.947615, -94.417513, -101.378726, -101.42206, -104.66471, -99.024839, -99.025514, -104.393928, -97.88042, -103.500552, -102.573756, -101.174834, -92.132644, -89.695333, -96.141711, -102.285924, -96.98147, -100.385905, -101.730812, -110.932889, -98.122363, -112.157303, -101.436711, -108.989827, -89.5488, -93.920658, -97.810778, -100.395074, -106.478543, -97414124, -97.047666, -99.51663]
            })
        
        # Agregar coordenadas al DataFrame restante
        data_restante['CiudadDestino'] = data_restante['CiudadDestino'].str.replace(' ', '')
        data_restante = data_restante.merge(Ubicaciones, left_on='CiudadDestino', right_on='City', how='left')
        
        
        if data_restante.empty:
            return pd.DataFrame(), data_restante

        # Calcular la matriz de distancias haversine
        dist = DistanceMetric.get_metric('haversine')
        coords = np.radians(data_restante[['Latitude', 'Longitude']])
        matriz_distancia = dist.pairwise(coords) * 6371  # Convert to kilometers

        # Listas para almacenar las parejas formadas
        parejas = []

        # Iterar sobre el DataFrame restante
        i = 0
        while i < len(data_restante):
            fila_actual = data_restante.iloc[i]
            for j in range(i + 1, len(data_restante)):
                fila_siguiente = data_restante.iloc[j]
                distancia = matriz_distancia[i, j]
                if distancia <= distancia_maxima and (fila_actual['Horas en patio'] > 23 or fila_siguiente['Horas en patio'] > 23):
                    parejas.append({
                        'Destino1': fila_actual['CiudadDestino'],
                        'Destino2': fila_siguiente['CiudadDestino'],
                        'IdSolicitud1': fila_actual['IdSolicitud'],
                        'IdSolicitud2': fila_siguiente['IdSolicitud'],
                        'Remolque1': fila_actual['Remolque'],
                        'Remolque2': fila_siguiente['Remolque'],
                        'Horas en patio1': fila_actual['Horas en patio'],
                        'Horas en patio2': fila_siguiente['Horas en patio'],
                        'ValorViaje1': fila_actual['ValorViaje'],
                        'ValorViaje2': fila_siguiente['ValorViaje'],
                        'Distancia': distancia
                    })
                    # Eliminar las planas emparejadas del DataFrame original
                    data_restante = data_restante.drop([i, j])
                    data_restante = data_restante.reset_index(drop=True)
                    matriz_distancia = np.delete(matriz_distancia, [i, j], axis=0)
                    matriz_distancia = np.delete(matriz_distancia, [i, j], axis=1)
                    i -= 1
                    break
            i += 1

        # Convertir las parejas a un DataFrame para mostrar el resultado
        parejas_df = pd.DataFrame(parejas)
        
        # Verificar si parejas_df está vacío antes de intentar concatenar las columnas
        if not parejas_df.empty:
            ids_emparejados = pd.concat([parejas_df['IdSolicitud1'], parejas_df['IdSolicitud2']])
            planas_restante = data_restante[~data_restante['IdSolicitud'].isin(ids_emparejados)]
        else:
            planas_restante = data_restante.copy()
        
        return parejas_df, planas_restante
    
    def emparejar_misma_ciudad_menor23(planas):
        # Filtrar las planas con más de 23 horas en el patio
        _, planas_restante = emparejar_destinos_cercanos_mayor23(planas, distancia_maxima=190)
        df_filtrado = planas_restante.copy()
        df_filtrado = planas[planas['Horas en patio'] < 23]
        
        # Listas para almacenar las parejas formadas
        parejas = []
        
        # Agrupar por 'CiudadDestino'
        destinos_agrupados = df_filtrado.groupby('CiudadDestino')
        
        for destino, grupo in destinos_agrupados:
            # Si hay más de una plana en el mismo destino
            if len(grupo) > 1:
                ids = grupo['IdSolicitud'].tolist()
                remolques = grupo['Remolque'].tolist()
                horas = grupo['Horas en patio'].tolist()
                valores = grupo['ValorViaje'].tolist()
                for i in range(0, len(remolques) - 1, 2):
                    if i + 1 < len(remolques):
                        parejas.append({
                            'Destino': destino,
                            'IdSolicitud1': ids[i],
                            'IdSolicitud2': ids[i + 1],
                            'Remolque1': remolques[i],
                            'Remolque2': remolques[i + 1],
                            'Horas en patio1': horas[i],
                            'Horas en patio2': horas[i + 1],
                            'ValorViaje1': valores[i],
                            'ValorViaje2': valores[i + 1]
                        })
 
        # Convertir las parejas a un DataFrame para mostrar el resultado
        parejas_df = pd.DataFrame(parejas)
        
        if not parejas_df.empty:
            ids_emparejados = pd.concat([parejas_df['IdSolicitud1'], parejas_df['IdSolicitud2']])
            planas_restante = planas_restante[~planas_restante['IdSolicitud'].isin(ids_emparejados)]
        else:
            planas_restante = planas_restante.copy()
        
        return parejas_df, planas_restante
    

        return parejas_df
       
    def emparejamiento_fin(planas):
        r1,_ = emparejar_misma_ciudad_mayor23(planas)
        r2,_ = emparejar_destinos_cercanos_mayor23(planas, distancia_maxima=200)
        r3,_ = emparejar_misma_ciudad_menor23(planas)
        rf = pd.concat([r1, r2, r3], ignore_index=True)
        
        def combinar_destinos(row):
            if pd.isna(row['Destino1']) and pd.isna(row['Destino2']):
                return row['Destino'] if pd.notna(row['Destino']) else ''
            elif pd.isna(row['Destino1']):
                return row['Destino2']
            elif pd.isna(row['Destino2']):
                return row['Destino1']
            else:
                return f"{row['Destino1']} | {row['Destino2']}"
        # Aplicar la función a cada fila del DataFrame
        rf['Destino'] = rf.apply(combinar_destinos, axis=1)
        rf['Horas en patio'] = rf[['Horas en patio1', 'Horas en patio2']].max(axis=1)
        rf['ValorViaje'] = rf['ValorViaje1'] + rf['ValorViaje2']
        rf= rf.sort_values(by='Horas en patio', ascending=False)
        rf = rf[['Destino', 'Remolque1', 'Remolque2', 'Horas en patio', 'ValorViaje']]
        
        # Reiniciar el índice comenzando desde 1
        rf.reset_index(drop=True, inplace=True)
        rf.index = rf.index + 1
        
        return rf

    return emparejamiento_fin(planas)