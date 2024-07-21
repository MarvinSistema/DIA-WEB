import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from flask import render_template, Blueprint
from sklearn.metrics import DistanceMetric
import networkx as nx
from db_manager import fetch_data, fetch_data_PRO, fetch_data_DIA
from planasEnPatio import procesar_operadores, planas_sac
from itertools import combinations
import requests
import os
from concurrent.futures import ThreadPoolExecutor

planasPorAsignar = Blueprint('planasPorAsignar', __name__)
@planasPorAsignar.route('/')
def index():
    planas, Operadores, Cartas, Gasto, Km, Bloqueo, ETAs, Permisos,\
    DataDIA, MttoPrev, CheckTaller, OrAbierta  =  \
        cargar_datos()
    planasSAC = planas_sac()
    operadores_sin_asignacion = procesar_operadores(Operadores, DataDIA)
    matchPlanas= procesar_planas(planas, DataDIA, planasSAC)
    asignacionesPasadasOperadores=  asignacionesPasadasOp(Cartas)
    siniestroKm= siniestralidad(Gasto, Km)
    ETAi= eta(ETAs)
    PermisosOp= permisosOperador(Permisos)
    cerca = cercaU()
    operadorDis = calOperador(
        operadores_sin_asignacion, 
        Bloqueo, 
        asignacionesPasadasOperadores, 
        siniestroKm, 
        ETAi, 
        PermisosOp, 
        cerca, 
        DataDIA, 
        OrAbierta,
        MttoPrev,
        CheckTaller
    )
    operadoresFull =operadorDis.to_html()
    empates_dobles = matchPlanas.to_html()
    return render_template('planasPorAsignar.html',  operadoresFull=operadoresFull, datos_html_empates_dobles=empates_dobles)


def cargar_datos():
    consultas = [
        ("fetch_data", """
            SELECT *
            FROM DimTableroControlRemolque_CPatio
            WHERE PosicionActual = 'NYC'
            AND Estatus = 'CARGADO EN PATIO'
            AND Ruta IS NOT NULL
            AND CiudadDestino NOT IN ('MONTERREY', 'GUADALUPE', 'APODACA')
            AND Remolque != 'P3169'
        """),
        ("fetch_data", "SELECT * FROM DimTableroControl_Disponibles"),
        ("fetch_data", """
            SELECT IdViaje, FechaSalida, Operador, Tractor, UnidadOperativa, Cliente, SubtotalMXN, Ruta, IdConvenio 
            FROM ReporteCartasPorte 
            WHERE FechaSalida > DATEADD(day, -90, GETDATE())
        """),
        ("fetch_data_PRO", """
            SELECT Reporte, Empresa, Tractor, FechaSiniestro, TotalFinal
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
            SELECT ClaveTractor, UltimoMantto, Descripcion, VencimientoD
            FROM DimPreventivoFlotillas
            WHERE VencimientoD>0.97
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



def procesar_planas(planas, DataDIA, planasSAC):
    """
    Procesa las planas para encontrar combinaciones de viajes optimizados.

    Parámetros:
    planas (pd.DataFrame): DataFrame con información de las planas.

    Retorna:
    pd.DataFrame: DataFrame con las combinaciones de viajes optimizadas.
    """  
    # Constantes
    distanciaMaxima = 220
    hourasMaxDifDestino = 21
    planas= planas[~planas['Remolque'].isin(DataDIA['Plana'])]
    planas = pd.merge(planas, planasSAC, on='Remolque', how='left')
    planas['Horas en patio'] = ((datetime.now() - planas['fecha de salida']).dt.total_seconds() / 3600.0).round(1)
  
        
    def mismoDestino(planas):
        # Clasificacion de Planas
        planas['Clasificado'], _ = pd.factorize(planas['CiudadDestino'], sort=True)

        # Asignar cuales son las ciudades destino repetidas, estos son las planas que se pueden empatar
        filas_repetidas = planas[planas.duplicated(subset='Clasificado', keep=False)]

        # Orden Clsificacion Planas
        filas_repetidas = filas_repetidas.sort_values(by=['Clasificado', 'Horas en patio'], ascending=[True, False])

        # Crear una lista para almacenar las combinaciones únicas sin repetición de remolques
        combinaciones_remolques  = []

        # Iterar sobre las filas repetidas y combinarlas de dos en dos
        i = 0
        while i < len(filas_repetidas):
            # Asegúrate de que no estás en la última fila
            if i < len(filas_repetidas) - 1: 
                # Verifica si la fila actual y la siguiente tienen el mismo clasificado
                if filas_repetidas['Clasificado'].iloc[i] == filas_repetidas['Clasificado'].iloc[i + 1]:
                    row1 = filas_repetidas.iloc[i]
                    row2 = filas_repetidas.iloc[i + 1]

                    # Agrega la combinación de filas a la lista
                    combinaciones_remolques .append([
                        row1['CiudadDestino'], 
                        row1['Horas en patio'], 
                        row2['Horas en patio'], 
                        row1['Remolque'], 
                        row2['Remolque'], 
                        row1['ValorViaje'], 
                        row2['ValorViaje']
                    ])
                    i += 1  # Salta la siguiente fila para evitar duplicar el emparejamiento

            i += 1  # Incrementa i para continuar al siguiente par

        # Crear un nuevo DataFrame con las combinaciones emparejadas
        df_mismoDestino = pd.DataFrame(combinaciones_remolques , columns=[
        'Ruta', 'Horas en patio_a', 'Horas en patio_b', 
        'remolque_a','remolque_b','ValorViaje_a', 'ValorViaje_b'
        ])
        df_mismoDestino['Ruta'] = 'MONTERREY-' + df_mismoDestino['Ruta']
        
        return df_mismoDestino, planas
    
        
    def diferentesDestino(planas):
        Ubicaciones = pd.DataFrame({
            'City': ['XALAPA,VER','AMATLANDELOSREYES', 'CUAUTLA,MORELOS','QUERETARO', 'GUADALAJARA', 'PUERTOVALLARTA', 'MAZATLAN', 'CULIACAN', 'LEON', 'MEXICO', 'SANLUISPOTOSI', 'VERACRUZ', 'TULTITLAN', 'JIUTEPEC', 'VILLAHERMOSA', 'PACHUCADESOTO', 'COLON', 'MERIDA', 'SALTILLO', 'CHIHUAHUA', 'TUXTLAGTZ', 'CORDOBA',
                        'TOLUCA', 'CIUDADHIDALGOCHP', 'CAMPECHE', 'ATITALAQUIA', 'MATAMOROS', 'ZAPOPAN', 'CIUDADCUAHUTEMOCCHH', 'MORELIA', 'TLAXCALA', 'GUADALUPE', 'SANTACRUZSON', 'LASVARAS', 'PACHUCA', 'CIUDADJUAREZ', 'TLAJOMULCO', 'PIEDRASNEGRAS', 'RAMOSARIZPE', 'ORIZABA', 'TAPACHULA', 'TEPATITLAN', 'TLAQUEPAQUE', 'TEAPEPULCO', 'LABARCA', 'ELMARQUEZ', 'CIUDADVICTORIA', 'NUEVOLAREDO', 'TIZAYUCA,HIDALGO', 'ELSALTO', 'OCOTLANJAL', 'TEZONTEPEC', 'ZAPOTILTIC', 'PASEOELGRANDE', 'POZARICA', 'JACONA', 'FRESNILLO', 'PUEBLA', 'TUXTLAGUTIERREZ', 'PLAYADELCARMEN', 'REYNOSA', 'MEXICALI', 'TEPEJIDELORODEOCAMPO',
                        'LEON', 'CUERNAVACA', 'CHETUMAL', 'CHIHUAHUA', 'SILAO', 'ACAPULCODEJUAREZ', 'AGUASCALIENTES', 'TIJUANA', 'OCOSINGO', 'MONCLOVA', 'OAXACA', 'SOLIDARIDAROO', 'JIUTEPEC', 'ELPRIETO', 'TORREON', 'HERMOSILLO', 'CELAYA', 'CANCUN', 'URUAPAN', 'ALTAMIRA', 'COATZACUALCOS', 'IRAPUATO', 'CASTAÑOS', 'DURANGO', 'COLON', 'CIUDADVALLLES', 'MANZANILLA', 'TAMPICO', 'GOMEZPALACIO', 'ZACATECAS', 'SALAMANCA', 'COMITANDEDOMINGUEZ', 'UMAN', 'TUXTEPEC', 'ZAMORA', 'CORDOBA', 'MONTERREY', 'PENJAMO', 'NOGALES', 'RIOBRAVO', 'CABORCA', 'FRONTERACOAHUILA', 'LOSMOCHIS', 'KANASIN', 'ARRIAGACHIAPAS', 'VALLEHERMOSA', 'SANJOSEITURBIDE', 'MAZATLAN', 'TEHUACAN', 'CHILTEPEC', 'CHILPANCINGODELOSBRAVO'],
            'Latitude': [19.533927, 18.846950, 18.836561, 20.592275, 20.74031, 20.655893, 23.255931, 24.800964, 21.133941, 19.440265, 22.158710, 19.19002, 19.647433, 18.891529, 17.992561, 20.106154, 20.781414, 20.984380, 25.427049, 28.643361, 16.761753, 18.890666,
                            19.271311, 14.679697, 18.833447, 20.054095, 25.845915, 20.76705, 28.431062, 19.736983, 19.500336, 25.717427, 31.239198, 28.165034, 20.13492, 31.785672, 20.488792, 28.721685, 25.594781, 18.88138, 14.950696, 20.842635, 20.646152, 19.799357, 20.313766, 20.958186, 23.786371, 27.541875, 19.863533, 20.531878, 20.380148, 19.891505, 19.641563, 20.566394, 20.576162, 19.971759, 23.215653, 19.132065, 16.801565, 20.707474, 26.128212, 32.6718, 19.943972,
                            21.188758, 18.998997, 18.561445, 31.542897, 20.968175, 16.923231, 21.942294, 32.550529, 16.922181, 26.965938, 17.128621, 20774439, 18.932162, 22.22124, 25.622625, 29.098203, 20.581304, 21.208637, 19.432413, 22.430696, 22.430608, 20.725167, 20.828685, 24.077945, 22.027654, 20.025186, 19.127328, 22.323528, 25.629602, 22.782732, 20.604713, 16.2059, 20.914188, 18.108973, 20.018848, 18.911559, 25.79573, 20.444102, 31.331515, 26.007962, 30.751014, 26.976145, 25.831174, 20.979043, 16.251855, 25.690649, 21.020823, 23.316277, 18.504335, 18.908622, 17.592174],
            'Longitude': [-96.909218, -96.914283, -98.944068, -100.394273, -103.31312, -105.221967, -106.412165, -107.390388, -101.661519, -99.206780, -100.970141, -96.196430, -99.164822, -99.181056, -92.942980, -98.759106, -100.047289, -89.620138, -100.985244, -106.056315, -93.108217, -96.932524,
                            -99.667407, -92.151656, -90.286039, -99.222389, -97.503895, -103.351047, -106.83201, -101.204422, -98.158429, -100.181515, -110.59637, -105.340582, -98.772788, -106.566775, -103.445088, -100.547409, -100.900214, -97.104977, -92.254966, -102.79309, -103.317318, -98.555426, -102.541315, -100.2477, -99.16679, -99.565339, -98.976743, -103.181408, -102.777496, -98.814611, -103.449286, -100.679298, -97.430099, -102.298419, -102.850368, -98.222853, -93.116207, -87.07644, -98.343761, -115.385465, -99.339322,
                            -101.768658, -99.257945, -88.27958, -107.90993, -101.415423, -99.825972, -102.298616, -116.875228, -92.093952, -101.400616, -97.76784, -86.986023, -99.181586, -97.917121, -103.387956, -110.978133, -100.812923, -86.837061, -102.021193, -97.947615, -94.417513, -101.378726, -101.42206, -104.66471, -99.024839, -99.025514, -104.393928, -97.88042, -103.500552, -102.573756, -101.174834, -92.132644, -89.695333, -96.141711, -102.285924, -96.98147, -100.385905, -101.730812, -110.932889, -98.122363, -112.157303, -101.436711, -108.989827, -89.5488, -93.920658, -97.810778, -100.395074, -106.478543, -97414124, -97.047666, -99.51663]
        })

        df_mismoDestino, planas  = mismoDestino(planas)

        #PlanasyaAsignadas = df_mismoDestino.copy()
        mismoDestino_concat = pd.concat([df_mismoDestino['remolque_a'], df_mismoDestino['remolque_b']], ignore_index=True)
        PlanasTotales_no_asignadas = planas[~planas['Remolque'].isin(mismoDestino_concat)].copy()
        PlanasTotales_no_asignadas.loc[:, 'City'] = PlanasTotales_no_asignadas['CiudadDestino'].str.replace(' ', '', regex=True)

        # Merge de DataFrames, seleccionando directamente las columnas deseadas
        df = pd.merge(PlanasTotales_no_asignadas, Ubicaciones, on='City', how='inner')[['City', 'Latitude', 'Longitude']]

        # Convertir coordenadas a radianes
        df[['Latitude', 'Longitude']] = np.radians(df[['Latitude', 'Longitude']])

        # Calculate the distance matrix
        dist = DistanceMetric.get_metric('haversine')
        matriz_distacia = dist.pairwise(df[['Latitude', 'Longitude']]) * 6371  # Convert to kilometers

        def crear_grafo_y_emparejamientos(df, distanciaMaxima):
            G = nx.Graph()
            added_edges = False
            for index, row in df.iterrows():
                G.add_node(row['City'])

            for i in range(len(df)):
                for j in range(i + 1, len(df)):
                    if matriz_distacia[i][j] <= distanciaMaxima:
                        G.add_edge(df.iloc[i]['City'], df.iloc[j]['City'], weight=matriz_distacia[i][j])
                        added_edges = True

            if not added_edges:
                return pd.DataFrame()  # Retorna un DataFrame vacío si no hay aristas

            matching = nx.algorithms.matching.min_weight_matching(G)
            if not matching:
                return pd.DataFrame()  # Asegurar que se maneje un conjunto de emparejamientos vacío
        
            matching_df = pd.DataFrame(list(matching), columns=['City1', 'City2'])
            matching_df['Distance'] = matching_df.apply(lambda x: G[x['City1']][x['City2']]['weight'], axis=1)
            return matching_df

        matching_df = crear_grafo_y_emparejamientos(df, distanciaMaxima)
    
        # Vamos a asignar un ID igual a cada par de ciudades y mostrarlas en una sola columana
        results = []
        # Recorrer cada fila y descomponer las ciudades en filas individuales
        for index, row in matching_df.iterrows():
            results.append({'Destino': row['City1'], 'IDe': index + 1})
            results.append({'Destino': row['City2'], 'IDe': index + 1})
        # Convertir la lista de resultados en un nuevo DataFrame
        paresAdiferenteCiudad= pd.DataFrame(results)

        noAsignadas = PlanasTotales_no_asignadas.copy()

        #Concatenar noAsignadas con paresAdiferenteCiudad(aqui adjuntamos los ID pares de cada ciudad al dataframe de planas sn asignar)
        if 'Destino' not in paresAdiferenteCiudad.columns:
            print("No se encontraron emparejamientos o la columna 'Destino' no existe en paresAdiferenteCiudad.")
            columnas = [
                'remolque_a', 'remolque_b', 'ValorViaje_a', 'ValorViaje_b',
                'Horas en patio_a', 'Horas en patio_b', 'Ruta'
            ]

            # Crear el DataFrame vacío con las columnas definidas
            diferentesDestino_df = pd.DataFrame(columns=columnas)

            combined_df=noAsignadas.copy()
            combined_df['IDe'] = np.nan 
            
        else:
            combined_df = pd.merge(noAsignadas, paresAdiferenteCiudad, how='left', left_on='City', right_on='Destino')
            combined_df = combined_df [['Remolque', 'Ruta', 'ValorViaje', 'IDe', 'Horas en patio']]
            combined_df.sort_values(by= 'IDe', ascending=True, inplace=True)
            
            # Crear una lista para almacenar las combinaciones únicas sin repetición de remolques
            filas_empate_doble = []
            # Iterar sobre las filas repetidas y combinarlas de dos en dos
            i = 0
            while i < len(combined_df):
                if i < len(combined_df) - 1: 
                    if combined_df['IDe'].iloc[i] == combined_df['IDe'].iloc[i+1]:
                        row1 = combined_df.iloc[i]
                        
                        row2 = combined_df.iloc[i + 1]
                        
                        filas_empate_doble.append([row1['IDe'], row1['Remolque'], row2['Remolque'], row1['ValorViaje'], row2['ValorViaje'], row1['Horas en patio'], row2['Horas en patio'], row1['Ruta'], row2['Ruta']])
                        i += 1  # Incrementa i solo si se cumple la condición
                i += 1  # Incrementa i en cada iteración del bucle while

            # Crear un nuevo DataFrame con las combinaciones únicas sin repetición de remolques
            df_empates_dobles = pd.DataFrame(filas_empate_doble, columns=['IDe', 'remolque_a', 'remolque_b', 'ValorViaje_a','ValorViaje_b','Horas en patio_a', 'Horas en patio_b', 'Ruta1', 'Ruta2'])

             
            # Crea una columna nueva y luego seleccionar columnas
            df_empates_dobles['Ruta'] = df_empates_dobles.apply(lambda x: f"{x['Ruta1']} | {x['Ruta2']}", axis=1)
            df_empates_dobles = df_empates_dobles[['remolque_a', 'remolque_b', 'ValorViaje_a', 'ValorViaje_b', 'Horas en patio_a', 'Horas en patio_b', 'Ruta']]
            
            return df_empates_dobles, combined_df

        
    def matchFinal(planas):
        # Se obtienen los dataframes de cada función
        df_mismoDestino, planas = mismoDestino(planas)
        diferentesDestino_df, combined_df = diferentesDestino(planas)
        
        # Filtrar DataFrames vacíos o con solo valores NA
        dataframes = [df_mismoDestino, diferentesDestino_df]
        valid_dataframes = [df for df in dataframes if not df.empty and not df.isna().all().all()]
        # Concatenar solo los DataFrames válidos
        if valid_dataframes:
            df_concatenado = pd.concat(valid_dataframes, ignore_index=True)
        else:
            df_concatenado = pd.DataFrame()  # Crear un DataFrame vacío si no hay DataFrames válidos
        


       # Crear un diccionario con las columnas seleccionadas y el valor 0 para rellenar
        columns_to_fill = {
            'remolque_a': 0,
            'remolque_b': 0,
            'ValorViaje_a': 0,
            'ValorViaje_b': 0
        }

        # Llenar NaN con ceros solo en las columnas seleccionadas
        df_concatenado.fillna(value=columns_to_fill, inplace=True)
        
        # Calcular valor total del viaje
        df_concatenado['Monto'] = df_concatenado['ValorViaje_a'] + df_concatenado['ValorViaje_b']
        df_concatenado['Horas en Patio'] = df_concatenado[['Horas en patio_a', 'Horas en patio_b']].max(axis=1)
               
        #Calcular horas en patio
        df_concatenado= df_concatenado[df_concatenado['Ruta'] != 'MONTERREY-ALLENDE']
   
        df_concatenado = df_concatenado.sort_values(by='Horas en Patio', ascending=False)
        
        df_concatenado.reset_index(drop=True, inplace=True)
        df_concatenado.index = df_concatenado.index + 1
        df_concatenado = df_concatenado[['Ruta', 'remolque_a', 'remolque_b', 'Monto', 'Horas en Patio']]
        
        return df_concatenado
   
    return matchFinal(planas)
    
def calOperador(operadores_sin_asignacion, Bloqueo, asignacionesPasadasOp, siniestroKm, ETAi, PermisosOp, cerca, DataDIA, OrAbierta, MttoPrev, CheckTaller):
    calOperador= operadores_sin_asignacion.copy()
    #calOperador =calOperador[calOperador['Tiempo Disponible'] > 0.2]
    calOperador= pd.merge(operadores_sin_asignacion, Bloqueo, left_on='Operador', right_on='NombreOperador', how='left')
    calOperador= calOperador[calOperador['Tractor'].isin(cerca['cve_uni'])]
    calOperador= calOperador[~calOperador['Operador'].isin(DataDIA['Operador'])]
    calOperador= pd.merge(calOperador, asignacionesPasadasOp, left_on='Operador', right_on='Operador', how='left')
    calOperador= pd.merge(calOperador, siniestroKm, left_on='Tractor', right_on='Tractor', how='left')
    calOperador= pd.merge(calOperador, PermisosOp, left_on='Operador', right_on='Nombre', how='left')
    calOperador= pd.merge(calOperador, ETAi, left_on='Operador', right_on='NombreOperador', how='left')
    calOperador['OrAbierta'] = calOperador['Tractor'].apply(
    lambda x: 'Si' if x in OrAbierta['ClaveEquipo'].values else 'No'
    )
    calOperador['Pasar a Mtto Preventivo'] = calOperador['Tractor'].apply(
    lambda x: 'Si' if x in MttoPrev['ClaveTractor'].values else 'No'
    )
    calOperador['¿Paso por Check/Mtto?'] = calOperador['Tractor'].apply(
    lambda x: 'Si' if x in CheckTaller['Tractor'].values else 'No'
    )
    
    
    #Control Valores FAltantes    
    if 'Calificacion SAC' not in calOperador.columns:
        calOperador['Calificacion SAC'] = 0  
    calOperador['ViajeCancelado']= 20
    calOperador['Activo_y'] = calOperador['Activo_y'].fillna('No')
     # Generar números aleatorios entre 25 y 50 para op nuevos 
    random_values = np.random.randint(25, 51, size=len(calOperador))
    # Convertir el ndarray en una serie de pandas
    random_series = pd.Series(random_values, index=calOperador.index)
    # Reemplazar los valores nulos con los valores aleatorios generados
    calOperador['CalificacionVianjesAnteiores'] = calOperador['CalificacionVianjesAnteiores'].fillna(random_series)
    calOperador['Puntos Siniestros'] = calOperador['PuntosSiniestros'].fillna(20)
    
    
    calOperador['CalFinal'] = (
    calOperador['CalificacionVianjesAnteiores'] +
    calOperador['PuntosSiniestros'] +
    calOperador['Calificacion SAC'] +
    calOperador['ViajeCancelado'] +
    (calOperador['Tiempo Disponible'] * 0.4)
    )
    
    
    calOperador = calOperador[['Operador', 'Tractor', 'UOperativa', 'Activo_y', 'OperadorBloqueado', 'OrAbierta',
        'Pasar a Mtto Preventivo', '¿Paso por Check/Mtto?',  'Tiempo Disponible', 'CalFinal']]
    
        
    calOperador = calOperador.rename(columns={
    'UOperativa_x': 'Operativa',
    'OperadorBloqueado': 'Bloqueado Por Seguridad',
    'Activo_y': 'Permiso'
    })


    calOperador =calOperador.sort_values(by=['Bloqueado Por Seguridad', 'CalFinal'], ascending=[True, False])
    calOperador = calOperador.reset_index(drop=True)
    calOperador.index = calOperador.index + 1
    
    return calOperador

def asignacionesPasadasOp(Cartas):
    CP= Cartas.copy()
    # 30 dias atras
    fecha_actual = datetime.now()
    fecha_75_dias_atras = fecha_actual - timedelta(days=75)
    # Dividir la columna 'Ruta' por '||' y luego por '-' para obtener origen
    CP[['ID1', 'Ciudad_Origen', 'Ciudad_Destino']] = CP['Ruta'].str.split(r' \|\| | - ', expand=True)
    # Filtro Mes actual, UO, ColumnasConservadas, Ciudad Origen
    CP = CP[CP['FechaSalida'] >= fecha_75_dias_atras]
    CP = CP[CP['UnidadOperativa'].isin(['U.O. 01 ACERO', 'U.O. 02 ACERO', 'U.O. 03 ACERO', 'U.O. 04 ACERO', 'U.O. 39 ACERO', 'U.O. 07 ACERO', 'U.O. 15 ACERO (ENCORTINADOS)', 'U.O. 52 ACERO (ENCORTINADOS SCANIA)',  'U.O. 41 ACERO LOCAL (BIG COIL)'])]
    CP= CP[['IdViaje', 'Cliente', 'Operador', 'Ruta', 'SubtotalMXN', 'FechaSalida', 'Ciudad_Origen']]
    CP = CP[CP['Ciudad_Origen'].isin(['MONTERREY'])]

    # Agrupar 
    CP = CP.groupby(['IdViaje', 'Operador']).agg({'SubtotalMXN': 'sum'}).reset_index()

    # Funsion para determinar tipo de viaje
    def etiquetar_tipo_viaje(subtotal):
        if subtotal >= 105000:
            return "Bueno"
        elif subtotal <= 81000:
            return "Malo"
        else:
            return "Regular"

    # Aplicar la función a la columna 'SubtotalMXN' para crear la columna 'TipoViaje'
    CP['TipoViaje'] = CP['SubtotalMXN'].apply(lambda subtotal: etiquetar_tipo_viaje(subtotal))

    # Crear una tabla pivote para contar la cantidad de 'Bueno', 'Malo' y 'Regular' para cada operador
    CP = pd.pivot_table(CP, index='Operador', columns='TipoViaje', aggfunc='size', fill_value=0)

   

    # Calcula el puntaje bruto para cada fila
    CP['PuntajeBruto'] = (CP['Malo'] * 0.5)+ (CP['Regular'] * 1) + (CP['Bueno'] * 2)

    # Calcula el máximo y mínimo puntaje bruto que podría existir basado en los datos del DataFrame
    min_puntaje_posible = CP['PuntajeBruto'].min()
    max_value = CP['PuntajeBruto'].max()

    # Calcular 'CalificacionVianjesAnteiores'
    CP['CalificacionVianjesAnteiores'] = 50 - (1 + (49 * (CP['PuntajeBruto'] - min_puntaje_posible) / (max_value - min_puntaje_posible)))
    # Reemplazar valores infinitos por 0
    CP['CalificacionVianjesAnteiores'] = CP['CalificacionVianjesAnteiores'].replace([float('inf'), -float('inf')], 0)
    # Redondear y convertir a entero
    CP['CalificacionVianjesAnteiores'] = CP['CalificacionVianjesAnteiores'].round().astype(int)


    CP = CP.reset_index()
    return CP

def siniestralidad(Gasto, Km):
    G= Gasto.copy()
    K = Km.copy()

    # Filtrar las filas que contengan "SINIESTRO" en la columna "Reporte", la empresa NYC
    G = G[G['Reporte'].str.contains("SINIESTRO")]
    G= G[G['Empresa'].str.contains("NYC")]

    # Quedarme con tres meses de historia hacia atras a partir de hoy, mantener las columnas
    G= G[pd.to_datetime(G['FechaSiniestro']).dt.date >= (datetime.now() - timedelta(days=3*30)).date()]
    G= G[["Tractor","TotalFinal"]]

    # Agrupar Por Tractor
    G = G.groupby('Tractor')['TotalFinal'].sum()

    # Resetear Index
    G= G.reset_index()

    # Quedarse con columnas
    K = K[["Tractor", "FechaPago", "KmsReseteo"]]

    # Quedarme con tres meses de historia hacia atras a partir de hoy
    K = K[pd.to_datetime(K['FechaPago']).dt.date >= (datetime.now() - timedelta(days=3*30)).date()]

    # Agrupar por la columna "Tractor" y sumar los valores de la columna "KmsReseteo"
    K= K.groupby('Tractor')['KmsReseteo'].sum()

    # Voy a pasarlo a un dataframe 
    K = K.reset_index()

    # Realizar left join entre kilometros y gasto
    K= K.merge(G, on='Tractor', how='left')

    # Rellenar los valores NaN en la columna "totalfinal" con ceros (0)
    K['TotalFinal'] = K['TotalFinal'].fillna(0)

    # Agregar una nueva columna llamada "SINIESTRALIDAD" al DataFrame resultado_join
    K['Siniestralidad'] = (K['TotalFinal'] / K['KmsReseteo']).round(2)

    # Ordenar el DataFrame resultado_join de menor a mayor por la columna "SINIESTRALIDAD"
    K= K.sort_values(by='Siniestralidad')

    # Funsion para asiganar puntaje
    def Sis(SiniestroP):
        if SiniestroP >= 0.15:
            return 0
        elif SiniestroP >= 0.06:
            return 10
        else:
            return 20
        
    # Asignar los puntajes por fila
    K['PuntosSiniestros'] = K['Siniestralidad'].apply(lambda SiniestroP: Sis(SiniestroP))

    #voy a pasarlo a un dataframe 
    K = K.reset_index()
    return K

def permisosOperador(Permisos):
    Permisos= Permisos.sort_values(by=['Nombre', 'FechaBloqueo'], ascending=[False, False])
    Permisos= Permisos.drop_duplicates(subset='Nombre', keep='first')
    Permisos = Permisos.query('Activo == "Si" & ~(Activo == "No")')
    return Permisos

def eta(ETAi):
    # Intentar crear una tabla pivote para contar la cantidad de 'Bueno', 'Malo' y 'Regular' para cada operador
    #try:
    ETAi = ETAi.pivot_table(index='NombreOperador', columns='CumpleETA', aggfunc='size', fill_value=0).reset_index()

    # Asegurar que las columnas 'Cumple' y 'No Cumple' están presentes antes de realizar cálculos
    if 'Cumple' in ETAi.columns and 'No Cumple' in ETAi.columns:
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

def cercaU():
    # Paso 1: Login y obtención del token
    url_login = 'http://74.208.129.205:3000/loginUser'
    payload_login = {
        'Usuario':  os.getenv('USUARIOTRACK'),
        'Password': os.getenv('PASSWORDTRACK')
    }
    
    
    # Intento de login
    response_login = requests.post(url_login, json=payload_login)
    if response_login.status_code != 200:
        print("Error en la conexión para login:", response_login.status_code)
        print("Detalle del error:", response_login.text)
        return None

    # Extracción del token
    token = response_login.json().get('token')
    print("Conexión exitosa para login! Token obtenido.")

    # Paso 2: Obtención de datos usando el token
    url_datos = 'http://74.208.129.205:3000/clientes/GNYC/TableroDeControlSPL'
    headers_datos = {'Authorization': f'Bearer {token}'}
    body_datos = {'idEmpresa': 1}
    
    # Solicitud de datos
    response_datos = requests.post(url_datos, headers=headers_datos, json=body_datos)
    if response_datos.status_code != 200:
        print("Error en la conexión para obtener datos:", response_datos.status_code)
        print("Detalle del error:", response_datos.text)
        return None

    # Conversión de los datos a DataFrame
    datos_empresa = response_datos.json()
    cerca= pd.DataFrame(datos_empresa)
    print("Conexión exitosa para obtener datos de la empresa!")
    cerca = cerca.loc[cerca['localizacion'] == '0.00 Km. NYC MONTERREY']
    cerca= cerca[['cve_uni']]
    return cerca