#LIBRERIAS#
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask, render_template, Blueprint
from sklearn.metrics import DistanceMetric
import networkx as nx
from db_manager import fetch_data


planasPorAsignar = Blueprint('planasPorAsignar', __name__)
@planasPorAsignar.route('/')
def index():
    planas, Operadores = cargar_datos()
    planasPorAsignar = procesar_planas(planas)
    operadores_sin_asignacion = procesar_operadores(Operadores)
    datos_html_operadores = operadores_sin_asignacion.to_html()
    datos_html_empates_dobles = planasPorAsignar.to_html()
    return render_template('planasPorAsignar.html', datos_html_operadores=datos_html_operadores, datos_html_empates_dobles=datos_html_empates_dobles)

def cargar_datos():
    consulta_planas = "SELECT * FROM DimTableroControlRemolque"
    consulta_operadores = "SELECT * FROM DimTableroControl"
    planas = fetch_data(consulta_planas)
    Operadores = fetch_data(consulta_operadores)
    return planas, Operadores


def procesar_planas(planas):
  
    # PLANAS AL MISMO DESTINO#
    P = planas.copy()

    # Filtros combinados para eficiencia y claridad usando .query()
    Pa= P.query("PosicionActual == 'NYC' and Estatus == 'CARGADO EN PATIO' and Ruta.notna()")

    #Generaba un warning sin este copy
    P = Pa.copy()
    # Concatena Cliente-Ciudad
    P['Cliente-Ciudad Destino'] = P['Cliente'].str.cat(P['CiudadDestino'], sep='-')
    P= P[P['Cliente-Ciudad Destino'] != 'TERNIUM MEXICO-MONTERREY']

    # Se Mantiene Columnas
    P = P[['Cliente-Ciudad Destino', 'FechaEstatus', 'Remolque','ValorViaje']]

    # Se Ordenan
    P.sort_values(by=['FechaEstatus','Cliente-Ciudad Destino'], ascending=True, inplace=True)
    P.reset_index(drop=True, inplace=True)

    # Clasificacion de Planas
    P['Clasificado'], _ = pd.factorize(P['Cliente-Ciudad Destino'], sort=True)

    # Asignar cuales son las ciudades destino repetidas, estos son las planas que se pueden empatar
    filas_repetidas = P[P.duplicated(subset='Clasificado', keep=False)]

    # Orden Clsificacion Planas
    filas_repetidas = filas_repetidas.sort_values(by=['Clasificado', 'FechaEstatus'], ascending=[True, True])

    # Generar todas las combinaciones únicas de índices de las filas repetidas
    #combinaciones_indices = list(combinations(filas_repetidas.index, 2))

    # Crear una lista para almacenar las combinaciones únicas sin repetición de remolques
    filas_empate_doble = []

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
                filas_empate_doble.append([
                    row1['Cliente-Ciudad Destino'], 
                    row1['FechaEstatus'], 
                    row2['FechaEstatus'], 
                    row1['Remolque'], 
                    row2['Remolque'], 
                    row1['ValorViaje'], 
                    row2['ValorViaje']
                ])
                i += 1  # Salta la siguiente fila para evitar duplicar el emparejamiento

        i += 1  # Incrementa i para continuar al siguiente par

    # Crear un nuevo DataFrame con las combinaciones emparejadas
    df_empates_dobles = pd.DataFrame(filas_empate_doble, columns=[
    'Cliente-Ciudad destino', 'Fecha Estatus_a', 'Fecha Estatus_b', 
    'remolque_a','remolque_b','ValorViaje_a', 'ValorViaje_b'
    ])

    # Dividir la columna 'Cliente-Ciudad destino' por el primer espacio
    df_empates_dobles [['Cliente', 'Ruta']] = df_empates_dobles ['Cliente-Ciudad destino'].str.split(n=1, expand=True)

    # Eliminar la columna original 'Cliente-Ciudad destino'
    df_empates_dobles.drop(columns=['Cliente-Ciudad destino', 'Cliente'], inplace=True)

    # Quitar espacios y ajustar ruta para join
    df_empates_dobles['Ruta'] = df_empates_dobles['Ruta'].str.replace(r'^.*?-','MONTERREY-', regex=True)
    df_empates_dobles['Ruta'] = df_empates_dobles['Ruta'].str.replace(' ', '')

    mismoDestino = df_empates_dobles.copy()




    #PLANAS A DIFERENTE DESTINO
    Ubicaciones = pd.DataFrame({
        'City': ['AMATLANDELOSREYES', 'CUAUTLA,MORELOS','QUERETARO', 'GUADALAJARA', 'PUERTOVALLARTA', 'MAZATLAN', 'CULIACAN', 'LEON', 'MEXICO', 'SANLUISPOTOSI', 'VERACRUZ', 'TULTITLAN', 'JIUTEPEC', 'VILLAHERMOSA', 'PACHUCADESOTO', 'COLON', 'MERIDA', 'SALTILLO', 'CHIHUAHUA', 'TUXTLAGTZ', 'CORDOBA',
                    'TOLUCA', 'CIUDADHIDALGOCHP', 'CAMPECHE', 'ATITALAQUIA', 'MATAMOROS', 'ZAPOPAN', 'CIUDADCUAHUTEMOCCHH', 'MORELIA', 'TLAXCALA', 'GUADALUPE', 'SANTACRUZSON', 'LASVARAS', 'PACHUCA', 'CIUDADJUAREZ', 'TLAJOMULCO', 'PIEDRASNEGRAS', 'RAMOSARIZPE', 'ORIZABA', 'TAPACHULA', 'TEPATITLAN', 'TLAQUEPAQUE', 'TEAPEPULCO', 'LABARCA', 'ELMARQUEZ', 'CIUDADVICTORIA', 'NUEVOLAREDO', 'TIZAYUCA,HIDALGO', 'ELSALTO', 'OCOTLANJAL', 'TEZONTEPEC', 'ZAPOTILTIC', 'PASEOELGRANDE', 'POZARICA', 'JACONA', 'FRESNILLO', 'PUEBLA', 'TUXTLAGUTIERREZ', 'PLAYADELCARMEN', 'REYNOSA', 'MEXICALI', 'TEPEJIDELORODEOCAMPO',
                    'LEON', 'CUERNAVACA', 'CHETUMAL', 'CHIHUAHUA', 'SILAO', 'ACAPULCODEJUAREZ', 'AGUASCALIENTES', 'TIJUANA', 'OCOSINGO', 'MONCLOVA', 'OAXACA', 'SOLIDARIDAROO', 'JIUTEPEC', 'ELPRIETO', 'TORREON', 'HERMOSILLO', 'CELAYA', 'CANCUN', 'URUAPAN', 'ALTAMIRA', 'COATZACUALCOS', 'IRAPUATO', 'CASTAÑOS', 'DURANGO', 'COLON', 'CIUDADVALLLES', 'MANZANILLA', 'TAMPICO', 'GOMEZPALACIO', 'ZACATECAS', 'SALAMANCA', 'COMITANDEDOMINGUEZ', 'UMAN', 'TUXTEPEC', 'ZAMORA', 'CORDOBA', 'MONTERREY', 'PENJAMO', 'NOGALES', 'RIOBRAVO', 'CABORCA', 'FRONTERACOAHUILA', 'LOSMOCHIS', 'KANASIN', 'ARRIAGACHIAPAS', 'VALLEHERMOSA', 'SANJOSEITURBIDE', 'MAZATLAN', 'TEHUACAN', 'CHILTEPEC', 'CHILPANCINGODELOSBRAVO'],
        'Latitude': [18.846950, 18.836561, 20.592275, 20.74031, 20.655893, 23.255931, 24.800964, 21.133941, 19.440265, 22.158710, 19.19002, 19.647433, 18.891529, 17.992561, 20.106154, 20.781414, 20.984380, 25.427049, 28.643361, 16.761753, 18.890666,
                        19.271311, 14.679697, 18.833447, 20.054095, 25.845915, 20.76705, 28.431062, 19.736983, 19.500336, 25.717427, 31.239198, 28.165034, 20.13492, 31.785672, 20.488792, 28.721685, 25.594781, 18.88138, 14.950696, 20.842635, 20.646152, 19.799357, 20.313766, 20.958186, 23.786371, 27.541875, 19.863533, 20.531878, 20.380148, 19.891505, 19.641563, 20.566394, 20.576162, 19.971759, 23.215653, 19.132065, 16.801565, 20.707474, 26.128212, 32.6718, 19.943972,
                        21.188758, 18.998997, 18.561445, 31.542897, 20.968175, 16.923231, 21.942294, 32.550529, 16.922181, 26.965938, 17.128621, 20774439, 18.932162, 22.22124, 25.622625, 29.098203, 20.581304, 21.208637, 19.432413, 22.430696, 22.430608, 20.725167, 20.828685, 24.077945, 22.027654, 20.025186, 19.127328, 22.323528, 25.629602, 22.782732, 20.604713, 16.2059, 20.914188, 18.108973, 20.018848, 18.911559, 25.79573, 20.444102, 31.331515, 26.007962, 30.751014, 26.976145, 25.831174, 20.979043, 16.251855, 25.690649, 21.020823, 23.316277, 18.504335, 18.908622, 17.592174],
        'Longitude': [-96.914283, -98.944068, -100.394273, -103.31312, -105.221967, -106.412165, -107.390388, -101.661519, -99.206780, -100.970141, -96.196430, -99.164822, -99.181056, -92.942980, -98.759106, -100.047289, -89.620138, -100.985244, -106.056315, -93.108217, -96.932524,
                        -99.667407, -92.151656, -90.286039, -99.222389, -97.503895, -103.351047, -106.83201, -101.204422, -98.158429, -100.181515, -110.59637, -105.340582, -98.772788, -106.566775, -103.445088, -100.547409, -100.900214, -97.104977, -92.254966, -102.79309, -103.317318, -98.555426, -102.541315, -100.2477, -99.16679, -99.565339, -98.976743, -103.181408, -102.777496, -98.814611, -103.449286, -100.679298, -97.430099, -102.298419, -102.850368, -98.222853, -93.116207, -87.07644, -98.343761, -115.385465, -99.339322,
                        -101.768658, -99.257945, -88.27958, -107.90993, -101.415423, -99.825972, -102.298616, -116.875228, -92.093952, -101.400616, -97.76784, -86.986023, -99.181586, -97.917121, -103.387956, -110.978133, -100.812923, -86.837061, -102.021193, -97.947615, -94.417513, -101.378726, -101.42206, -104.66471, -99.024839, -99.025514, -104.393928, -97.88042, -103.500552, -102.573756, -101.174834, -92.132644, -89.695333, -96.141711, -102.285924, -96.98147, -100.385905, -101.730812, -110.932889, -98.122363, -112.157303, -101.436711, -108.989827, -89.5488, -93.920658, -97.810778, -100.395074, -106.478543, -97414124, -97.047666, -99.51663]
    })

    planasTotales = P.copy()
    PlanasyaAsignadas = df_empates_dobles.copy()
    PlanasyaAsignadas = pd.concat([PlanasyaAsignadas['remolque_a'], PlanasyaAsignadas['remolque_b']], ignore_index=True)

    PlanasTotales_no_asignadas = planasTotales[~planasTotales['Remolque'].isin(PlanasyaAsignadas)].copy()
    #Se extrae  Ciudad destino y se asigna a nueva columna 'City'
    PlanasTotales_no_asignadas.loc[:, 'City'] = PlanasTotales_no_asignadas['Cliente-Ciudad Destino'].str.split('-').str[1]
    PlanasTotales_no_asignadas.loc[:, 'City'] = PlanasTotales_no_asignadas['City'].str.replace(' ', '', regex=True)
    
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



    distanciaMaxima = 220  # o cualquier otro valor que consideres adecuado

    matching_df = crear_grafo_y_emparejamientos(df, distanciaMaxima)
   

    # Vamos a asignar un ID igual a cada par de ciudades y mostrarlas en una sola columana
    results = []
    # Recorrer cada fila y descomponer las ciudades en filas individuales
    for index, row in matching_df.iterrows():
        results.append({'Destino': row['City1'], 'IDe': index + 1})
        results.append({'Destino': row['City2'], 'IDe': index + 1})
    # Convertir la lista de resultados en un nuevo DataFrame
    paresAdiferenteCiudad= pd.DataFrame(results)

    #De las planas no asignadas buscamos separar 'Destino', y hacer merge con resul_df(contiene los pares a difeentes ciudades)
    noAsignadas = PlanasTotales_no_asignadas.copy()
    #Remplaza "TERNIUM MEXICO" por "MONTERREY" en la columna Cliente-Ciudad Destino y quitar espacios. Normalizamos  la columna
    # Combinar operaciones replace
    noAsignadas['Ruta'] = noAsignadas['Cliente-Ciudad Destino'].replace(['TERNIUM MEXICO', ' '], ['MONTERREY', ''], regex=True)

    #datframe temporar que contiene Origen y destino
    temp_df = noAsignadas['Ruta'].str.split('-', expand=True)
    # Se agregan las columnas Origen y destino de temp_df al datframe noAsignadas
    noAsignadas['Origen'] = temp_df[0]
    noAsignadas['Destino'] = temp_df[1]


    #Concatenar noAsignadas con paresAdiferenteCiudad(aqui adjuntamos los ID pares de cada ciudad al dataframe de planas sn asignar)
    if 'Destino' not in paresAdiferenteCiudad.columns:
        print("No se encontraron emparejamientos o la columna 'Destino' no existe en paresAdiferenteCiudad.")
        columnas = [
            'remolque_a', 'remolque_b', 'ValorViaje_a', 'ValorViaje_b',
            'Fecha Estatus_a', 'Fecha Estatus_b', 'Ruta'
        ]

        # Crear el DataFrame vacío con las columnas definidas
        diferentesDestino = pd.DataFrame(columns=columnas)

        combined_df=noAsignadas.copy()
        combined_df['IDe'] = np.nan 

    else:
        combined_df = pd.merge(noAsignadas, paresAdiferenteCiudad, how='left', on='Destino')    
        combined_df = combined_df [['Remolque', 'Ruta', 'ValorViaje', 'IDe', 'FechaEstatus']]
        combined_df.sort_values(by= 'IDe', ascending=True, inplace=True)
        

        # Generar todas las combinaciones únicas de índices de las planas sin asignar a diferente destino
        #combinaciones_indices = list(combinations(combined_df.index, 2))

        # Crear una lista para almacenar las combinaciones únicas sin repetición de remolques
        filas_empate_doble = []
        # Iterar sobre las filas repetidas y combinarlas de dos en dos
        i = 0
        while i < len(combined_df):
            if i < len(combined_df) - 1: 
                if combined_df['IDe'].iloc[i] == combined_df['IDe'].iloc[i+1]:
                    row1 = combined_df.iloc[i]
                    
                    row2 = combined_df.iloc[i + 1]
                    
                    filas_empate_doble.append([row1['IDe'], row1['Remolque'], row2['Remolque'], row1['ValorViaje'], row2['ValorViaje'], row1['FechaEstatus'], row2['FechaEstatus'], row1['Ruta'], row2['Ruta']])
                    i += 1  # Incrementa i solo si se cumple la condición
            i += 1  # Incrementa i en cada iteración del bucle while

        # Crear un nuevo DataFrame con las combinaciones únicas sin repetición de remolques
        df_empates_dobles = pd.DataFrame(filas_empate_doble, columns=['IDe', 'remolque_a', 'remolque_b', 'ValorViaje_a','ValorViaje_b','Fecha Estatus_a', 'Fecha Estatus_b', 'Ruta1', 'Ruta2'])

        # Crea una columna nueva y luego seleccionar columnas
        df_empates_dobles['Ruta'] = df_empates_dobles.apply(lambda x: f"{x['Ruta1']} | {x['Ruta2']}", axis=1)
        df_empates_dobles = df_empates_dobles[['remolque_a', 'remolque_b', 'ValorViaje_a', 'ValorViaje_b', 'Fecha Estatus_a', 'Fecha Estatus_b', 'Ruta']]

        diferentesDestino = df_empates_dobles.copy()
        




    #PLANAS SIN PARES MAYOR A 24 HRS
    #Planas sin pares al mismo destino a destinos cercanos
    combined_df= combined_df[pd.isna(combined_df['IDe'])]
    ahora = datetime.now()
    limite = ahora - timedelta(hours=24)

    #Filtrar el DataFrame para quedarte solo con las filas cuya 'Fecha Estatus_a' sea mayor a 24 horas atrás
    combined_df= combined_df[combined_df['FechaEstatus'] < limite]

    #Renombrar columnas
    combined_df.rename(columns={
    'Remolque': 'remolque_a',
    'FechaEstatus': 'Fecha Estatus_a',
    'ValorViaje':'ValorViaje_a'
    }, inplace=True)



    #DATAFRAME FINAL
    # Concatena todos los dataframes 
    df_concatenado = pd.concat([mismoDestino, diferentesDestino], ignore_index=True)
    df_concatenado = pd.concat([df_concatenado , combined_df], ignore_index=True)

    # Calcular valor total del viaje
    df_concatenado['Monto'] = df_concatenado['ValorViaje_a'] + df_concatenado['ValorViaje_b']

    # Definir la fecha y hora actual
    ahora = datetime.now()

    # Reemplazar valores NaT por la fecha y hora actual en la columna 'Fecha Estatus_b'
    df_concatenado['Fecha Estatus_b'] = df_concatenado['Fecha Estatus_b'].fillna(ahora)

    #Obtener la fecha mas antigua entre las dos planas
    df_concatenado['Fecha Más Antigua'] = np.where(df_concatenado['Fecha Estatus_a'] < df_concatenado['Fecha Estatus_b'],
                                                    df_concatenado['Fecha Estatus_a'],
                                                    df_concatenado['Fecha Estatus_b'])

    df_concatenado = df_concatenado.sort_values(by='Fecha Más Antigua', ascending=True)

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
    #df_concatenado['Monto'] = df_concatenado['Monto'].map('{:,.0f}'.format)
    
    #Calcular horas en patio
    df_concatenado['Horas en Patio'] = ((ahora - df_concatenado['Fecha Más Antigua']).dt.total_seconds()/3600).round(1) 
    df_concatenado = df_concatenado[['Ruta', 'remolque_a', 'remolque_b', 'Monto', 'Horas en Patio']]
    df_concatenado= df_concatenado[df_concatenado['Ruta'] != 'MONTERREY-ALLENDE']
    #df_concatenado.reset_index(drop=True, inplace=True)
    #df_concatenado.index = df_concatenado.index + 1

    df_concatenado.reset_index(drop=True, inplace=True)
    df_concatenado.index = df_concatenado.index + 1

    return df_concatenado


def procesar_operadores(Operadores):
    Operadores = Operadores[Operadores['Estatus'].isin(['Disponible'])]
    Operadores  = Operadores [Operadores ['UOperativa'].isin(['U.O. 01 ACERO', 'U.O. 02 ACERO', 'U.O. 03 ACERO', 'U.O. 04 ACERO', 'U.O. 07 ACERO','U.O. 39 ACERO', 'U.O. 15 ACERO (ENCORTINADOS)', 'U.O. 41 ACERO LOCAL (BIG COIL)', 'U.O. 52 ACERO (ENCORTINADOS SCANIA)'])]
    Operadores = Operadores[Operadores['Destino'].isin(['NYC'])]
    Operadores['Tiempo Disponible'] = ((datetime.now() - Operadores['FechaEstatus']).dt.total_seconds() / 3600).round(1)
    Operadores = Operadores[Operadores['ObservOperaciones'].isna() | Operadores['ObservOperaciones'].eq('')]
    Operadores = Operadores[['Operador', 'Tractor', 'UOperativa', 'Tiempo Disponible']]
    Operadores.sort_values(by='Tiempo Disponible', ascending=False, inplace=True)
    Operadores.reset_index(drop=True, inplace=True)
    Operadores.index += 1
    operadores_sin_asignacion = Operadores.copy()
    return operadores_sin_asignacion