import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from flask import render_template, Blueprint
from sklearn.metrics import DistanceMetric
from db_manager import fetch_data, fetch_data_PRO, fetch_data_DIA
from planasEnPatio import procesar_operadores, planas_sac
from itertools import combinations
import requests
import os
from concurrent.futures import ThreadPoolExecutor
import urllib3

planasPorAsignar = Blueprint('planasPorAsignar', __name__)
@planasPorAsignar.route('/')
def index():
    planas, Operadores,Bloqueo, Permisos,\
    DataDIA, MttoPrev, CheckTaller, OrAbierta  =  \
        cargar_datos()
    OperadorYaAsignado, IdSolicitudYaAsignado = api_spl_get()
    planasSAC = planas_sac()
    operadores_sin_asignacion = procesar_operadores(Operadores, DataDIA, OperadorYaAsignado)
    emparejamientosPla= emparejamientosPlanas(planas, DataDIA, planasSAC, IdSolicitudYaAsignado)
    PermisosOp= permisosOperador(Permisos)
    cerca = cercaU()
    operadorDis = calOperador(
        operadores_sin_asignacion, 
        Bloqueo, 
        PermisosOp, 
        cerca, 
        DataDIA, 
        OrAbierta,
        MttoPrev,
        CheckTaller
    )
    operadoresFull = operadorDis.to_html()
    empates_dobles = emparejamientosPla.to_html()
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
            SELECT NombreOperador, Activo, OperadorBloqueado
            FROM DimOperadores
            WHERE Activo = 'Si'
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
            WHERE FechaFinalizacion IS NULL AND TipoEquipo = 'Tractor'  AND Descripcion != 'Orden de Reparacion Especial'
        """)
    ]

    with ThreadPoolExecutor() as executor:
        # Lanzar cada consulta en un hilo separado
        futures = [executor.submit(globals()[func], sql) for func, sql in consultas]
        # Esperar a que todas las consultas se completen y recoger los resultados
        results = [future.result() for future in futures]

    return tuple(results)

def calOperador(operadores_sin_asignacion, Bloqueo, PermisosOp, cerca, DataDIA, OrAbierta, MttoPrev, CheckTaller):
    calOperador= operadores_sin_asignacion.copy()
    #calOperador =calOperador[calOperador['Tiempo Disponible'] > 0.2]
    calOperador= pd.merge(operadores_sin_asignacion, Bloqueo, left_on='Operador', right_on='NombreOperador', how='left')
    calOperador= calOperador[calOperador['Tractor'].isin(cerca['cve_uni'])]
    calOperador= calOperador[~calOperador['Operador'].isin(DataDIA['Operador'])]
    calOperador= pd.merge(calOperador, PermisosOp, left_on='Operador', right_on='Nombre', how='left')
    calOperador['OrAbierta'] = calOperador['Tractor'].apply(
    lambda x: 'Si' if x in OrAbierta['ClaveEquipo'].values else 'No'
    )
    calOperador['Pasar a Mtto Preventivo'] = calOperador['Tractor'].apply(
    lambda x: 'Si' if x in MttoPrev['ClaveTractor'].values else 'No'
    )
    calOperador['¿Paso por Check/Mtto?'] = calOperador['Tractor'].apply(
    lambda x: 'Si' if x in CheckTaller['Tractor'].values else 'No'
    )
    
    
   
    calOperador['Activo_y'] = calOperador['Activo_y'].fillna('No')
    calOperador['OperadorBloqueado'] = calOperador['OperadorBloqueado'].fillna('No')
    # Generar números aleatorios entre 25 y 50 para op nuevos 
    random_values = np.random.randint(25, 51, size=len(calOperador))
    # Convertir el ndarray en una serie de pandas
    random_series = pd.Series(random_values, index=calOperador.index)
    # Reemplazar los valores nulos con los valores aleatorios generados

    
    
    calOperador = calOperador[['Operador', 'Tractor', 'UOperativa', 'Activo_y', 'OperadorBloqueado', 'OrAbierta',
        'Pasar a Mtto Preventivo', '¿Paso por Check/Mtto?',  'Tiempo Disponible']]
    
        
    calOperador = calOperador.rename(columns={
    'UOperativa_x': 'Operativa',
    'OperadorBloqueado': 'Bloqueado Por Seguridad',
    'Activo_y': 'Permiso'
    })


    calOperador =calOperador.sort_values(by=['Bloqueado Por Seguridad', 'Permiso'], ascending=[True, True])
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
    CP['CalificacionVianjesAnteiores'] = 20 - (1 + (24 * (CP['PuntajeBruto'] - min_puntaje_posible) / (max_value - min_puntaje_posible)))
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
    if 'Reporte' in G.columns:
        G = G[G['Reporte'].str.contains("SINIESTRO")]
        G= G[G['Empresa'].str.contains("NYC")]

    # Quedarme con tres meses de historia hacia atras a partir de hoy, mantener las columnas
    G= G[pd.to_datetime(G['FechaSiniestro']).dt.date >= (datetime.now() - timedelta(days=3*30)).date()]

     
    # grupar Por Tractor
    G = G.groupby('NombreOperador')['TotalFinal'].sum()

    # Resetear Index
    G= G.reset_index()
 
    # Quedarse con columnas
    K = K[["NombreOperador", "FechaPago", "KmsReseteo"]]

    # Quedarme con tres meses de historia hacia atras a partir de hoy
    K = K[pd.to_datetime(K['FechaPago']).dt.date >= (datetime.now() - timedelta(days=90)).date()]

    # Agrupar por la columna "NombreOperador" y sumar los valores de la columna "KmsReseteo"
    K= K.groupby('NombreOperador')['KmsReseteo'].sum()

    # Voy a pasarlo a un dataframe 
    K = K.reset_index()

    # Realizar left join entre kilometros y gasto
    K= K.merge(G, on='NombreOperador', how='left')

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

def api_spl_get():
    def token_api():
        url = 'https://splpro.mx/ApiSpl/api/Login/authenticate'
        credentials = {
            'UserName': os.getenv('USERNAME'),
            'Password': os.getenv('PASSWORD'),
            'IdEmpresa': os.getenv('ID_EMPRESA', 1)  # El segundo argumento es el valor por defecto
        }
        response = requests.post(url, json=credentials, verify=False)
        response_data = response.json()

        if response.status_code == 200 and response_data.get('Success'):
            token = response_data.get('Token')
            print(f'Token recibido: {token}')
            return token
        else:
            print(f'Error en la solicitud: {response.status_code}')
            print(f'Mensaje del error: {response_data.get("Message")}')
            return None

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    token = token_api()
    if token:
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        url = 'https://splpro.mx/ApiSpl/api/ArmadoFull/GetSolicitudes'
        
        # Realizar la solicitud GET
        response = requests.get(url, headers=headers, verify=False)
        
        if response.status_code == 200:
            print("Solicitud GET procesada correctamente")
            data = response.json()

            if not data:  # Si el diccionario está vacío
                print("No se recibieron datos en la respuesta.")
                return pd.DataFrame(), pd.DataFrame() 
            else:
                # Si se recibieron datos
                data = pd.DataFrame(data)
                IdSolicitud = pd.concat([data['IdSolicitud'], data['IdSolicitud2']], axis=0).reset_index(drop=True)
                IdSolicitud = pd.DataFrame(IdSolicitud, columns=['IdSolicitud'])
                Operador = data[['Operador']].head(15)

                return Operador, IdSolicitud
        else:
            print(f"Error en la solicitud GET: {response.status_code}")
            print(response.text)
            return None

def emparejamientosPlanas(planas, DataDIA, planasSAC, IdSolicitudYaAsignado):
    planas= planas[~planas['Remolque'].isin(DataDIA['Plana'])]
    if not IdSolicitudYaAsignado.empty:
        planas= planas[~planas['IdSolicitud'].isin(IdSolicitudYaAsignado['IdSolicitud'])]
    planas = pd.merge(planas, planasSAC, on='Remolque', how='left')
    planas['Horas en patio'] = ((datetime.now() - planas['fecha de salida']).dt.total_seconds() / 3600.0).round(1)
    planas = planas.sort_values(by='Horas en patio', ascending=False)
    planas = planas[['IdSolicitud', 'Remolque', 'CiudadDestino', 'Horas en patio', 'ValorViaje']]
    planas = planas[planas['CiudadDestino'] != 'ALLENDE'] 
    planas.loc[:, 'CiudadDestino'] = planas['CiudadDestino'].str.replace(' ', '') 
    planas.loc[:, 'CiudadDestino'] = planas['CiudadDestino'].str.replace('JALISCO', 'GUADALAJARA')     
    
    Ubicaciones = pd.DataFrame({
        'City': ['CUAUTITLANIZCALLI', 'VICTORIA', 'SANJUANDELRIO', 'ELMARQUES', 'JALISCO', 'CORREGIDORA', 'SANTACRUZ', 'APIZACO', 'SAYULITA', 'SANFRANSICODELOSROMOS', 'OCOTLAN', 'SANTACRUZXOXOCOTLAN', 'JIUTEPEC,MOR', 'ELPRIETO,VERACRUZ', 'GUADALUPEZACATECAS', 'TEZOYUCA', 'CUAUTLA', 'CDVALLES', 'ACAPULCO', 'XALAPA,VER','AMATLANDELOSREYES', 'CUAUTLA,MORELOS','QUERETARO', 'GUADALAJARA', 'PUERTOVALLARTA', 'MAZATLAN', 'CULIACAN', 'LEON', 'MEXICO', 'SANLUISPOTOSI', 'VERACRUZ', 'TULTITLAN', 'JIUTEPEC', 'VILLAHERMOSA', 'PACHUCADESOTO', 'COLON', 'MERIDA', 'SALTILLO', 'CHIHUAHUA', 'TUXTLAGTZ', 'CORDOBA',
                    'TOLUCA', 'CIUDADHIDALGOCHP', 'CAMPECHE', 'ATITALAQUIA', 'MATAMOROS', 'ZAPOPAN', 'CIUDADCUAHUTEMOCCHH', 'MORELIA', 'TLAXCALA', 'GUADALUPE', 'SANTACRUZSON', 'LASVARAS', 'PACHUCA', 'CIUDADJUAREZ', 'TLAJOMULCO', 'PIEDRASNEGRAS', 'RAMOSARIZPE', 'ORIZABA', 'TAPACHULA', 'TEPATITLAN', 'TLAQUEPAQUE', 'TEAPEPULCO', 'LABARCA', 'ELMARQUEZ', 'CIUDADVICTORIA', 'NUEVOLAREDO', 'TIZAYUCA,HIDALGO', 'ELSALTO', 'OCOTLANJAL', 'TEZONTEPEC', 'ZAPOTILTIC', 'PASEOELGRANDE', 'POZARICA', 'JACONA', 'FRESNILLO', 'PUEBLA', 'TUXTLAGUTIERREZ', 'PLAYADELCARMEN', 'REYNOSA', 'MEXICALI', 'TEPEJIDELORODEOCAMPO',
                    'LEON', 'CUERNAVACA', 'CHETUMAL', 'CHIHUAHUA', 'SILAO', 'ACAPULCODEJUAREZ', 'AGUASCALIENTES', 'TIJUANA', 'OCOSINGO', 'MONCLOVA', 'OAXACA', 'SOLIDARIDAROO', 'JIUTEPEC', 'ELPRIETO', 'TORREON', 'HERMOSILLO', 'CELAYA', 'CANCUN', 'URUAPAN', 'ALTAMIRA', 'COATZACUALCOS', 'IRAPUATO', 'CASTAÑOS', 'DURANGO', 'COLON1', 'CIUDADVALLLES', 'MANZANILLA', 'TAMPICO', 'GOMEZPALACIO', 'ZACATECAS', 'SALAMANCA', 'COMITANDEDOMINGUEZ', 'UMAN', 'TUXTEPEC', 'ZAMORA', 'CORDOBA', 'MONTERREY', 'PENJAMO', 'NOGALES', 'RIOBRAVO', 'CABORCA', 'FRONTERACOAHUILA', 'LOSMOCHIS', 'KANASIN', 'ARRIAGACHIAPAS', 'VALLEHERMOSA', 'SANJOSEITURBIDE', 'MAZATLAN', 'TEHUACAN', 'CHILTEPEC', 'CHILPANCINGODELOSBRAVO'],
        'Latitude': [19.642906, 23.737420, 20.389307, 20.622969, 20.664082, 20.542695, 31.233600, 19.415142, 20.867196, 22.074109, 20.353894, 17.028832, 18.894928, 22.221429, 22.763576, 19.595099, 18.831580, 22.998189, 16.889844, 19.533927, 18.846950, 18.836561, 20.592275, 20.74031, 20.655893, 23.255931, 24.800964, 21.133941, 19.440265, 22.158710, 19.19002, 19.647433, 18.891529, 17.992561, 20.106154, 20.781414, 20.984380, 25.427049, 28.643361, 16.761753, 18.890666,
                       19.271311, 14.679697, 18.833447, 20.054095, 25.845915, 20.76705, 28.431062, 19.736983, 19.500336, 25.717427, 31.239198, 28.165034, 20.13492, 31.785672, 20.488792, 28.721685, 25.594781, 18.88138, 14.950696, 20.842635, 20.646152, 19.799357, 20.313766, 20.958186, 23.786371, 27.541875, 19.863533, 20.531878, 20.380148, 19.891505, 19.641563, 20.566394, 20.576162, 19.971759, 23.215653, 19.132065, 16.801565, 20.707474, 26.128212, 32.6718, 19.943972,
                        21.188758, 18.998997, 18.561445, 31.542897, 20.968175, 16.923231, 21.942294, 32.550529, 16.922181, 26.965938, 17.128621, 20774439, 18.932162, 22.22124, 25.622625, 29.098203, 20.581304, 21.208637, 19.432413, 22.430696, 22.430608, 20.725167, 20.828685, 24.077945, 22.027654, 20.025186, 19.127328, 22.323528, 25.629602, 22.782732, 20.604713, 16.2059, 20.914188, 18.108973, 20.018848, 18.911559, 25.79573, 20.444102, 31.331515, 26.007962, 30.751014, 26.976145, 25.831174, 20.979043, 16.251855, 25.690649, 21.020823, 23.316277, 18.504335, 18.908622, 17.592174],
        'Longitude': [-99.221770,-99.140663, -99.982418, -100.275149, -103.343537, -100.451902, -110.596481, -98.139633, -105.439843, -102.270805, -102.772942, -96.735289, -99.178066, -97.918783, -102.547755, -98.910005, -98.943625, -99.010334, -99.830687, -96.909218, -96.914283, -98.944068, -100.394273, -103.31312, -105.221967, -106.412165, -107.390388, -101.661519, -99.206780, -100.970141, -96.196430, -99.164822, -99.181056, -92.942980, -98.759106, -100.047289, -89.620138, -100.985244, -106.056315, -93.108217, -96.932524,
                        -99.667407, -92.151656, -90.286039, -99.222389, -97.503895, -103.351047, -106.83201, -101.204422, -98.158429, -100.181515, -110.59637, -105.340582, -98.772788, -106.566775, -103.445088, -100.547409, -100.900214, -97.104977, -92.254966, -102.79309, -103.317318, -98.555426, -102.541315, -100.2477, -99.16679, -99.565339, -98.976743, -103.181408, -102.777496, -98.814611, -103.449286, -100.679298, -97.430099, -102.298419, -102.850368, -98.222853, -93.116207, -87.07644, -98.343761, -115.385465, -99.339322,
                        -101.768658, -99.257945, -88.27958, -107.90993, -101.415423, -99.825972, -102.298616, -116.875228, -92.093952, -101.400616, -97.76784, -86.986023, -99.181586, -97.917121, -103.387956, -110.978133, -100.812923, -86.837061, -102.021193, -97.947615, -94.417513, -101.378726, -101.42206, -104.66471, -99.024839, -99.025514, -104.393928, -97.88042, -103.500552, -102.573756, -101.174834, -92.132644, -89.695333, -96.141711, -102.285924, -96.98147, -100.385905, -101.730812, -110.932889, -98.122363, -112.157303, -101.436711, -108.989827, -89.5488, -93.920658, -97.810778, -100.395074, -106.478543, -97414124, -97.047666, -99.51663]
    })
        
    def parejas_unicas (planas, distancia_maxima=190):

      def match_entre_unicos_mayor23():
        unique_destinos = planas['CiudadDestino'].value_counts()
        unique_destinos = unique_destinos[unique_destinos == 1].index
        # Filtrar el DataFrame
        planas_unicas = planas[planas['CiudadDestino'].isin(unique_destinos)]
        #planas_unicas.loc[:, 'CiudadDestino'] = planas_unicas['CiudadDestino'].str.replace(' ', '')
        planas_unicas = planas_unicas.merge(Ubicaciones, left_on='CiudadDestino', right_on='City', how='left')
        planas_unicas = planas_unicas[planas_unicas['Horas en patio'] >0]
  
        if planas_unicas.empty:
              return pd.DataFrame(), planas_unicas, planas
            
        # Calcular la matriz de distancias haversine
        dist = DistanceMetric.get_metric('haversine')
        coords = np.radians(planas_unicas[['Latitude', 'Longitude']])
        matriz_distancia = dist.pairwise(coords) * 6371  # Convert to kilometers
       
        # Listas para almacenar las parejas formadas
        parejas = []

        # Iterar sobre el DataFrame restante
        i = 0
        while i < len(planas_unicas):
            fila_actual = planas_unicas.iloc[i]
            for j in range(i + 1, len(planas_unicas)):
                fila_siguiente = planas_unicas.iloc[j]
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
                    planas_unicas = planas_unicas.drop([i, j]).reset_index(drop=True)
                    # Actualizar la matriz de distancias eliminando las filas y columnas correspondientes
                    matriz_distancia = np.delete(matriz_distancia, [i, j], axis=0)
                    matriz_distancia = np.delete(matriz_distancia, [i, j], axis=1)
                    # Reiniciar el índice i para empezar de nuevo
                    i = -1
                    break
            i += 1

        # Convertir las parejas a un DataFrame
        parejas_df = pd.DataFrame(parejas)
                
        # Verificar si parejas_df está vacío antes de intentar concatenar las columnas
        if not parejas_df.empty:
            parejas_df['Destino'] = parejas_df['Destino1'] + '-' + parejas_df['Destino2']
            ids_emparejados = pd.concat([parejas_df['IdSolicitud1'], parejas_df['IdSolicitud2']])
            planas_totales_restante = planas[~planas['IdSolicitud'].isin(ids_emparejados)]
            planas_unicas_restantes = planas_unicas[~planas_unicas['IdSolicitud'].isin(ids_emparejados)]
        else:
            planas_totales_restante = planas.copy()
            planas_unicas_restantes = planas_unicas.copy()
        
        return parejas_df, planas_unicas_restantes, planas_totales_restante

      def match_unicos_conNones_mayor23():
            _, planas_unicas_restantes, planas_totales_restante = match_entre_unicos_mayor23()
            planas_totales_restante_23hrs = planas_totales_restante[planas_totales_restante['Horas en patio'] > 23]

            # Contar las ocurrencias de cada valor en la columna 'CiudadDestino'
            counts = planas_totales_restante_23hrs['CiudadDestino'].value_counts()

            # Filtrar los destinos que tienen una cuenta impar
            impar_values = counts[counts % 2 != 0].index
            planas_totales_restante_23hrs_nones = planas_totales_restante_23hrs[planas_totales_restante_23hrs['CiudadDestino'].isin(impar_values)].copy()

            planas_totales_restante_23hrs_nones['unico'] = planas_totales_restante_23hrs['CiudadDestino'].apply(
                lambda x: 'si' if x in planas_unicas_restantes['CiudadDestino'].values else 'no'
            )
            
            planas_totales_restante_23hrs_nones = planas_totales_restante_23hrs_nones.merge(Ubicaciones, left_on='CiudadDestino', right_on='City', how='left')

            if planas_totales_restante_23hrs_nones.empty:
                return pd.DataFrame(), planas_unicas_restantes, planas_totales_restante

            # Calcular la matriz de distancias haversine
            dist = DistanceMetric.get_metric('haversine')
            coords = np.radians(planas_totales_restante_23hrs_nones[['Latitude', 'Longitude']])
            matriz_distancia = dist.pairwise(coords) * 6371  # Convert to kilometers

        # Listas para almacenar las parejas formadas
            parejas = []

            # Iterar sobre el DataFrame restante
            i = 0
            while i < len(planas_totales_restante_23hrs_nones):
                fila_actual = planas_totales_restante_23hrs_nones.iloc[i]
                if fila_actual['unico'] == 'si':
                    posibles_parejas = []
                    for j in range(len(planas_totales_restante_23hrs_nones)):
                        if i != j:
                            fila_siguiente = planas_totales_restante_23hrs_nones.iloc[j]
                            if fila_siguiente['unico'] == 'no':
                                distancia = matriz_distancia[i, j]
                                if distancia <= distancia_maxima and (fila_actual['Horas en patio'] > 23 or fila_siguiente['Horas en patio'] > 23):
                                    horas_diferencia = abs(fila_actual['Horas en patio'] - fila_siguiente['Horas en patio'])
                                    posibles_parejas.append((j, distancia, horas_diferencia))
                    
                    if posibles_parejas:
                        # Ordenar las posibles parejas por horas de diferencia y luego por distancia
                        posibles_parejas.sort(key=lambda x: (x[2], x[1]))
                        print(posibles_parejas)
                        mejor_pareja = posibles_parejas[0]
                        j = mejor_pareja[0]
                        fila_siguiente = planas_totales_restante_23hrs_nones.iloc[j]
                        distancia = mejor_pareja[1]
                        
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
                        planas_totales_restante_23hrs_nones = planas_totales_restante_23hrs_nones.drop([i, j]).reset_index(drop=True)
                        # Actualizar la matriz de distancias eliminando las filas y columnas correspondientes
                        matriz_distancia = np.delete(matriz_distancia, [i, j], axis=0)
                        matriz_distancia = np.delete(matriz_distancia, [i, j], axis=1)
                        # Reiniciar el índice i para empezar de nuevo
                        i = -1
                i += 1

            # Convertir las parejas a un DataFrame
            parejas_df = pd.DataFrame(parejas)
        
                # Verificar si parejas_df está vacío antes de intentar concatenar las columnas
            if not parejas_df.empty:
                parejas_df['Destino'] = parejas_df['Destino1'] + '-' + parejas_df['Destino2']
                ids_emparejados = pd.concat([parejas_df['IdSolicitud1'], parejas_df['IdSolicitud2']])
                planas_totales_restante = planas_totales_restante[~planas_totales_restante['IdSolicitud'].isin(ids_emparejados)]
                planas_unicas_restantes = planas_unicas_restantes[~planas_unicas_restantes['IdSolicitud'].isin(ids_emparejados)]


            return parejas_df, planas_unicas_restantes, planas_totales_restante
      
      def match_unicos_con_pares_may23():
        _, planas_unicas_restantes, planas_totales_restante =  match_unicos_conNones_mayor23()
        
        planas_unicas_restantes['unico'] = 'si'
        planas_totales_restante_mayor23hrs = planas_totales_restante[planas_totales_restante['Horas en patio'] >23]
        planas_totales_restante_menor23hrs = planas_totales_restante[planas_totales_restante['Horas en patio'] <=23]
        ciudades_menor23hrs = planas_totales_restante_menor23hrs['CiudadDestino'].unique()
        
        # Contar las ocurrencias de cada valor en la columna 'CiudadDestino'
        counts = planas_totales_restante_mayor23hrs['CiudadDestino'].value_counts()

        # Filtrar los destinos que tienen una cuenta impar
        impar_values = counts[counts % 2 != 0].index
        planas_totales_restante_pares_mayor23hrs = planas_totales_restante_mayor23hrs[~planas_totales_restante_mayor23hrs['CiudadDestino'].isin(impar_values)]
        planas_totales_restante_pares_mayor23hrs = planas_totales_restante_mayor23hrs[planas_totales_restante_mayor23hrs['CiudadDestino'].isin(ciudades_menor23hrs)]
        planas_totales_restante_pares_mayor23hrs = planas_totales_restante_pares_mayor23hrs.merge(Ubicaciones, left_on='CiudadDestino', right_on='City', how='left')
        planas_totales_restante_pares_mayor23hrs['unico'] = 'no'
        planas_totales_restante_pares_mayor23hrs= pd.concat([planas_unicas_restantes, planas_totales_restante_pares_mayor23hrs], ignore_index=True)
        
        if planas_totales_restante_pares_mayor23hrs.empty:
          return pd.DataFrame(), planas_unicas_restantes, planas_totales_restante
           
        # Calcular la matriz de distancias haversine
        dist = DistanceMetric.get_metric('haversine')
        coords = np.radians(planas_totales_restante_pares_mayor23hrs[['Latitude', 'Longitude']])
        matriz_distancia = dist.pairwise(coords) * 6371  # Convert to kilometers
        planas_totales_restante_pares_mayor23hrs['IndiceGrupo'] = planas_totales_restante_pares_mayor23hrs.groupby('CiudadDestino').cumcount() + 1  
        planas_totales_restante_pares_mayor23hrs = planas_totales_restante_pares_mayor23hrs.sort_values(by='Horas en patio', ascending=False)
        planas_totales_restante_pares_mayor23hrs['NuevoIndice'] = range(1, len(planas_totales_restante_pares_mayor23hrs) + 1)
        planas_totales_restante_pares_mayor23hrs = planas_totales_restante_pares_mayor23hrs[planas_totales_restante_pares_mayor23hrs['IndiceGrupo'] % 2 != 0]
        planas_totales_restante_pares_mayor23hrs['unico'] = planas_totales_restante_pares_mayor23hrs['unico'].replace({'si': 1, 'no': 0})
        planas_totales_restante_pares_mayor23hrs = planas_totales_restante_pares_mayor23hrs.sort_values(by='unico', ascending=False)
        planas_totales_restante_pares_mayor23hrs['unico'] = planas_totales_restante_pares_mayor23hrs['unico'].replace({1: 'si', 0: 'no'})

 
        # Listas para almacenar las parejas formadas
        parejas = []

        # Iterar sobre el DataFrame restante

        i = 0
        while i < len(planas_totales_restante_pares_mayor23hrs):
            fila_actual = planas_totales_restante_pares_mayor23hrs.iloc[i]
            if fila_actual['unico'] == 'si':
                for j in range(len(planas_totales_restante_pares_mayor23hrs)):
                    if i != j:
                        fila_siguiente = planas_totales_restante_pares_mayor23hrs.iloc[j]
                        if fila_siguiente['unico'] == 'no':
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
                                planas_totales_restante_pares_mayor23hrs = planas_totales_restante_pares_mayor23hrs.drop([i, j]).reset_index(drop=True)
                                # Actualizar la matriz de distancias eliminando las filas y columnas correspondientes
                                matriz_distancia = np.delete(matriz_distancia, [i, j], axis=0)
                                matriz_distancia = np.delete(matriz_distancia, [i, j], axis=1)
                                # Reiniciar el índice i para empezar de nuevo
                                i = -1
                                break
            i += 1
        # Convertir las parejas a un DataFrame
        parejas_df = pd.DataFrame(parejas)
        
                # Verificar si parejas_df está vacío antes de intentar concatenar las columnas
        if not parejas_df.empty:
            parejas_df['Destino'] = parejas_df['Destino1'] + '-' + parejas_df['Destino2']
            ids_emparejados = pd.concat([parejas_df['IdSolicitud1'], parejas_df['IdSolicitud2']])
            planas_totales_restante = planas_totales_restante[~planas_totales_restante['IdSolicitud'].isin(ids_emparejados)]
            planas_unicas_restantes = planas_unicas_restantes[~planas_unicas_restantes['IdSolicitud'].isin(ids_emparejados)]
        
        return parejas_df, planas_unicas_restantes, planas_totales_restante
      
      def match_entre_unicos_menor23():
        _, planas_unicas_restantes, planas_totales_restante = match_unicos_con_pares_may23()
        
        planas_unicas_restantes['unico'] = 'si'
        planas_unicas_restantes = planas_unicas_restantes[planas_unicas_restantes['Horas en patio'] > 23]
        planas_totales_restante_menor23hrs = planas_totales_restante[planas_totales_restante['Horas en patio'] <=23]
        U= planas_totales_restante_menor23hrs.copy()
        planas_totales_restante_menor23hrs = planas_totales_restante_menor23hrs.merge(Ubicaciones, left_on='CiudadDestino', right_on='City', how='left')
        #planas_totales_restante_menor23hrs['unico'] = 'no'
               
        planas_totales_restante_menor23hrs= pd.concat([planas_unicas_restantes, planas_totales_restante_menor23hrs], ignore_index=True)
        
        if planas_totales_restante_menor23hrs.empty:
          return pd.DataFrame(), planas_unicas_restantes, planas_totales_restante
           
        # Calcular la matriz de distancias haversine
        dist = DistanceMetric.get_metric('haversine')
        coords = np.radians(planas_totales_restante_menor23hrs[['Latitude', 'Longitude']])
        matriz_distancia = dist.pairwise(coords) * 6371  # Convert to kilometers
       
        # Listas para almacenar las parejas formadas
        parejas = []

        # Iterar sobre el DataFrame restante
        i = 0
        while i < len(planas_totales_restante_menor23hrs):
            fila_actual = planas_totales_restante_menor23hrs.iloc[i]
            if fila_actual['unico'] == 'si':
                for j in range(len(planas_totales_restante_menor23hrs)):
                    if i != j:
                        fila_siguiente = planas_totales_restante_menor23hrs.iloc[j]
                        if fila_siguiente['unico'] == 'no':
                            distancia = matriz_distancia[i, j]
                            if distancia <= distancia_maxima and (fila_actual['Horas en patio'] < 23 or fila_siguiente['Horas en patio'] < 23):
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
                                planas_totales_restante_menor23hrs = planas_totales_restante_menor23hrs.drop([i, j]).reset_index(drop=True)
                                # Actualizar la matriz de distancias eliminando las filas y columnas correspondientes
                                matriz_distancia = np.delete(matriz_distancia, [i, j], axis=0)
                                matriz_distancia = np.delete(matriz_distancia, [i, j], axis=1)
                                # Reiniciar el índice i para empezar de nuevo
                                i = -1
                                break
            i += 1

        # Convertir las parejas a un DataFrame
        parejas_df = pd.DataFrame(parejas)
        
                # Verificar si parejas_df está vacío antes de intentar concatenar las columnas
        if not parejas_df.empty:
            parejas_df['Destino'] = parejas_df['Destino1'] + '-' + parejas_df['Destino2']
            ids_emparejados = pd.concat([parejas_df['IdSolicitud1'], parejas_df['IdSolicitud2']])
            planas_totales_restante = planas_totales_restante[~planas_totales_restante['IdSolicitud'].isin(ids_emparejados)]
            planas_unicas_restantes = planas_unicas_restantes[~planas_unicas_restantes['IdSolicitud'].isin(ids_emparejados)]
  
        return parejas_df, planas_unicas_restantes, planas_totales_restante
      
      def match_fin():
        parejas_df1, _, _ = match_entre_unicos_mayor23()
        parejas_df2, _, _ = match_unicos_con_pares_may23()
        parejas_df3, _, _ = match_unicos_conNones_mayor23()
        parejas_df4, _, planas_totales_restante = match_entre_unicos_menor23()
        
        parejasFin = pd.concat([parejas_df1, parejas_df2, parejas_df3, parejas_df4], ignore_index=True) 

        return parejasFin, planas_totales_restante
      
      return match_fin()  

    def emparejar_misma_ciudad_mayor23(planas):
      
        _, planas_restante = parejas_unicas (planas, 190)
        df_filtrado = planas_restante.copy()
        df_filtrado = df_filtrado[df_filtrado['Horas en patio'] >0]# Filtrar las planas con más de 23 horas en el patio
        parejas = [] # Listas para almacenar las parejas formadas
        destinos_agrupados = df_filtrado.groupby('CiudadDestino')# Agrupar por 'CiudadDestino'
        
        #Se genera pareja de destinos
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
    
    def emparejar_destinos_cercanos_mayor23(planas, distancia_maxima):
        
        _, data_restante = emparejar_misma_ciudad_mayor23(planas)
        

        # Agregar coordenadas al DataFrame restante
        data_restante.loc[:, 'CiudadDestino'] = data_restante['CiudadDestino'].str.replace(' ', '')

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
            parejas_df['Destino'] = parejas_df['Destino1'] + '-' + parejas_df['Destino2']
            ids_emparejados = pd.concat([parejas_df['IdSolicitud1'], parejas_df['IdSolicitud2']])
            planas_restante = data_restante[~data_restante['IdSolicitud'].isin(ids_emparejados)]
        else:
            planas_restante = data_restante.copy()
        
        return parejas_df, planas_restante
    
    def emparejar_misma_ciudad_menor23(planas):
        # Filtrar las planas con más de 23 horas en el patio
        _, planas_restante = emparejar_destinos_cercanos_mayor23(planas, distancia_maxima=190)
        df_filtrado = planas_restante.copy()
        # df_filtrado = planas[planas['Horas en patio'] < 0]
        
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
        r1,_ = parejas_unicas (planas, distancia_maxima=190)
        r2,_ = emparejar_misma_ciudad_mayor23(planas)
        r3,_ = emparejar_destinos_cercanos_mayor23(planas, distancia_maxima=200)
        r4,_ = emparejar_misma_ciudad_menor23(planas)
    
        rf = pd.concat([r1, r2, r3, r4], ignore_index=True)
          
        rf['Horas en patio'] = rf[['Horas en patio1', 'Horas en patio2']].max(axis=1)
        rf['ValorViaje'] = rf['ValorViaje1'] + rf['ValorViaje2']
        rf= rf.sort_values(by='Horas en patio', ascending=False)
        
        #rf = rf[['Destino', 'Remolque1', 'Remolque2', 'Horas en patio']]
        rf = rf[['Destino', 'Remolque1', 'Remolque2', 'Horas en patio1', 'Horas en patio2', 'ValorViaje']]
        
        # Renombrar las columnas del DataFrame
        rf.rename(columns={
            'Destino': 'Ruta',
            'Remolque1': 'remolque_a',
            'Remolque2': 'remolque_b',
            'Horas en patio1': 'Horas en Patio',
            'ValorViaje': 'Monto'
        }, inplace=True)
        
        # Reiniciar el índice comenzando desde 1
        rf.reset_index(drop=True, inplace=True)
        rf.index = rf.index + 1

        return rf

                  
    return emparejamiento_fin(planas)
 