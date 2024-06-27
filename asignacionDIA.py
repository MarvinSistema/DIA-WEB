from flask import render_template, Blueprint, request, jsonify
import pandas as pd
import json
import requests
import urllib3
import os
from twilio.rest import Client
from db_manager import fetch_data, fetch_data_PRO
from planasPorAsignar import procesar_planas, procesar_operadores, calOperador, asignacionesPasadasOp, siniestralidad, eta, permisosOperador
global f_concatenado


asignacionDIA = Blueprint('asignacionDIA', __name__)
@asignacionDIA.route('/')
def index():
    global f_concatenado
    planas, Operadores, Cartas, Gasto, Km, Bloqueo, ETAs, Permisos, Op, Tractor  = cargar_datos()
    operadores_sin_asignacion = procesar_operadores(Operadores)
    planasPorAsignar = procesar_planas(planas)
    asignacionesPasadas= asignacionesPasadasOp(Cartas)
    siniestroKm= siniestralidad(Gasto, Km)
    ETAi= eta(ETAs)
    PermisosOp= permisosOperador(Permisos)
    calOperadores, operadorNon, operadorFull = calOperador(operadores_sin_asignacion, Bloqueo, asignacionesPasadas, siniestroKm, ETAi, PermisosOp)
    f_concatenado= asignacion2(planasPorAsignar, calOperadores, planas, Op, Tractor)
    a= api_dias()
    datos_html = f_concatenado.to_html()

    return  render_template('asignacionDIA.html', datos_html=datos_html)
    
def cargar_datos():
    consulta_planas = """
        SELECT * 
        FROM DimTableroControlRemolque 
        WHERE PosicionActual = 'NYC' 
        AND Estatus = 'CARGADO EN PATIO' 
        AND Ruta IS NOT NULL 
        AND CiudadDestino != 'MONTERREY'
        AND CiudadDestino != 'GUADALUPE'
        AND CiudadDestino != 'APODACA'
    """
    consulta_operadores = "SELECT * FROM DimTableroControl"
    consultaOp= "SElECT * FROM DimOperadores"
    consultaTrac= "SElECT * FROM Cat_Tractor"
    ConsultaCartas = f"SELECT * FROM ReporteCartasPorte WHERE FechaSalida > '2024-01-01'"
    ConsultaGasto= f"SELECT *   FROM DimReporteUnificado"
    ConsultaKm = f"SELECT *   FROM DimRentabilidadLiquidacion"
    ConsultaBloqueo = f"SELECT *   FROM DimOperadores Where Activo = 'Si'"
    ConsultaETA = f"""
        SELECT NombreOperador, FechaFinalizacion, CumpleETA 
        FROM DimIndicadoresOperaciones 
        WHERE FechaSalida > '2024-01-01' 
        AND FechaLlegada IS NOT NULL
    """
    ConsultaPermiso = "SELECT NoOperador, Nombre, Activo, FechaBloqueo  FROM DimBloqueosTrafico"
    planas = fetch_data(consulta_planas)
    Operadores = fetch_data(consulta_operadores)
    Cartas = fetch_data(ConsultaCartas)
    Gasto = fetch_data_PRO(ConsultaGasto)
    Km = fetch_data(ConsultaKm)
    Bloqueo = fetch_data(ConsultaBloqueo)
    ETAs = fetch_data(ConsultaETA)
    Permisos = fetch_data(ConsultaPermiso)
    Op= fetch_data(consultaOp)
    Tractor= fetch_data(consultaTrac)
    
           
    return planas, Operadores, Cartas, Gasto, Km, Bloqueo, ETAs, Permisos, Op, Tractor
      
def asignacion2(planasPorAsignar, calOperador, planas, Op, Tractor):
    if (planasPorAsignar['remolque_b'] == 0).any():
        calOperador= calOperador[calOperador['Bloqueado Por Seguridad'].isin(['No'])]
        calOperador= calOperador[calOperador['Permiso'].isin(['No'])]
            
        operardorNon = calOperador[calOperador ['UOperativa_y'].isin([ 'U.O. 15 ACERO (ENCORTINADOS)', 'U.O. 41 ACERO LOCAL (BIG COIL)', 'U.O. 52 ACERO (ENCORTINADOS SCANIA)'])]
        #Crear una columna auxiliar para priorizar 'U.O. 41 ACERO LOCAL (BIG COIL)'
        operardorNon['priority'] = (operardorNon['UOperativa_y'] == 'U.O. 41 ACERO LOCAL (BIG COIL)').astype(int)
        operardorNon = operardorNon.sort_values(by=['priority', 'CalFinal', 'Tiempo Disponible'],ascending=[False, False, False])
        operardorNon = operardorNon.reset_index(drop=True)
        operardorNon.index = operardorNon.index + 1
        operardorNon.drop(columns=['priority'], inplace=True)
        
        operadorFull = calOperador[calOperador['UOperativa_y'].isin(['U.O. 01 ACERO', 'U.O. 02 ACERO', 'U.O. 03 ACERO', 'U.O. 04 ACERO', 'U.O. 07 ACERO','U.O. 39 ACERO'])]
        operadorFull = operadorFull.sort_values(by=['CalFinal', 'Tiempo Disponible'], ascending=[False, False])
        operadorFull = operadorFull.reset_index(drop=True)
        operadorFull.index = operadorFull.index + 1
        
        planasNon = planasPorAsignar[planasPorAsignar['remolque_b'] == 0]
        planasNon = planasNon.sort_values(by='Monto', ascending=False)
        planasNon = planasNon.reset_index(drop=True)
        planasNon.index = planasNon.index + 1
          
        planasFull= planasPorAsignar[planasPorAsignar['remolque_b'] != 0]
        planasFull= planasFull.sort_values(by='Monto', ascending=False)
        planasFull = planasFull.reset_index(drop=True)
        planasFull.index = planasFull.index + 1
   
        asignacionNon= pd.merge(planasNon, operardorNon, left_index=True, right_index=True, how='left')
        asignacionFull= pd.merge(planasFull, operadorFull, left_index=True, right_index=True, how='left')
          
        f_concatenado=  pd.concat([asignacionNon, asignacionFull], axis=0)
        
        
        # Unión para Plana 1
        f_concatenado = pd.merge(f_concatenado, planas, left_on='remolque_a', right_on='Remolque', how='left', suffixes=('', '_right1'))
        f_concatenado.rename(columns={'IdSolicitud': 'IdSolicitud1', 'IdRemolque': 'IdRemolque1'}, inplace=True)

        # Unión para Plana 2
        f_concatenado= pd.merge(f_concatenado, planas, left_on='remolque_b', right_on='Remolque', how='left', suffixes=('', '_right2'))
        f_concatenado.rename(columns={'IdSolicitud': 'IdSolicitud2', 'IdRemolque': 'IdRemolque2'}, inplace=True)
        f_concatenado= pd.merge(f_concatenado, Op, left_on='Operador', right_on='NombreOperador', how='left')
        f_concatenado= pd.merge(f_concatenado, Tractor, left_on='Tractor', right_on='ClaveTractor', how='left')
        
        #f_concatenado['IdOperador'] = f_concatenado['IdOperador'].astype(int)
        f_concatenado = f_concatenado[['Ruta', 'remolque_a', 'remolque_b', 'Operador', 'IdOperador', 'IdTractor', 'Tractor', 'IdRemolque1', 'IdSolicitud1', 'IdRemolque2', 'IdSolicitud2' ]]

        f_concatenado.rename(columns={
        'remolque_a': 'Plana 1',
        'remolque_b': 'Plana 2',
        }, inplace=True)
        
        f_concatenado = f_concatenado[f_concatenado['Operador'].notna()]
       
        return f_concatenado
    else:
        calOperador= calOperador[calOperador['Bloqueado Por Seguridad'].isin(['No'])]
        calOperador= calOperador[calOperador['Permiso'].isin(['No'])]
        calOperador = calOperador[calOperador['Operativa'].isin(['U.O. 01 ACERO', 'U.O. 02 ACERO', 'U.O. 03 ACERO', 'U.O. 04 ACERO', 'U.O. 07 ACERO','U.O. 39 ACERO'])]
        calOperador = calOperador.sort_values(by=['CalFinal', 'Tiempo Disponible'], ascending=[False, False])
        calOperador = calOperador.reset_index(drop=True)
        calOperador.index = calOperador.index + 1

        
        planasPorAsignar = planasPorAsignar.sort_values(by='Monto', ascending=False)
        planasPorAsignar = planasPorAsignar.reset_index(drop=True)
        planasPorAsignar.index = planasPorAsignar.index + 1

        #f_concatenado = pd.concat([planasPorAsignar, calOperador], axis=1)
        f_concatenado= pd.merge(planasPorAsignar, calOperador, left_index=True, right_index=True, how='left')
        
        
        # Unión para Plana 1
        f_concatenado = pd.merge(f_concatenado, planas, left_on='remolque_a', right_on='Remolque', how='left', suffixes=('', '_right1'))
        f_concatenado.rename(columns={'IdSolicitud': 'IdSolicitud1', 'IdRemolque': 'IdRemolque1'}, inplace=True)

        # Unión para Plana 2
        f_concatenado= pd.merge(f_concatenado, planas, left_on='remolque_b', right_on='Remolque', how='left', suffixes=('', '_right2'))
        f_concatenado.rename(columns={'IdSolicitud': 'IdSolicitud2', 'IdRemolque': 'IdRemolque2'}, inplace=True)
        f_concatenado= pd.merge(f_concatenado, Op, left_on='Operador', right_on='NombreOperador', how='left')
        f_concatenado= pd.merge(f_concatenado, Tractor, left_on='Tractor', right_on='ClaveTractor', how='left')
        
        #f_concatenado['IdOperador'] = f_concatenado['IdOperador'].astype(int)
        f_concatenado = f_concatenado[['Ruta', 'remolque_a', 'remolque_b', 'Operador', 'IdOperador', 'IdTractor', 'Tractor', 'IdRemolque1', 'IdSolicitud1', 'IdRemolque2', 'IdSolicitud2' ]]

       
        f_concatenado.rename(columns={
        'remolque_a': 'Plana 1',
        'remolque_b': 'Plana 2'
        }, inplace=True)


        f_concatenado = f_concatenado[f_concatenado['Operador'].notna()]
        
        

        return f_concatenado
def sender ():
    account_sid = 'AC53a449493940232f199fd2d6601ac007'
    auth_token = '08e7151244f7c3bab5093b9448740efb'
    client = Client(account_sid, auth_token)

    # Formatear el mensaje
    mensaje = (
        "Hola [Nombre del Operador],\n\n"
        "Te informamos sobre tu próximo viaje asignado:\n\n"
        "Destino: [Ingresar destino]\n"
        "Fecha y hora: [Ingresar fecha y hora]\n\n"
        "Esta asignación considera tu historial de accidentes, puntualidad, destinos previos y respuestas a viajes anteriores.\n\n"
        "Por favor, confirma si aceptas o rechazas este viaje con un \"Sí\" o \"No\".\n\n"
        "Gracias,"
    )

    # Enviar el mensaje
    message = client.messages.create(
        from_='whatsapp:+14155238886',
        body=mensaje,
        to='whatsapp:+5218125906703'
    )





#@asignacionDIA.route('/enviar', methods=['POST'])

def api_dia():
    global f_concatenado
    f_concatenado = f_concatenado[['IdSolicitud1', 'IdSolicitud2', 'IdRemolque1', 'IdRemolque2', 'IdTractor', 'IdOperador']]
   
    def token_api():
        # URL de la API para autenticación
        url = 'https://splpro.mx/ApiSpl/api/Login/authenticate'

        # Credenciales para el inicio de sesión
        credentials = {
            'UserName': '21011',
            'Password': 'Marvin12',
            'IdEmpresa': 1
        }

        # Realiza la solicitud POST con las credenciales, desactivando la verificación de SSL
        response = requests.post(url, json=credentials, verify=False)

        # Extrae la respuesta en formato JSON
        response_data = response.json()

        # Verifica si la autenticación fue exitosa
        if response.status_code == 200 and response_data.get('Success'):
            # Extrae el token de la respuesta
            token = response_data.get('Token')
            print(f'Token recibido: {token}')
            return token
        else:
            print(f'Error en la solicitud: {response.status_code}')
            print(f'Mensaje del error: {response_data.get("Message")}')
            return None

        # Suprime advertencias de SSL (opcional, sólo para limpiar la salida si no te preocupa la seguridad)
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Obtener token válido
    token = token_api()

    # URL de la API para realizar la operación de unir solicitudes
    url = 'https://splpro.mx/ApiSpl/api/ArmadoFull/UnirSolicitudes'

    # Datos a enviar en el JSON
    payload = {
        'IdSolicitud1': '5',
        'IdSolicitud2': '6',
        'IdRemolque1': '3',
        'IdRemolque2': '4',
        'IdTractor': '5',
        'IdOperador': '6'
    }

    # Encabezados HTTP necesarios para la autenticación con token
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Realiza la solicitud POST
    response = requests.post(url, json=payload, headers=headers, verify=False)

    # Verifica si la solicitud fue exitosa
    if response.status_code == 200:
        print("Solicitud procesada correctamente")
        print(response.json())  # Imprimir respuesta del servidor
    else:
        print(f"Error en la solicitud: {response.status_code}")
        print(response.text)  # Imprimir mensaje de error

    # Suprime advertencias de SSL (opcional)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def api_dias():
    global f_concatenado
    f_concatenado = f_concatenado[['IdSolicitud1', 'IdSolicitud2', 'IdRemolque1', 'IdRemolque2', 'IdTractor', 'IdOperador']]

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
        url = 'https://splpro.mx/ApiSpl/api/ArmadoFull/UnirSolicitudes'
        
        # Itera sobre cada fila del DataFrame y envía una solicitud por cada fila
        for index, row in f_concatenado.iterrows():
            payload = {
                'IdSolicitud1': int(row['IdSolicitud1']),
                'IdSolicitud2': int(row['IdSolicitud2']),
                'IdRemolque1': int(row['IdRemolque1']),
                'IdRemolque2': int(row['IdRemolque2']),
                'IdTractor': int(row['IdTractor']),
                'IdOperador': int(row['IdOperador'])
            }

            response = requests.post(url, json=payload, headers=headers, verify=False)
            if response.status_code == 200:
                print(f"Solicitud procesada correctamente para el índice {index}")
                print(response.json())
            else:
                print(f"Error en la solicitud para el índice {index}: {response.status_code}")
                print(response.text)

        # Suprime advertencias de SSL (opcional)
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

