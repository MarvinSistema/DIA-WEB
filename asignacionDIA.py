from flask import render_template, Blueprint
import pandas as pd
from twilio.rest import Client
from db_manager import fetch_data, fetch_data_PRO
from planasPorAsignar import procesar_planas, procesar_operadores, calOperador, asignacionesPasadasOp, siniestralidad, eta

asignacionDIA = Blueprint('asignacionDIA', __name__)
@asignacionDIA.route('/')
def index():
    planas, Operadores, Cartas, Gasto, Km, Bloqueo, ETAs  = cargar_datos()
    operadores_sin_asignacion = procesar_operadores(Operadores)
    planasPorAsignar = procesar_planas(planas)
    asignacionesPasadas= asignacionesPasadasOp(Cartas)
    siniestroKm= siniestralidad(Gasto, Km)
    ETAi= eta(ETAs)
    calOperadores = calOperador(operadores_sin_asignacion, Bloqueo, asignacionesPasadas, siniestroKm, ETAi)
    asignacion= asignacion2(planasPorAsignar, calOperadores)
    return asignacion
    
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
    planas = fetch_data(consulta_planas)
    Operadores = fetch_data(consulta_operadores)
    Cartas = fetch_data(ConsultaCartas)
    Gasto = fetch_data_PRO(ConsultaGasto)
    Km = fetch_data(ConsultaKm)
    Bloqueo = fetch_data(ConsultaBloqueo)
    ETAs = fetch_data(ConsultaETA)
    
    file_path = r'C:\Users\hernandezm\Desktop\DBasignacion.xlsx'
    # Cargar los datos de la hoja 'DB'
    data = pd.read_excel(file_path, sheet_name='DB')
    # Eliminar las columnas que solo contienen NaN
    data_clean = data.dropna(axis=1, how='all')
    planas = planas[~planas['Remolque'].isin(data_clean['Remolque'])]
           
    return planas, Operadores, Cartas, Gasto, Km, Bloqueo, ETAs
      
def asignacion2(planasPorAsignar, calOperador):
    if (planasPorAsignar['remolque_b'] == 0).any():
        calOperador= calOperador[calOperador['OperadorBloqueado'].isin(['No'])]
            
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
        f_concatenado = f_concatenado[['Ruta', 'remolque_a', 'remolque_b', 'Operador']]

        f_concatenado.rename(columns={
        'remolque_a': 'Plana 1',
        'remolque_b': 'Plana 2',
        }, inplace=True)

        f_concatenado =  f_concatenado.to_html()
        return render_template('asignacionDIA.html', datos_html=f_concatenado)
    else:
        #calOperador= calOperador[calOperador['OperadorBloqueado'].isin(['No'])]
        calOperador = calOperador.sort_values(by=['CalFinal', 'Tiempo Disponible'], ascending=[False, False])
        calOperador = calOperador.reset_index(drop=True)
        calOperador.index = calOperador.index + 1
        
        planasPorAsignar = planasPorAsignar.sort_values(by='Monto', ascending=False)
        planasPorAsignar = planasPorAsignar.reset_index(drop=True)
        planasPorAsignar.index = planasPorAsignar.index + 1

        #f_concatenado = pd.concat([planasPorAsignar, calOperador], axis=1)
        f_concatenado= pd.merge(planasPorAsignar, calOperador, left_index=True, right_index=True, how='left')
        f_concatenado = f_concatenado[['Ruta', 'remolque_a', 'remolque_b', 'Operador']]

        f_concatenado.rename(columns={
        'remolque_a': 'Plana 1',
        'remolque_b': 'Plana 2'
        }, inplace=True)

        f_concatenado =   f_concatenado.to_html()

        return render_template('asignacionDIA.html', datos_html=f_concatenado)

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