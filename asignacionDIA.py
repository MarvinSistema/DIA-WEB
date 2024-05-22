from flask import Flask, render_template, Blueprint
import pandas as pd
from twilio.rest import Client
from datetime import datetime, timedelta
from db_manager import fetch_data, fetch_data_PRO
from planasPorAsignar import procesar_planas, procesar_operadores

asignacionDIA = Blueprint('asignacionDIA', __name__)
@asignacionDIA.route('/')
def index():
    planas, Operadores, Cartas, Gasto, Km, Bloqueo, ETAs  = cargar_datos()
    planasPorAsignar = procesar_planas(planas)
    operadores_sin_asignacion = procesar_operadores(Operadores)
    asignacionesPasadasOperadores=  asignacionesPasadasOp(Cartas)
    siniestroKm= siniestralidad(Gasto, Km)
    ETAi= eta(ETAs)
    asignacionF = asignacion(planasPorAsignar, operadores_sin_asignacion, Bloqueo, asignacionesPasadasOperadores, siniestroKm, ETAi)
    I=sender()

    return asignacionF
    
def cargar_datos():
    consulta_planas = "SELECT * FROM DimTableroControlRemolque WHERE PosicionActual = 'NYC' AND Estatus = 'CARGADO EN PATIO' AND Ruta IS NOT NULL AND CiudadDestino != 'MONTERREY'AND CiudadDestino != 'GUADALUPE'"
    consulta_operadores = "SELECT * FROM DimTableroControl"
    ConsultaCartas = f"SELECT * FROM ReporteCartasPorte WHERE FechaSalida > '2024-01-01'"
    ConsultaGasto= f"SELECT *   FROM DimReporteUnificado"
    ConsultaKm = f"SELECT *   FROM DimRentabilidadLiquidacion"
    ConsultaBloqueo = f"SELECT *   FROM DimOperadores Where Activo = 'Si'"
    ConsultaETA = f"SELECT NombreOperador, FechaFinalizacion, CumpleETA FROM DimIndicadoresOperaciones WHERE FechaSalida > '2024-01-01' AND FechaLlegada IS NOT NULL"
    planas = fetch_data(consulta_planas)
    Operadores = fetch_data(consulta_operadores)
    Cartas = fetch_data(ConsultaCartas)
    Gasto = fetch_data_PRO(ConsultaGasto)
    Km = fetch_data(ConsultaKm)
    Bloqueo = fetch_data(ConsultaBloqueo)
    ETAs = fetch_data(ConsultaETA)
    
    return planas, Operadores, Cartas, Gasto, Km, Bloqueo, ETAs

def asignacion(planasPorAsignar, operadores_sin_asignacion, Bloqueo, asignacionesPasadasOperadores, siniestroKm, ETAs):

    if (planasPorAsignar['remolque_b'] == 0).any():
        calOperador= pd.merge(operadores_sin_asignacion, Bloqueo, left_on='Operador', right_on='NombreOperador', how='left')
        calOperador= pd.merge(calOperador, asignacionesPasadasOperadores, left_on='Operador', right_on='Operador', how='left')
        calOperador= pd.merge(calOperador, siniestroKm, left_on='ClaveTractor', right_on='Tractor', how='left')
        calOperador= pd.merge(calOperador, ETAs, left_on='Operador', right_on='NombreOperador', how='left')

        calOperador['ViajeCancelado']= 20
     
        calOperador['CalFinal']= calOperador['CalificacionVianjesAnteiores']+calOperador['PuntosSiniestros']+calOperador['ViajeCancelado']+calOperador['SAC']
        #calOperador=calOperador[['Operador', 'OperadorBloqueado', 'Bueno', 'Regular', 'Malo', 'CalificacionVianjesAnteiores', 'PuntosSiniestros', 'ViajeCancelado', 'PuntosSAC', 'CalFinal', 'Tiempo Disponible']]
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
        f_concatenado = f_concatenado[['Ruta', 'remolque_a', 'remolque_b', 'Monto', 'Operador', 'Bueno',	'Regular',	'Malo',	'CalificacionVianjesAnteiores',	'PuntosSiniestros',	'ViajeCancelado',	'SAC',	'CalFinal']]

        f_concatenado.rename(columns={
        'remolque_a': 'Plana 1',
        'remolque_b': 'Plana 2',
        'CalificacionVianjesAnteiores': 'Calificacion Viajes Anteriores',
        'PuntosSiniestros':'Puntos Siniestros',
        'CalFinal':'Calificacion Final Operador'
        }, inplace=True)

        f_concatenado =  f_concatenado.to_html()

        return render_template('asignacionDIA.html', datos_html=f_concatenado)
    else:
        calOperador= pd.merge(operadores_sin_asignacion, Bloqueo, left_on='Operador', right_on='NombreOperador', how='left')
        calOperador = calOperador[calOperador['UOperativa_y'].isin(['U.O. 01 ACERO', 'U.O. 02 ACERO', 'U.O. 03 ACERO', 'U.O. 04 ACERO', 'U.O. 07 ACERO','U.O. 39 ACERO'])]
        calOperador= pd.merge(calOperador, asignacionesPasadasOperadores, left_on='Operador', right_on='Operador', how='left')
        calOperador= pd.merge(calOperador, siniestroKm, left_on='ClaveTractor', right_on='Tractor', how='left')
        calOperador= pd.merge(calOperador, ETAs, left_on='Operador', right_on='NombreOperador', how='left')

        calOperador['ViajeCancelado']= 20
             
        calOperador['CalFinal']= calOperador['CalificacionVianjesAnteiores']+calOperador['PuntosSiniestros']+calOperador['ViajeCancelado']+calOperador['SAC']
        #calOperador=calOperador[['Operador', 'OperadorBloqueado', 'Bueno', 'Regular', 'Malo', 'CalificacionVianjesAnteiores', 'PuntosSiniestros', 'ViajeCancelado', 'PuntosSAC', 'CalFinal', 'Tiempo Disponible']]
        calOperador= calOperador[calOperador['OperadorBloqueado'].isin(['No'])]
        #calOperador = calOperador[calOperador['UOperativa_y'].isin(['U.O. 01 ACERO', 'U.O. 02 ACERO', 'U.O. 03 ACERO', 'U.O. 04 ACERO', 'U.O. 07 ACERO','U.O. 39 ACERO'])]
        
        calOperador = calOperador.sort_values(by=['CalFinal', 'Tiempo Disponible'], ascending=[False, False])
        calOperador = calOperador.reset_index(drop=True)
        calOperador.index = calOperador.index + 1
        
        planasPorAsignar = planasPorAsignar.sort_values(by='Monto', ascending=False)
        planasPorAsignar = planasPorAsignar.reset_index(drop=True)
        planasPorAsignar.index = planasPorAsignar.index + 1

        #f_concatenado = pd.concat([planasPorAsignar, calOperador], axis=1)
        f_concatenado= pd.merge(planasPorAsignar, calOperador, left_index=True, right_index=True, how='left')
        f_concatenado = f_concatenado[['Ruta', 'remolque_a', 'remolque_b', 'Monto', 'Operador', 'Bueno',	'Regular',	'Malo',	'CalificacionVianjesAnteiores',	'PuntosSiniestros',	'ViajeCancelado',	'SAC',	'CalFinal']]

        f_concatenado.rename(columns={
        'remolque_a': 'Plana 1',
        'remolque_b': 'Plana 2',
        'CalificacionVianjesAnteiores': 'Calificacion Viajes Anteriores',
        'PuntosSiniestros':'Puntos Siniestros',
        'CalFinal':'Calificacion Final Operador'
        }, inplace=True)

        f_concatenado =   f_concatenado.to_html()

        return render_template('asignacionDIA.html', datos_html=f_concatenado)
        
def asignacionesPasadasOp(Cartas):
    CP= Cartas.copy()
    # 30 dias atras
    fecha_actual = datetime.now()
    fecha_30_dias_atras = fecha_actual - timedelta(days=60)
    # Dividir la columna 'Ruta' por '||' y luego por '-' para obtener origen
    CP[['ID1', 'Ciudad_Origen', 'Ciudad_Destino']] = CP['Ruta'].str.split(r' \|\| | - ', expand=True)
    # Filtro Mes actual, UO, ColumnasConservadas, Ciudad Origen
    CP = CP[CP['FechaSalida'] >= fecha_30_dias_atras]
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

    ''' 
    # Calculo de Puntajes
    CP['Puntajes'] = CP['Bueno'] - CP['Malo']

    (CP['Malo']*2)

    # Funsion para asiganar puntaje
    def Puntaje(puntajes):
        if puntajes >= 2:
            return 30
        elif puntajes <= -2:
            return 50
        else:
            return 40
        
    # Asignar los puntajes por fila
    CP['CalificacionVianjesAnteiores'] = CP['Puntajes'].apply(lambda puntajes: Puntaje(puntajes))
    
    # Resetear el índice para obtener 'Operador' como una columna
    '''
    # Define los pesos
    P1 = 2
    P2 = 1

    # Calcula el puntaje bruto para cada fila
    CP['PuntajeBruto'] = (CP['Malo'] * P1) - (CP['Bueno'] * P2)

    # Calcula el máximo y mínimo puntaje bruto que podría existir basado en los datos del DataFrame
    max_puntaje_posible = (CP['Malo'] + CP['Bueno']) * P1
    min_puntaje_posible = (CP['Malo'] + CP['Bueno']) * P2 * -1

    # Determina el rango de puntaje total
    rango_puntaje = max_puntaje_posible.max() - min_puntaje_posible.min()

    # Ajusta los puntajes para que el mínimo sea 0 y normaliza a una escala de 50
    CP['CalificacionVianjesAnteiores'] = 15+40 * (CP['PuntajeBruto'] - min_puntaje_posible.min()) / rango_puntaje

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
    K['Siniestralidad'] = K['TotalFinal'] / K['KmsReseteo']

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

def eta(ETAi):
    # Crear una tabla pivote para contar la cantidad de 'Bueno', 'Malo' y 'Regular' para cada operador
    ETAi= pd.pivot_table(ETAi, index='NombreOperador', columns='CumpleETA', aggfunc='size', fill_value=0)
    # Resetear el índice para obtener 'Operador' como una columna
    ETAi= ETAi.reset_index()
    ETAi['SAC'] = ((ETAi['Cumple'] / (ETAi['Cumple'] + ETAi['No Cumple'])) * 10).round(0).astype(int)



    return ETAi 

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

