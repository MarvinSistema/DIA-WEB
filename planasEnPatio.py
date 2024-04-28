from flask import Flask, render_template, Blueprint
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import time

planasEnPatio = Blueprint('planasEnPatio', __name__)

@planasEnPatio.route('/')
def index():
    # Parámetros de conexión
    server = '74.208.51.229'
    database = 'DWH_SPLGNYC'
    username = 'mhernandez'
    password = '7xkDa7j7ejT5qi!n'
    Planas = 'DimTableroControlRemolque'

    # Intentos de conexión configurables
    max_attempts = 3
    attempt = 0
    while attempt < max_attempts:
        try:
            # Cadena de conexión SQLAlchemy
            engine = create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=SQL+Server")
            # Consulta SQL optimizada
            ConsultaPlanas = f"""
            SELECT *
            FROM {Planas}
            WHERE PosicionActual = 'NYC' AND Estatus = 'CARGADO EN PATIO' AND Ruta IS NOT NULL AND CiudadDestino != 'MONTERREY'
            """
            # Ejecutar la consulta SQL y obtener el resultado como un dataframe
            Planas_df = pd.read_sql(ConsultaPlanas, engine)
            break  # Si la conexión es exitosa y la consulta se ejecuta, salimos del bucle
        except Exception as e:
            print(f"Error en la conexión o consulta SQL: {e}")
            attempt += 1
            time.sleep(5)  # Esperar 5 segundos antes del próximo intento
        finally:
            # Cerrar la conexión siempre que se crea
            engine.dispose()

    if attempt == max_attempts:
        return "No se pudo conectar a la base de datos después de varios intentos."

    # Procesamiento de datos
    Planas_df['Horas en patio'] = ((datetime.now() - Planas_df['FechaEstatus']).dt.total_seconds() / 3600.0).round(1)
    Planas_df['FechaEstatus'] = Planas_df['FechaEstatus'].dt.strftime('%Y-%m-%d %H:%M')
    Planas_df['ValorViaje'] = Planas_df['ValorViaje'].apply(lambda x: "${:,.0f}".format(x))
    Planas_df.sort_values(by=['FechaEstatus'], ascending=True, inplace=True)
    Planas_df = Planas_df[['Remolque', 'CiudadDestino', 'ValorViaje', 'Horas en patio']]
   
 

    Planas_df.reset_index(drop=True, inplace=True)
    Planas_df.index += 1


    # Convertir el DataFrame a HTML
    datos_renderizados = Planas_df.to_html()

    # Retornar el DataFrame como HTML
    return render_template('planasEnPatio.html', datos_html=datos_renderizados)

# Configuración de la app de Flask en caso de que sea el archivo principal
if __name__ == '__main__':
    app = Flask(__name__)
    app.register_blueprint(planasEnPatio)
    app.run(debug=True)
