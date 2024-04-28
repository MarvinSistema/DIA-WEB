from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from config import DB_CONFIG, DB_CONFIG_PRO

def create_engine_db():
    """Crea y devuelve un motor SQLAlchemy basado en la configuración de la base de datos."""
    connection_str = f"mssql+pyodbc://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['server']}/{DB_CONFIG['database']}?driver={DB_CONFIG['driver']}"
    engine = create_engine(connection_str)
    return engine

def create_engine_db_PRO():
    """Crea y devuelve un motor SQLAlchemy basado en la configuración de la base de datos."""
    connection_str = f"mssql+pyodbc://{DB_CONFIG_PRO['username']}:{DB_CONFIG_PRO['password']}@{DB_CONFIG_PRO['server']}/{DB_CONFIG_PRO['database']}?driver={DB_CONFIG_PRO['driver']}"
    engine = create_engine(connection_str)
    return engine


def fetch_data(query):
    """Ejecuta una consulta y devuelve los resultados como un DataFrame de Pandas."""
    engine = create_engine_db()
    try:
        with engine.connect() as connection:
            result = pd.read_sql(query, connection)
    except SQLAlchemyError as e:
        print(f"Error al realizar la consulta: {e}")
        result = pd.DataFrame()
    finally:
        engine.dispose()  # Asegurarse de cerrar la conexión
    return result


def fetch_data_PRO(query):
    """Ejecuta una consulta y devuelve los resultados como un DataFrame de Pandas."""
    engine = create_engine_db_PRO()
    try:
        with engine.connect() as connection:
            result = pd.read_sql(query, connection)
    except SQLAlchemyError as e:
        print(f"Error al realizar la consulta: {e}")
        result = pd.DataFrame()
    finally:
        engine.dispose()  # Asegurarse de cerrar la conexión
    return result