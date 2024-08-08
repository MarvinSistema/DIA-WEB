import cProfile
import pstats

def main():
    # Importa y ejecuta tu aplicación Flask
    from app import app  # Asegúrate de que 'app' es el nombre correcto del archivo sin la extensión .py
    app.run()

if __name__ == "__main__":
    # Ejecuta el perfilado y guarda los resultados en 'profiling_results'
    cProfile.run('main()', 'profiling_results')

    # Procesa y muestra los resultados del perfilado
    p = pstats.Stats('profiling_results')
    p.sort_stats('cumulative').print_stats(10)
