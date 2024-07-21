from flask import Flask, redirect, url_for
from planasEnPatio import planasEnPatio
from planasPorAsignar import planasPorAsignar
from historicoAsignado import historicoAsignado
from contact import contact
from home import home
from about import about
import os

app = Flask(__name__)

# Registrar los Blueprints con sus prefijos de URL
app.register_blueprint(home, url_prefix='/home')
app.register_blueprint(about, url_prefix='/about')
app.register_blueprint(contact, url_prefix='/contact')
app.register_blueprint(planasEnPatio, url_prefix='/planasEnPatio')
app.register_blueprint(planasPorAsignar, url_prefix='/planasPorAsignar')
app.register_blueprint(historicoAsignado, url_prefix='/historicoAsignado')


@app.route('/')
def index():
    return redirect(url_for('home.index'))  # Redirigir a la página principal

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port)