from planasEnPatio import planasEnPatio
from planasPorAsignar import planasPorAsignar
from contact import contact
from historicoAsignado import historicoAsignado
from home import home
from about import about
from flask import Flask, render_template


app = Flask(__name__)

# Registrar los Blueprints con sus prefijos de URL
app.register_blueprint(planasEnPatio, url_prefix='/planasEnPatio')
app.register_blueprint(home, url_prefix='/home')
app.register_blueprint(about, url_prefix='/about')
app.register_blueprint(contact, url_prefix='/contact')
app.register_blueprint(planasPorAsignar, url_prefix='/planasPorAsignar')
app.register_blueprint(historicoAsignado, url_prefix='/historicoAsignado')

if __name__ == '__main__':
    app.run(debug=True)
    
    
    




