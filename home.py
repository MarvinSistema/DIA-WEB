from flask import Flask, render_template
from flask import Blueprint



home = Blueprint('home', __name__)


@home.route('/')
def index():
    # Retornar el DataFrame como HTML
    return render_template('home.html')