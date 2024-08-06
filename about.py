from flask import Flask, render_template
from flask import Blueprint



about = Blueprint('about', __name__)


@about.route('/')
def index():
    # Retornar el DataFrame como HTML
    return render_template('about.html')