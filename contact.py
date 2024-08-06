from flask import Flask, render_template
from flask import Blueprint



contact = Blueprint('contact', __name__)


@contact.route('/')
def index():
    # Retornar el DataFrame como HTML
    return render_template('contact.html')