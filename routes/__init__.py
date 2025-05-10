from flask import Blueprint

api_bp = Blueprint('api', __name__)
views_bp = Blueprint('views', __name__)

from . import api, views
