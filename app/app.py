from flask import Flask

# bson
import json
from bson import json_util

# CORS dependencies
from flask_cors import CORS

app = Flask(__name__)

#CORS instance
cors = CORS(app, resources={r"/*": {"origins": "*"}}) #CORS :WARNING everything!

@app.route('/')
def home_page():
    return 'hello world'
