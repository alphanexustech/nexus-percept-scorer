from flask import Blueprint
from flask import render_template, redirect, url_for, jsonify, request

import requests, json

from . import controllers

# mongo dependencies
import pymongo
from flask_pymongo import ObjectId

# bson
import json
from bson import json_util

scorer = Blueprint('scorer', __name__)

@scorer.route('/', methods=['GET'])
def default():
    return controllers.default()

@scorer.route('/freqdist/', methods=['GET'])
def get_frequency_distribution():
    return jsonify(controllers.get_frequency_distribution())

@scorer.route('/freqdist/bucketed/', methods=['GET'])
def get_bucketed_frequency_distribution():
    return jsonify(controllers.get_bucketed_frequency_distribution())

@scorer.route('/freqdist/stopwords/', methods=['GET'])
def get_percept_stop_words():
    return jsonify(controllers.get_percept_stop_words())

@scorer.route('/memberdist/', methods=['GET'])
def get_member_distribution():
    return jsonify(controllers.get_member_distribution())

@scorer.route('/memberdist/bucketed/', methods=['GET'])
def get_bucketed_member_distribution():
    return jsonify(controllers.get_bucketed_member_distribution())

@scorer.route('/commonsetnamelist/', methods=['GET'])
def common_set_percepts():
    return jsonify(controllers.common_set_percepts())

@scorer.route('/memberlist/', methods=['GET'])
def get_member_list():
    return jsonify(controllers.get_member_list())

@scorer.route('/<percept_set>/', methods=['POST'])
def analyze_text(percept_set=None):
    r = request.get_json()
    doc = r.get('doc')
    return jsonify(controllers.analyze_text(percept_set=percept_set, doc=doc))
