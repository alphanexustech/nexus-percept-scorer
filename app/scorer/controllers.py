from flask import Blueprint
from flask import render_template, redirect, url_for, jsonify, request

import requests, json

from nltk.corpus import stopwords
from nltk.tokenize import wordpunct_tokenize
from nltk.stem.snowball import SnowballStemmer
from nltk.stem.wordnet import WordNetLemmatizer

# Databases
from config.databases import percept_corpus

# Configuration
from config import configurations

# mongo dependencies
import pymongo
from flask_pymongo import ObjectId

# bson
import json
from bson import json_util

# Date
from datetime import datetime

# File processing
import os
import csv

# Application context
from app import app

def default():
    return 'Hello Scorers!'

'''
The following methods are used for getting the data from the database/csv file, but should not
be used for data processing. Use the app context (from app import app) instead!
==
get_frequency_distribution()
get_bucketed_frequency_distribution()
get_percept_stop_words()
get_member_distribution()
get_bucketed_member_distribution()
common_set_percepts()
'''
def get_frequency_distribution():
    rcd_cursor = percept_corpus.db[configurations.freq_dist_collection].find({});
    frequency_distribution = {}
    for i in rcd_cursor:
        percepts = list(set(i['percepts']))
        common_percepts = []
        for r in percepts:
            if r in app.common_set_percepts:
                common_percepts.append(app.common_set_percepts[r])
        if len(common_percepts) > 0:
            frequency_distribution[i['word']] = common_percepts
    return {
        "status": "OK",
        "frequency_distribution": frequency_distribution
    }

def get_bucketed_frequency_distribution():
    rcd_cursor = percept_corpus.db[configurations.freq_dist_collection].find({});
    frequency_distribution = {}
    for i in rcd_cursor:
        percepts = list(set(i['percepts']))
        common_percepts = []
        for r in percepts:
            if r in app.common_set_percepts:
                common_percepts.append(app.common_set_percepts[r])
        if len(common_percepts) > 0:
            if len(common_percepts) not in frequency_distribution:
                frequency_distribution[len(common_percepts)] = [i['word']]
            else:
                frequency_distribution[len(common_percepts)].append(i['word'])
    return {
        "status": "OK",
        "frequency_distribution": frequency_distribution
    }

def get_percept_stop_words():
    data = get_bucketed_frequency_distribution()
    buckets = data['frequency_distribution']
    stopwords = []
    # Add words that show up in over half of the percepts.
    for bucket in buckets:
        if bucket > 300:
            stopwords += buckets[bucket]
    # Add words that show up in just once.
    stopwords += buckets[1]
    return {
        "status": "OK",
        "percept_stop_words": stopwords,
        "length_percept_stop_words": len(stopwords)
    }

def get_member_distribution():
    rcd_cursor = percept_corpus.db[configurations.membership_collection].find({});
    member_distribution = {}
    for i in rcd_cursor:
        if i['percept'] in app.common_set_percepts:
            member_distribution[app.common_set_percepts[i['percept']]] = i['data']
    return {
        "status": "OK",
        "member_distribution": member_distribution
    }

def get_bucketed_member_distribution():
    rcd_cursor = percept_corpus.db[configurations.membership_collection].find({});
    member_distribution = {}
    for i in rcd_cursor:
        if i['percept'] in app.common_set_percepts:
            if len(i['data']) not in member_distribution:
                member_distribution[len(i['data'])] = [app.common_set_percepts[i['percept']]]
            else:
                member_distribution[len(i['data'])].append(app.common_set_percepts[i['percept']])
    return {
        "status": "OK",
        "member_distribution": member_distribution
    }

def common_set_percepts(flaskResponse=None):

    #
    # Manually pruned data from CSV
    #

    if sys.version_info[0] == 2:  # Not named on 2.6
        kwargs = {}
    else:
        kwargs ={
            'encoding': 'utf8',
            'newline': ''
            }

    script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in
    rel_path = "..\\..\\data\\meta_alternative_name_list.csv"
    if os.name == 'posix': # If on linux system
        rel_path = "../../data/meta_alternative_name_list.csv"
    abs_file_path = os.path.join(script_dir, rel_path)
    percepts = {};

    with open(abs_file_path, **kwargs) as csvfile:
        data = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in data:
            if row[2] == '1':
                if row[1] != '':
                    percepts[row[0]] = row[1]
                else:
                    percepts[row[0]] = row[0]

    csvfile.close();

    return {
        "status": "OK",
        "percepts": percepts,
        "len_percepts": len(percepts)
    }

'''
Other useful methods for understanding the corpus
'''
def get_member_list():
    rcd_cursor = percept_corpus.db[configurations.membership_collection].find({});
    member_list = []
    for i in rcd_cursor:
        member_list.append(i['percept'])
    return {
        "status": "OK",
        "member_list": list(set(member_list)),
        "member_list_length": len(list(set(member_list)))
    }

'''
word_count - the total number of words found in the document signaling the percept
percept_length - the total possible number of percepts
'''
def calculate_normalized_percept_score(word_count, percept_length):
    return float(word_count)/percept_length * 100

def calculate_percept_density_score(r_percept_score, length_words_no_stop):
    r_percept_density_score = float(r_percept_score)/length_words_no_stop * 100
    return r_percept_density_score

'''
word_count - the total number of words found in the document signaling the percept
percept_length - the total possible number of percepts
'''
def calculate_percept_scores(word_count, percept_length, length_words_no_stop):
    scores = {}
    scores['normalized_percept_score'] = calculate_normalized_percept_score(word_count, percept_length)
    scores['percept_density_score'] = calculate_percept_density_score(scores['normalized_percept_score'], length_words_no_stop)
    return scores

def format_name(name):
    # Handle the other names, but return back a title case format in most cases
    if (name == 'yang di-pertuan agong'):
        return 'Yang di-Pertuan Agong'
    if (name == 'son of heaven'):
        return 'Song of Heaven'
    else:
        return name.title()

def format_data(process_type, list_of_words, lang, freqdist, memberdist):
    # Stem and lemmatize
    stemmer = SnowballStemmer(lang) # This is the stemmer
    lemma = WordNetLemmatizer() # This is the lemma

    result = {}

    r_percepts_found = {}
    for word in list_of_words:
        # The variable 'w' is the changed word, based on the process_type
        if process_type == 'stem':
            w = stemmer.stem(word)
        elif process_type == 'lemma':
            w = lemma.lemmatize(word)
        else: # Default to 'base'
            w = word

        if w in freqdist:
            found_percepts = list(set(freqdist[w]))
            for percept in found_percepts:
                if percept not in r_percepts_found:
                    r_percepts_found[percept] = [w]
                else:
                    r_percepts_found[percept] += [w]

    return r_percepts_found

def process_text(doc=None):
    tokenized_words = wordpunct_tokenize(doc)
    lang = 'english'

    # Stop Words
    stop_words = stopwords.words(lang)
    percept_stop_words = app.percept_stop_words
    all_stop_words = stop_words + percept_stop_words
    freqdist = app.frequency_distribution
    memberdist = app.member_distribution

    list_of_words = [i.lower() for i in tokenized_words if i.lower() not in all_stop_words]

    sorted_result = [];
    base_results = format_data('base', list_of_words, lang, freqdist, memberdist)
    stem_results = format_data('stem', list_of_words, lang, freqdist, memberdist)
    lemma_results = format_data('lemma', list_of_words, lang, freqdist, memberdist)
    all_results = {
        'base_words': base_results,
        'stem_words': stem_results,
        'lemma_words': lemma_results,
    }

    reformatted_result = {}
    # 1) Combine all the results form data formatting - into a reformatted result
    for result in all_results:
        for percept in all_results[result]:
            percept_words = all_results[result][percept]
            if percept not in reformatted_result:
                new_r_percept = {}
                # This finds and creates the list of words found...
                # ... for a percept
                # ... for a base/stem/lemma
                new_r_percept['all_words'] = {
                    "words": percept_words,
                    "word_count": len(percept_words)
                }
                new_r_percept[result] = {
                    "words": percept_words,
                    "word_count": len(percept_words)
                }
                reformatted_result[percept] = new_r_percept
            else:
                updated_r_percept = reformatted_result[percept]
                # This finds and updates the list of words found...
                # ... for a percept
                # ... for a base/stem/lemma
                updated_r_percept['all_words']['words'] = updated_r_percept['all_words']['words'] + percept_words
                updated_r_percept['all_words']['word_count'] = len(updated_r_percept['all_words']['words'])
                updated_r_percept[result] = {
                    "words": percept_words,
                    "word_count": len(percept_words)
                }
                reformatted_result[percept] = updated_r_percept

    # 2) Combine all the results form re-formatting - into a final_result
    final_result = []
    for percept in reformatted_result:
        words_found = (reformatted_result[percept]['all_words']['words'])
        percept_length = len(memberdist[percept])
        r = {}
        r['name'] = percept
        r['pretty_name'] = format_name(percept)
        r['words_found'] = words_found
        r['word_count'] = reformatted_result[percept]['all_words']['word_count']
        r['percept_length'] = percept_length
        r['document_length'] = len(tokenized_words)
        r['percept_metadata'] = reformatted_result[percept]
        # IDEA: Find POS for each word here, too.
        r['scores'] = calculate_percept_scores(len(words_found), percept_length, len(tokenized_words))
        final_result.append(r)

    sorted_final_result = sorted(final_result, key=lambda x: x['scores']['percept_density_score'], reverse=True)

    return sorted_final_result

def analyze_text(percept_set=None, doc=None):
    if percept_set == 'all_percepts': # Get the common_set
        if doc:
            result = process_text(doc)
            return {
                "doc": doc,
                "status": "OK",
                "percept_set": result,
                "percepts_found": len(result),
                "date": [datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]]
            }
        else:
            return {
                "status": "OK",
                "message": "The document is missing."
            }
    else:
        return {
            "status": "OK",
            "message": "Not Implemented"
        }
