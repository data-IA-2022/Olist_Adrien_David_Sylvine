import os

import yaml
from flask import Flask, Response, jsonify, redirect, render_template, request, url_for
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app_model import *


app = Flask(__name__)

secret="ef55f77145dc4e62b3d480efbdeb7589"

# for local developpement
# with open('config.yaml', 'r') as file:
#     config = yaml.safe_load(file)
# OLIST=config['pgsql_writer']

# for docker run
OLIST=os.environ['OLIST'] 

print('OLIST=', OLIST)
engine = create_engine(OLIST)
print(engine)


@app.route("/")
def hello_world(): 
    with Session(engine) as session:
        it = session.query(ProductCategory).all()
    return render_template('olist_trad.html', it=it, key=secret)


@app.route("/api/categories", methods=['GET'])
def cat_list():
    # Vérification que la requète est authentifiée
    key=''
    if 'key' in request.form:
        key=request.form['key']
    if 'Subscription-Key' in request.headers:
        key=request.headers['Subscription-Key']
    if key != secret:
        return Response('Pas OK', 401)
    # Récupère la liste des ProductCategory
    with Session(engine) as session:
        it = session.query(ProductCategory).all()
    print(it) 
    return jsonify([pc.to_json() for pc in it]) # Retourne une liste JSON




@app.route("/api/category", methods=['POST'])
def cat_update():
    # Vérification que la requète est authentifiée
    key=''
    if 'key' in request.form:
        key=request.form['key']
    if 'Subscription-Key' in request.headers:
        key=request.headers['Subscription-Key']
    if key != secret:
        return Response('Pas OK', 401)
    # récupération des paramètres cat et fr de la requête
    pk=request.form['cat'] 
    fr=request.form['fr']
    print('cat_update: ', pk, fr)
    # mise à jour de l'objet ProductCategory correspondant
    with Session(engine) as session:
        pc = session.query(ProductCategory).get(pk) 
        pc.set_fr(fr)
        session.commit() 
    return redirect(url_for('hello_world')) # redirection vers le chemin /
