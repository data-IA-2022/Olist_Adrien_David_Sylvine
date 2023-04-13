from flask import Flask, jsonify, render_template, request, redirect, url_for, Response
from app_model import *
from sqlalchemy.orm import Session
import yaml, os
from sqlalchemy import create_engine, text

app = Flask(__name__)



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
    return render_template('olist_trad.html', it=it)



@app.route("/api/categories", methods=['GET'])
def cat_list():
    with Session(engine) as session:
        it = session.query(ProductCategory).all()
    print(it) 
    return jsonify([pc.to_json() for pc in it])




@app.route("/api/category", methods=['POST'])
def cat_update():
    pk=request.form['cat'] # est dictionnaire contenant les valeurs 
    fr=request.form['fr']
    print('cat_update: ', pk, fr)
    with Session(engine) as session:
        pc = session.query(ProductCategory).get(pk) 
        pc.set_fr(fr)
        session.commit()
    return redirect(url_for('hello_world')) # jsonify('OK')
