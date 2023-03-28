from flask import Flask
import psycopg2
from 

app = Flask(__name__)

# connexion à la BDD
# vers la VM
# conn = psycopg2.connect("postgresql://Groupe2_ADS:Groupe2_ADSroot@groupe2-ads.francecentral.cloudapp.azure.com:5432/OList") # adapter pour accéder au port 172.17.0.2 aussi

# en local
connection = psycopg2.connect("postgresql://postgres:postgres@localhost:5432/OList")


@app.route('/')
def index():
    conn = connection
    return 'Index Page'

@app.route('/hello')
def hello():
    return 'Bienvenue sur cette super app youpi'