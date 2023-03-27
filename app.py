from flask import Flask
import psycopg2

app = Flask(__name__)

# connexion à la BDD
# vers la VM
conn = psycopg2.connect("postgresql://Groupe2_ADS:Groupe2_ADSroot@groupe2-ads.francecentral.cloudapp.azure.com:5432/postgres")

# en local
conn = psycopg2.connect("postgresql://Groupe2_ADS:Groupe2_ADSroot@groupe2-ads.francecentral.cloudapp.azure.com:5432/postgres")


@app.route('/')
def index():
    #conn = conn
    return 'Index Page'

@app.route('/hello')
def hello():
    return 'Bienvenue sur cette super app youpi'