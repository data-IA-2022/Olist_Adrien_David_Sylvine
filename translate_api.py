import requests
from googletrans import Translator


# adresse de base: pointe vers le port 5000 de la VM
base_url="http://groupe2-ads.francecentral.cloudapp.azure.com:5000/" 

# pour dévelopement en local:
# base_url="http://localhost:5000/" 
print(f'base_url : {base_url}')

translator = Translator(service_urls=['translate.googleapis.com'])

r = requests.get(base_url+"/api/categories")

print(f'status_code: {r.status_code}')
# print(r.text)

for cat in r.json():

	print(cat)
	pk=cat['product_category_name']

	if cat['product_category_name_french'] is None:
	    # Si pas de version FR

		txt=cat['product_category_name_english'].replace('_', ' ')
		traduc = translator.translate(txt, dest='fr').text
		traduc = traduc.replace(' ', '_')
		print(f"TRADUCTION de '{txt}' -> '{traduc}'")

		# Requete API OLIST pour update
		r2 = requests.post(base_url+"/api/category", data={'cat':pk, 'fr': traduc})
		print(r2.status_code)
		#print(r2.text)
