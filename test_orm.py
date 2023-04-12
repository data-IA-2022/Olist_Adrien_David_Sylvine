from app_model import *
import json, yaml
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)
#print(config)
engine = create_engine(config['pgsql_writer'])


print('OK')
print(engine)


pc=ProductCategory(product_category_name='cat1', product_category_name_english='cat_en')
print(pc)

pcjson = [pc.to_json()]
print('en json:', pcjson)

print('Apres modif:')
pc.set_fr('cat_FR') # Utilisation du setter
print(pc)


print('FR:', pc.get_fr())

print('en json:', pc.to_json())

with Session(engine) as session:
    #session.add(pc)
    #session.commit()

    it = session.query(ProductCategory).filter(text("product_category_name_english is not NULL")).order_by('product_category_name_english')
    #print(it)
    for pc in it:
        print(f"  - {pc} {pc.get_en()}")

    obj = [pc.to_json() for pc in it]
    #print(json.dumps(obj))

    print('count', session.query(ProductCategory).count())



    '''pc2 = session.query(ProductCategory).get('bebes')
    print(pc2)
    pc2.set_FR('BB')
    print(pc2.to_json())
    session.commit()'''
