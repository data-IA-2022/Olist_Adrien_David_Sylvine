# import librairies
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base() # créer la classe Base


class ProductCategory(Base):
    __tablename__ = "product_category_name_translation"

    product_category_name=Column(String, primary_key=True, index=True)
    product_category_name_english=Column(String)
    product_category_name_french=Column(String)


    def __repr__(self):
        # permet de modifier le format string de l'objet
        return f"ProductCategory[{self.product_category_name}]"


    def to_json(self):
        # retourne un dictionnaire contenant les valeurs d'une seule ligne
        return {
                'product_category_name': self.product_category_name,
                'product_category_name_english': self.product_category_name_english,
                'product_category_name_french': self.product_category_name_french,
                }


    def set_fr(self, x):
        # setter sur champ product_category_name_french
        self.product_category_name_french = x


    def get_fr(self):
        # getter sur le champ product_category_name_french
        return self.product_category_name_french


    def get_en(self):
        # getter sur le champ product_category_name_french
        return self.product_category_name_english
