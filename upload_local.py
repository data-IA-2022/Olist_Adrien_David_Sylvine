from sqlalchemy import create_engine
from utils import get_config
import pandas as pd
import os
from IPython.display import display

engine = create_engine(get_config('PG'))

review = pd.read_csv('dataset/olist_data/olist_order_reviews_dataset.csv')
review.to_sql('Review', engine, if_exists='replace')

