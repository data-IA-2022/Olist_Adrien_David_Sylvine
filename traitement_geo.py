from collections import Counter

import pandas as pd
from unidecode import unidecode

from utils import relative_path


df = pd.read_csv(relative_path("data", "olist_geolocation_dataset.csv"))

print(df.shape)

df["geolocation_city"] = df["geolocation_city"].apply(lambda x: unidecode(x).strip().lower())

df_latlng = df[["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"]].groupby("geolocation_zip_code_prefix").mean()

df_citystate = df[["geolocation_zip_code_prefix", "geolocation_city", "geolocation_state"]].groupby("geolocation_zip_code_prefix").agg(lambda srs: Counter(list(srs)).most_common(1)[0][0])

df = pd.concat((df_latlng, df_citystate), axis=1).reset_index()

print(df.shape)
print(df)

df.to_csv(relative_path("data", "clean_geolocation.csv"))
