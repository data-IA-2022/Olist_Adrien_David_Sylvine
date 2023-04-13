# import
import os

import pandas as pd
from IPython.display import display


# main
def main():
    path = '/home/dakoro/Data_IA/TP/Olist_Adrien_David_Sylvine/dataset/olist_data'
    fns = os.listdir(path)

    # delete duplicate geoloacation file
    geo = pd.read_csv('/home/dakoro/Data_IA/TP/Olist_Adrien_David_Sylvine/dataset/olist_data/olist_geolocation_dataset.csv')
    geo = geo.drop_duplicates(['geolocation_zip_code_prefix', 'geolocation_city', 'geolocation_state'], keep='first')
    geo.to_csv('/home/dakoro/Data_IA/TP/Olist_Adrien_David_Sylvine/dataset/olist_data/olist_geolocation_dataset.csv')

    # count le nombre de caractère
    for fn in fns:
        result = pd.read_csv(f"{path}/{fn}", sep=",")
        print(f"file : {fn}")
        for col in result.select_dtypes(include=[object]):
            print(col)
            col_bytes_len = int(result[col].str.encode('utf-8').str.len().max())
            print(f"+--> length : {col_bytes_len}")

if __name__=='__main__':
    main()