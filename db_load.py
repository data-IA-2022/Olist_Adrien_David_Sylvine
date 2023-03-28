import gzip
import time
from math import ceil
from utils import calc_time, relative_path, connect_to_db, connect_ssh_tunnel

import pandas as pd
import sqlalchemy.types as SQLAT
import yaml
from sqlalchemy import Column, ForeignKey, MetaData, Table, ForeignKeyConstraint


def convert_dtype(types: dict) -> dict:

    conv = {
        'VARCHAR':      SQLAT.VARCHAR,
        'CHAR':         SQLAT.CHAR,
        'SMALLINT':     SQLAT.SMALLINT,
        'BOOLEAN':      SQLAT.BOOLEAN,
        'INTEGER':      SQLAT.INTEGER,
        'FLOAT':        SQLAT.FLOAT,
        'DOUBLE':       SQLAT.DOUBLE,
        'DATETIME':     SQLAT.DATETIME,
        'TIMESTAMP':    SQLAT.TIMESTAMP,
        'TEXT':         SQLAT.TEXT,
    }

    return {k: conv[v]() if len(v.split()) == 1 else conv[v.split()[0]](int(v.split()[1])) for k, v in types.items()}


##### Chargement des configs
config = yaml.safe_load(open(relative_path('config_db_load.yaml'), 'r'))

param = config['param']

##### Params
ch_size = param['chunksize']
doss = param['folder']

meta = MetaData()

##### Connexion bdd
print(f"\nConnexion à la bdd...")

config_file = relative_path('config.yaml')

ssh_tunnel = connect_ssh_tunnel(config_file)
engine = connect_to_db(config_file)

if engine == None:
    quit()

print('\nConnexion établie !', end='\n\n')

##### Noms des Tables + temps
table_names = list(config['tables'].keys())
global_time = time.time()

##### Boucle création des tables
print("Création des tables :", end='\n\n')

dict_references = {}

n_table = 0
for table_name in table_names:

    n_table += 1

    table_config = config['tables'][table_name]
    column_types = convert_dtype(table_config['column_types'])
    columns = []

    for column_name in column_types:
        if 'foreign_keys' in table_config and column_name in table_config['foreign_keys']:

            reference = table_config['foreign_keys'][column_name]

            if reference not in dict_references:
                dict_references[reference] = pd.Series(name=reference.split('.')[1])

            columns.append(Column(
                column_name,
                column_types[column_name],
                ForeignKey(reference),
                primary_key= column_name in table_config['primary_keys']
            ))
        else:
            columns.append(Column(
                column_name,
                column_types[column_name],
                primary_key= column_name in table_config['primary_keys']
            ))

    if 'multi_foreign_keys' in table_config:

        FK_cols, FK_refs = zip(*table_config['multi_foreign_keys'].items())

        dict_references[FK_refs[0].split('.')[0] + '..'] = pd.DataFrame(columns=[k.split('.')[1] for k in FK_refs])

        Table(
            table_name,
            meta,
            *columns,
            ForeignKeyConstraint(FK_cols, FK_refs),
        )

    else:
        Table(
            table_name,
            meta,
            *columns
        )

    print(f"{n_table:2}/{len(table_names)} - {table_name}")

print("\nReferences à vérifier :", ', '.join(dict_references.keys()), end='\n\n')

meta.create_all(engine)

conn = engine.connect()

##### Boucle remplissage des tables
print("Remplissage des tables :", end='\n\n')

n_table = 0
for table_name in table_names:

    n_table += 1
    table_time = time.time()

    table_config = config['tables'][table_name]
    path = relative_path(doss, table_config['name_file'])

    print(f"{n_table:2}/{len(table_names)} - {table_name} - nombre de lignes : ", end='')

    nb_lines = sum(1 for _ in (gzip.open(path) if table_config.get('gzip') else open(path, encoding='utf-8'))) - 1

    print(f"{nb_lines:,}")

    nb_chunks = ceil(nb_lines / ch_size)

    column_types = convert_dtype(table_config['column_types'])

    separator = table_config['separator'] if 'separator' in table_config else ','

    with pd.read_csv(path, sep=separator, na_values=table_config['na_values'], low_memory=False, chunksize=ch_size) as chunk_it:

        n = 0
        for df in chunk_it:

            lines_time = time.time()

            if explode_on := table_config.get('explode_on'):

                new_name = table_config.get('explode_new_name', explode_on)

                df[new_name] = df[explode_on].apply(lambda x: str(x).split(','))

                df = df.explode(new_name).dropna()

            df = df[list(column_types.keys())]

            for FK_cols, FK_refs in table_config.get('foreign_keys', {}).items():

                ok = df[FK_cols].isin(dict_references[FK_refs])

                if not df[~ok].empty:
                    df[~ok].to_sql(table_name + '_echecs', conn, dtype = column_types, if_exists='append', index=False)

                df = df[ok]

            if multi_fk := table_config.get('multi_foreign_keys'):

                FK_cols, FK_refs = zip(*table_config['multi_foreign_keys'].items())
                table_ref = FK_refs[0].split('.')[0]
                FK_refs = [k.split('.')[1] for k in FK_refs]

                dict_rename = {k: v for k, v in zip(FK_cols, FK_refs)}

                df_to_compare = df[list(FK_cols)].rename(columns=dict_rename)
                dict_ref_to_compare = dict_references[table_ref + '..']

                ok = df_to_compare.apply(lambda z: (z == dict_ref_to_compare).all(1).any(), axis=1)

                if not df[~ok].empty:
                    df[~ok].to_sql(table_name + '_echecs', conn, dtype = column_types, if_exists='append', index=False)

                df = df[ok]

            names_references = [k for k in dict_references.keys() if k.split('.')[0] == table_name]

            for name_reference in names_references:

                a = dict_references[name_reference]
                dict_references[name_reference] = pd.concat((a, df[a.name if type(a) == pd.Series else a.columns]), ignore_index=True)

            df.to_sql(table_name, conn, dtype = column_types, if_exists='append', index=False)

            n += 1
            print(f" - {n_table:2}/{len(table_names)} - {table_name} : {n:5,} / {nb_chunks:,}  -  {n/nb_chunks:8.3%}  -  {calc_time(lines_time)}")

    print(f" ----- {calc_time(table_time)}", end='\n\n')

##### Commit pour être sûr
conn.commit()

print("Temps total :", calc_time(global_time))

if ssh_tunnel != None:
    ssh_tunnel.close()