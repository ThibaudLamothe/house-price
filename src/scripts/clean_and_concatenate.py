# Python libraries
import json
import numpy as np
import pandas as pd

# Personal functions
import functions as f
import database_connection as db_connection


################################################################################
################################################################################


def transform_to_numeric(df, col):
    df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def transform_prix(df):
    df['prix'] = df['prix'].apply(lambda x: x.split(' ')[0])
    df['prix'] = pd.to_numeric(df['prix'], errors='coerce') * 1000
    return df


def calculate_m2(df):
    df['prix_m2'] = df['prix'].div(df['surface']).apply(np.round, 2)
    return df


def fullfill_cp(df):
    def fullfilling(line):
        corresp = {'Rennes': 35000,
                   'Talence': 33400,
                   'Reims': 51100}
        if pd.isnull(line['code_postal']):
            if line['ville'] in corresp.keys():
                return corresp[line['ville']]
            return None
        return line['code_postal']

    df['code_postal'] = df.apply(fullfilling, axis=1)
    return df


def get_dept(df):

    # df['dept'] = df['code_postal'].apply(lambda x:x[:-3])  #if string
    # df['dept'] = pd.to_numeric(df['dept'], errors='coerce') # if string
    # df['dept'] = df['code_postal'].div(1000).apply(np.int) # if numeric but not work for nan
    df['dept'] = pd.to_numeric(df['code_postal'], errors='coerce')
    df['dept'] = df['dept'].apply(
        lambda x: np.int(x / 1000) if pd.isnull(x) == False else np.NaN)  # if numeric but not work for nan
    return df


def transform_string_col_into_dict(df, col='critere'):
    # print(df[col].head())
    # val = df[col].iloc[0]['Surface']
    # print(val)
    # df[col] = df[col].apply(lambda x: json.loads(x)) #.replace('\'', '"')))
    return df


def concat_criteres(df):
    list_id = []
    list_k = []
    list_v = []
    for i in df[['id_annonce', 'critere']].values:
        # try:
        for k, v in i[1].items():
            list_id.append(i[0])
            list_k.append(k)
            list_v.append(v)
        # except:
        #     print('Il n y a pas de critere pour cette annonce')
    df_crit = pd.DataFrame({'id_annonce': list_id, 'critere_name': list_k, 'critere_value': list_v})
    df_criteres = pd.pivot(df_crit.drop_duplicates(), index='id_annonce', columns='critere_name', values='critere_value')
    df_merge = pd.merge(df, df_criteres, on='id_annonce', how='left')
    return df_merge


def transform_surface(df):
    df['surface'] = df['Surface'].apply(lambda x: str(x).split(' ')[0])
    df['surface'] = pd.to_numeric(df['surface'], errors='coerce')
    df = df.drop('Surface', axis=1)
    return df


def parse_date(df, drop_=True, sep_date_heure=' ', name_col_date='date_annonce'):
    date_ = df[name_col_date].apply(lambda x: x.split(sep_date_heure)[0])
    heure_ = df[name_col_date].apply(lambda x: x.split(sep_date_heure)[-1])
    # heure_ = heure_.apply(lambda x : x.replace('h',':'))

    df['date_annonce'] = pd.to_datetime(date_, errors='coerce')
    df['heure_annonce'] = heure_  # pd.to_time(heure_, errors='coerce')

    if drop_:
        df = df.drop(name_col_date, axis=1)
    return df


def separate_ville_cp(df):
    df['ville'] = df['lieu'].apply(lambda x: x.split(' ')[0])
    df['code_postal'] = df['lieu'].apply(lambda x: x.split(' ')[1])
    df.drop('lieu', axis=1)
    return df


def round_(df, col, rd=2):
    df[col] = np.round(df[col], rd)
    return df


def transform_sl_prix(df):
    df['prix'] = df['prix'].apply(lambda x: int(str(x)[:-4]) * 1000)
    return df


def transform_sl_surface(df):
    df['surface'] = df['surface'].apply(lambda x: int(x.split(',')[0]) if pd.isnull(x) == False else np.NaN)
    return df


def select_columns(df, col_list):
    df_select = df[col_list]
    return df_select


def rename_column(df, col1, col2):
    df = df.rename(columns={col1: col2})
    return df


################################################################################
################################################################################

def clean_lbc(df):
    df_lbc_annonce = (df.pipe(transform_string_col_into_dict, 'critere')
                      .pipe(concat_criteres)
                      .pipe(transform_prix)
                      .pipe(transform_surface)
                      .pipe(calculate_m2)
                      .pipe(round_, col='prix_m2')
                      .pipe(get_dept)
                      .pipe(parse_date)
                      .drop('critere', axis=1)
                      .assign(origine='lbc')
                      )

    lbc_small = df_lbc_annonce.pipe(select_columns,
                                    ['prix', 'surface', 'prix_m2', 'ville', 'code_postal', 'origine', 'dept', 'id_annonce'])
    return lbc_small


def clean_sl(df):
    df_sl = (df.pipe(transform_sl_prix)
             .pipe(transform_sl_surface)
             .pipe(calculate_m2)
             .pipe(fullfill_cp)
             .pipe(get_dept)
             #  .pipe(rename_column, 'annonce', 'id_')
             .assign(origine='sl')
             )
    sl_small = df_sl.pipe(select_columns,
                          ['prix', 'surface', 'prix_m2', 'ville', 'code_postal', 'origine', 'dept', 'id_annonce'])
    return sl_small


def clean_pv(df):
    df_pv = (df.pipe(transform_prix)
             .pipe(transform_to_numeric, col='nb_pict')
             .pipe(transform_to_numeric, col='surface')
             .pipe(calculate_m2)
             .pipe(get_dept)
             #  .pipe(rename_column, 'annonce', 'id_')
             .assign(origine='pv')
             )
    pv_small = df_pv.pipe(select_columns,
                          ['prix', 'surface', 'prix_m2', 'ville', 'code_postal', 'origine', 'dept', 'id_annonce'])
    return pv_small


################################################################################
################################################################################


if __name__ == "__main__":

    # Load config file
    config = f.read_json(f.CONFIG_PATH)
    db = db_connection.ImmoDB(config['database'])


    # Configuration information
    SOURCES = config['general']['scraping_list']
    # FOLDER_TMP = config['general']['tmp_folder_path']
    # TITLE_CLEAN_TMP = config['processing']['clean_data_filename']


    column_dict = {
        'LBC':['id', 'date_scrap', 'id_annonce', 'url_annonce', 'titre', 'prix', 'date_annonce', 'auteur', 'ville', 'code_postal', 'is_msg', 'is_num', 'categorie', 'critere', 'nb_pict', 'descr', 'processed'],
        'PV':['id', 'date_scrap', 'id_annonce', 'url_annonce', 'titre', 'prix', 'surface', 'date_annonce', 'auteur', 'ville', 'code_postal', 'nb_pieces', 'nb_pict', 'agence', 'processed']
        }


    # list for new references dataFrames
    df_list = []

    # Appending each datasource
    for source in SOURCES:
        print(' >>>>>>', source)
        # clean_csv_name = FOLDER_TMP + 'new_{}.csv'.format(source)
        clean_function = clean_lbc if source == 'LBC' else clean_pv if source == 'PV' else clean_sl
        table_name = f.get_raw_tablename(source)
        sql_request = 'SELECT * FROM {} WHERE processed=0'.format(table_name)
        # print(sql_request)
        df_raw = db.sql_to_df(sql_request) #, with_col=True, index='id')
        df_raw.columns = column_dict[source]
        df_raw = df_raw.set_index('id')
        print(df_raw.head())
        print(df_raw.columns)
        # print(df_raw['critere'].head())
        print(df_raw.iloc[0])
        df_clean = df_raw.pipe(clean_function)
        df_list.append(df_clean)

    # Make aggregation
    df_agg = pd.concat(df_list).dropna()
    # print(df_agg.head())

    # # Save new data
    # path = FOLDER_TMP + TITLE_CLEAN_TMP
    # df_agg.to_csv(path, header=True, index=False)
    db.execute_sql_insert(df_agg, 'tmp_cleaned')

    db.update_sql('UPDATE raw_lbc SET processed=1 WHERE processed=0')
    db.update_sql('UPDATE raw_pv SET processed=1 WHERE processed=0')
    

