import json
import pandas as pd
import numpy as np


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
    df['dept'] = df['code_postal'].apply(
        lambda x: np.int(x / 1000) if pd.isnull(x) == False else np.NaN)  # if numeric but not work for nan
    return df


def transform_string_col_into_dict(df, col='critere'):
    df[col] = df[col].apply(lambda x: json.loads(x.replace('\'', '"')))
    return df


def concat_criteres(df):
    list_id = []
    list_k = []
    list_v = []
    for i in df[['id_', 'critere']].values:
        for k, v in i[1].items():
            list_id.append(i[0])
            list_k.append(k)
            list_v.append(v)
    df_crit = pd.DataFrame({'id_': list_id, 'critere_name': list_k, 'critere_value': list_v})
    df_criteres = pd.pivot(df_crit.drop_duplicates(), index='id_', columns='critere_name', values='critere_value')
    df_merge = pd.merge(df, df_criteres, on='id_', how='left')
    return df_merge


def transform_surface(df):
    df['surface'] = df['Surface'].apply(lambda x: str(x).split(' ')[0])
    df['surface'] = pd.to_numeric(df['surface'], errors='coerce')
    df = df.drop('Surface', axis=1)
    return df


def parse_date(df, drop_=True, sep_date_heure=' ', name_col_date='date_absolue'):
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

def clean_lbc(lbc_file):
    df_lbc_annonce = (pd.read_csv(lbc_file)
                      .pipe(transform_string_col_into_dict, 'critere')
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
                                    ['prix', 'surface', 'prix_m2', 'ville', 'code_postal', 'origine', 'dept', 'id_'])
    return lbc_small


def clean_sl(sl_file):
    df_sl = (pd.read_csv(sl_file)
             .pipe(transform_sl_prix)
             .pipe(transform_sl_surface)
             .pipe(calculate_m2)
             .pipe(fullfill_cp)
             .pipe(get_dept)
             #  .pipe(rename_column, 'annonce', 'id_')
             .assign(origine='sl')
             )
    sl_small = df_sl.pipe(select_columns,
                          ['prix', 'surface', 'prix_m2', 'ville', 'code_postal', 'origine', 'dept', 'id_'])
    return sl_small


def clean_pv(pv_file):
    df_pv = (pd.read_csv(pv_file)
             .pipe(transform_prix)
             .pipe(transform_to_numeric, col='nb_pict')
             .pipe(transform_to_numeric, col='surface')
             .pipe(calculate_m2)
             .pipe(get_dept)
             #  .pipe(rename_column, 'annonce', 'id_')
             .assign(origine='pv')
             )
    pv_small = df_pv.pipe(select_columns,
                          ['prix', 'surface', 'prix_m2', 'ville', 'code_postal', 'origine', 'dept', 'id_'])
    return pv_small


################################################################################
################################################################################


if __name__ == "__main__":

    # Configuration information
    FOLDER = '../../data/new_tmp_data'

    # Create files path
    lbc_file = '{}/new_LBC.csv'.format(FOLDER)
    pv_file = '{}/new_PV.csv'.format(FOLDER)
    sl_file = '{}/new_SL.csv'.format(FOLDER)

    # Select sources
    SOURCE = ['LBC', 'PV', 'SL']
    SOURCE = ['LBC', 'PV']
    df_list = []

    # Prepare dataframes
    if 'LBC' in SOURCE:
        df_list.append(clean_lbc(lbc_file))
    if 'SL' in SOURCE:
        df_list.append(clean_sl(sl_file))
    if 'PV' in SOURCE:
        df_list.append(clean_pv(pv_file))

    # Make aggregation
    df_agg = (pd.concat(df_list).dropna())
    df_agg.head()

    # Save new data 
    TITLE = 'new_clean_data.csv'
    path = '{}/{}'.format(FOLDER, TITLE)
    df_agg.to_csv(path, header=True, index=False)
