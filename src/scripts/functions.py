import os
import json
import datetime
import pandas as pd
import numpy as np


CONFIG_PATH = '../config/config.json'
DATA_PATH = '../../data/'
DATA_PATH_TIME = '../../data/time/'


def read_json(json_path):
    with open(json_path) as json_file:
        data = json.load(json_file)
    return data


def read_jl_file(file_name):
    values = []
    with open(file_name, 'rb') as file:
        line = '---'
        while len(line) > 1:
            line = file.readline()
            values.append(line)
    values = values[:-1]
    values = [json.loads(i) for i in values]
    df = pd.DataFrame(values)
    return df


def save_ts_alert():
#     # TODO : que se passe t il si le fichier n'existe pas ?
#     # TODO : Penser peut etre à en mettre un par défaut au moment de la création de la structure du folder.
    ts = str(datetime.datetime.now())[:19]
    path = DATA_PATH_TIME + 'last_alert.txt'
    with open(path, 'w') as file:
        file.write(ts)


def load_ts_alert():
    path = DATA_PATH_TIME + 'last_alert.txt'
    with open(path, 'r') as file:
        ts = file.read()
    ts = pd.to_datetime(ts)
    return ts


def get_url(df, line=0, all=False, col=None):
    if all:
        for i in range(df.shape[0]):
            url = df.url.iloc[i]
            if col is not None:
                li = [df[ma_col].iloc[i] for ma_col in col]
                print(url, li)
            else:
                print(url)
    else:
        url = df.url.iloc[line]
        if col is not None:
            li = [df[ma_col].iloc[line] for ma_col in col]
            print(url, li)
        else:
            print(url)


def get_now(original=False):
    now = str(datetime.datetime.now())[:19]
    if original:
        return now
    return now.replace(' ', '_').replace(':', '-')


def load_csv(path, name='DF'):
    if os.path.isfile(path):
        df = pd.read_csv(path)
        print('Shape of {} : {}'.format(name, df.shape))
        return df
    print('No data at : \n{}'.format(path))
    return pd.DataFrame()


def fake_old_new(df_new):
    half = np.int(df_new.shape[0] / 2)
    print(half)
    df_old = df_new.iloc[:half]
    df_new = df_new.iloc[half:]
    return df_old, df_new


def get_raw_tablename(project_name):
      table_name = 'raw_{}'.format(project_name.lower())
      return table_name