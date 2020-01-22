import json
import pandas as pd
import datetime

def read_jl_file(file_name):
    values = []
    with open(file_name, 'rb') as file:
        line = '---'
        while len(line)>1:
            line = file.readline()
            values.append(line)
    values = values[:-1]
    values = [json.loads(i) for i in values]
    df = pd.DataFrame(values)
    return df


def save_ts_analyse():
    ts = str(datetime.datetime.now())[:19]
    with open('data/last_analyse.txt', 'w') as file:
        file.write(ts)
def load_ts_analyse():
    with open('data/last_analyse.txt', 'r') as file:
        ts = file.read()
    ts = pd.to_datetime(ts)
    return ts


def save_ts_alert():
    ts = str(datetime.datetime.now())[:19]
    with open('data/last_alert.txt', 'w') as file:
        file.write(ts)
def load_ts_alert():
    with open('data/last_alert.txt', 'r') as file:
        ts = file.read()
    ts = pd.to_datetime(ts)
    return ts




def get_url(df, line=0, all=False,col=None):
    if all:
        for i in range(df.shape[0]):
            url = df.url.iloc[i]
            if col is not None:
                li = [df[ma_col].iloc[i] for ma_col in col]
                print(url,li )
            else:
                print(url)
    else:
        url = df.url.iloc[line]
        if col is not None:
            li = [df[ma_col].iloc[line] for ma_col in col]
            print(url,li )
        else:
            print(url)
            
            
def get_now():
    return str(datetime.datetime.now())[:19].replace(' ','_').replace(':','-')