import os
import json
import numpy as np
import pandas as pd
import functions as f


############################################################################
############################################################################
DATA_PATH = '../../data/'


def load_new():
    path = DATA_PATH + 'tmp/new_process_data.csv'
    if os.path.isfile(path):
        return pd.read_csv(path)
    print('No new processed data at : \n{}'.format(path))
    return pd.DataFrame()


def load_old():
    path = DATA_PATH + 'processed/process_data.csv'
    if os.path.isfile(path):
        return pd.read_csv(path)
    print('No old processed data at : \n{}'.format(path))
    return pd.DataFrame()


def load_dvf():
    return None


def save_alert(message, channel="test_channel"):
    alert = {"channel": channel,
             "message": message,
             "emoji": ":female-firefighter:"}

    folder = DATA_PATH + 'alert_files/'
    now = f.get_now()
    path = '{}/alert_{}.json'.format(folder, now)
    with open(path, 'w') as outfile:
        json.dump(alert, outfile)


def save_process_data_history():
    return None


def concat_process_with_previous(new=2):
    return new


def save_ids():
    return None

############################################################################
############################################################################

if __name__ == "__main__":

    # Loading information of when last analyse was realized
    last_analyse = f.load_ts_analyse()
    # last_analyse = pd.Timestamp('20190425')
    print('Last analyse realised at {}'.format(last_analyse))

    df_new = load_new()
    df_old = load_old()
    df_dvf = load_dvf()

    print('NEW : ', df_new.shape)
    print('OLD : ', df_old.shape)

    half = np.int(df_new.shape[0] / 2)
    print(half)
    df_old = df_new.iloc[:half]
    df_new = df_new.iloc[half:]

    # Deletint viager data for further analyse
    # df = df[df['viager']==False]
    # del(df['viager'])

    # Looking at cities in both dataFrame and their intersection
    ville_in_new = df_new['ville'].unique().tolist()
    ville_in_old = df_old['ville'].unique().tolist()
    ville_new = [ville for ville in ville_in_new if ville not in ville_in_old]
    ville_inter = [ville for ville in ville_in_new if ville in ville_in_old]
    ville_in_new.sort()
    ville_in_old.sort()
    ville_new.sort()
    ville_inter.sort()
    if False:
        print('Ville in new : {}'.format(ville_in_new))
        print('Ville in old : {}'.format(ville_in_old))
        print('Ville in both : {}'.format(ville_inter))
        print('Ville new : {}'.format(ville_new))

    # Calculating information
    df_old['prix'] = np.float(df_old['prix'])
    df_old['surface'] = np.float(df_old['surface'])
    df_old['prix_m2'] = np.float(df_old['prix_m2'])
    old_mean = df_old.groupby('ville').mean()[['prix', 'surface', 'prix_m2']].applymap(lambda x: np.round(x, 2))
    # old_d1 = df_old.groupby('ville').quantile(q=0.1)[['prix', 'surface', 'prix_m2']].applymap(lambda x: np.round(x, 2))
    old_d1 = df_old.groupby('ville').median()[['prix', 'surface', 'prix_m2']].applymap(lambda x: np.round(x, 2))

    # Computing columuns for comparison
    df_new['moy_ville'] = df_new['ville'].apply(lambda x: old_mean.loc[x, 'prix_m2'] if x in ville_inter else -1)
    df_new['inf_moy_ville'] = df_new['prix_m2'] < df_new['moy_ville']

    df_new['d1_ville'] = df_new['ville'].apply(
        lambda x: old_d1.loc[x, 'prix_m2'] if x in ville_inter else -1)  # Ville nouvelle => Moy à 0
    df_new['inf_d1_ville'] = df_new['prix_m2'] < df_new['d1_ville']

    df_new['pct_mieux_m2'] = ((df_new['prix_m2'] - df_new['moy_ville']).div(df_new['moy_ville']) * 100).apply(np.round)

    df_new.loc[df_new['moy_ville'] < 0, ['inf_moy_ville', 'inf_d1_ville']] = False
    df_new.loc[df_new['moy_ville'] < 0, ['d1_ville', 'pct_mieux_m2']] = np.NaN

    # Selectiong onlu lines where price is in decil one
    df_inf_d1 = df_new[df_new.inf_d1_ville]

    if len(df_inf_d1) == 0:
        print('> No new line : nothing to update.')
    else:
        # Computing the message for slack
        message = ''
        for key, value in df_inf_d1.ville.value_counts().sort_index().to_dict().items():
            message += '*{}*:{} annonces décile 1.\n'.format(key, value)

        default_url = 'www.google.fr'
        for i in df_inf_d1.sort_values(by='ville').iterrows():
            line = i[1]
            ville = line['ville']
            # url = line['url']
            url = default_url
            prix = line['prix']
            surface = line['surface']
            moy_ville = line['moy_ville']
            prix_m2 = line['prix_m2']
            pct_mieux_m2 = line['pct_mieux_m2']
            ville_url = '\n<{}|{}>'.format(url, ville)
            message += '\n{} : {} m², {} €, \n[€/m² *{}* vs {} : {}%]'.format(ville_url, surface, prix, prix_m2,
                                                                              moy_ville, pct_mieux_m2)

    save_alert(message)
    save_process_data_history()
    concat_process_with_previous()
    save_ids()
