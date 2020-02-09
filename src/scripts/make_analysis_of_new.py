# Python libraries
import json
import numpy as np
import pandas as pd

# Personal functions
import functions as f
import database_connection as db_connection

pd.set_option('chained_assignment',None)

############################################################################
############################################################################

def delete_viager(df):
    df = df[df['viager'] == False]
    del (df['viager'])
    return df


# Looking at cities in both dataFrame and their intersection
def get_ville_repartition(df_old, df_new):
    ville_in_new = df_new['ville'].unique().tolist()
    ville_in_old = df_old['ville'].unique().tolist()
    ville_new = [ville for ville in ville_in_new if ville not in ville_in_old]
    ville_inter = [ville for ville in ville_in_new if ville in ville_in_old]
    ville_old = [ville for ville in ville_in_old if ville not in ville_in_new]

    ville_in_new.sort()
    ville_in_old.sort()
    ville_new.sort()
    ville_old.sort()
    ville_inter.sort()

    return {
        'in_new': ville_in_new,
        'in_old': ville_in_old,
        'old': ville_old,
        'new': ville_new,
        'inter': ville_inter
    }


def get_important_new_lines(df_old, df_new, repartition):
    # Calculating information

    for col in ['prix', 'surface', 'prix_m2']:
        df_old[col] = pd.to_numeric(df_old[col], errors='coerce')
        df_new[col] = pd.to_numeric(df_new[col], errors='coerce')
    
    old_mean = df_old.groupby('ville').mean()[['prix', 'surface', 'prix_m2']].applymap(lambda x: np.round(x, 2))
    # old_d1 = df_old.groupby('ville').quantile(q=0.1)[['prix', 'surface', 'prix_m2']].applymap(lambda x: np.round(x, 2))
    old_d1 = df_old.groupby('ville').median()[['prix', 'surface', 'prix_m2']].applymap(lambda x: np.round(x, 2))

    # Computing columuns for comparison
    df_new['moy_ville'] = df_new['ville'].apply(
        lambda x: old_mean.loc[x, 'prix_m2'] if x in repartition['inter'] else -1)
    df_new['inf_moy_ville'] = df_new['prix_m2'] < df_new['moy_ville']

    df_new['d1_ville'] = df_new['ville'].apply(
        lambda x: old_d1.loc[x, 'prix_m2'] if x in repartition['inter'] else -1)  # Ville nouvelle => Moy à 0
    df_new['inf_d1_ville'] = df_new['prix_m2'] < df_new['d1_ville']

    df_new['pct_mieux_m2'] = ((df_new['prix_m2'] - df_new['moy_ville']).div(df_new['moy_ville']) * 100).apply(
        np.round)

    df_new.loc[df_new['moy_ville'] < 0, ['inf_moy_ville', 'inf_d1_ville']] = False
    df_new.loc[df_new['moy_ville'] < 0, ['d1_ville', 'pct_mieux_m2']] = np.NaN

    # Selectiong onlu lines where price is in decil one
    df_inf_d1 = df_new[df_new.inf_d1_ville]
    return df_inf_d1


################################################################################
################################################################################


def prepare_alerts(df):
    message = ''
    for key, value in df['ville'].value_counts().sort_index().to_dict().items():
        message += '*{}*:{} annonces décile 1.\n'.format(key, value)

    default_url = 'www.google.fr'
    for i in df.sort_values(by='ville').iterrows():
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
    return message


def save_alert_db(message, db, channel="immo_scrap"):

    emoji = ':female-firefighter:'
    id_user = 1
    now = f.get_now(original=True)

    content = {
        'id_user':[id_user],
        'emoji':[emoji],
        'channel_name':[channel],
        'date_alerte':[now],
        'alerte':message
        }
    df = pd.DataFrame(content)
    db.execute_sql_insert(df, 'alerts')

############################################################################
############################################################################

# def manage_alerts(processed_path, criteres, alert_path, channel, db, real_split=True):
def manage_alerts(criteres, channel, db, real_split=True):


    # Loading different data sources
    table_name = 'processed_annonce'
    sql_request = 'SELECT * FROM {}'.format(table_name)
    df = db.sql_to_df(sql_request, with_col=True, index='id') # TODO : precise column names to avoid random switches


    # Make separation of the dataset into old and new to conduct analyses
    if real_split:
        df_new = df[df['new'] == 1]
        df_old = df[df['new'] == 0]
    else:
        df_old, df_new = f.fake_old_new(df)

    # Get city repartition
    repartition = get_ville_repartition(df_old, df_new)

    # Parsing request
    # TODO : use real criteria from database
    for critere_name, critere_description in criteres.items():
        
        # Disply information about critere
        print(' > Critere :', critere_name)

        # Creating an alert dataFrame given request configuration file
        df_alert = get_important_new_lines(df_old, df_new, repartition)

        # Transform the dataFrame into a message for Slack
        alert = prepare_alerts(df_alert)

        # If no new alerts
        if len(alert)==0:
            print('Pas d\'alerte à annoncer')

            # TODO : remove that line and add the save_alert in an else statement
            alert ='Ceci est la traduction d\'une alerte vide'
        
        # Save alerts into database
        save_alert_db(alert, db, channel)


############################################################################
############################################################################

if __name__ == "__main__":

    # Load config file
    config = f.read_json(f.CONFIG_PATH)
    now = f.get_now()
    db = db_connection.ImmoDB(config['database'])

    # Configuration
    CRITERE_PATH = config['general']['critere_path']
    SLACK_CHANNEL = config['alerting']['channel']

    # Loading content
    criteres = f.read_json(CRITERE_PATH)

    # Create alerts
    manage_alerts(criteres, SLACK_CHANNEL, db=db, real_split=False)

