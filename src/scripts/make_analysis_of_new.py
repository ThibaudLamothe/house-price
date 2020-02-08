# Python libraries
import json
import numpy as np

# Personal functions
import functions as f

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


def save_alert(message, alert_path, channel="immo_scrap"):

    # Create the alert object for slack library
    alert = {"channel": channel,
             "message": message,
             "emoji": ":female-firefighter:"}

    # Save it in the alerts folder
    path = alert_path + 'alert_{}.json'.format(f.get_now())
    with open(path, 'w') as outfile:
        json.dump(alert, outfile)


############################################################################
############################################################################

def manage_alerts(processed_path, criteres, alert_path, channel, real=True):

    # Loading different data sources
    df = f.load_csv(processed_path)

    # Make separation of the dataset into old and new to conduct analyses
    if real:
        df_new = df[df['new'] == 1]
        df_old = df[df['new'] == 0]
    else:
        df_old, df_new = f.fake_old_new(df)

    # Get city repartition
    repartition = get_ville_repartition(df_old, df_new)

    # Parsing request
    for critere_name, critere_description in criteres.items():
        print(' > Critere :', critere_name)

        # Creating an alert dataFrame given request configuration file
        df_alert = get_important_new_lines(df_old, df_new, repartition)

        # Transform the dataFrame into a message for Slack
        alert = prepare_alerts(df_alert)

        #
        if len(alert)==0:
            print('Pas d\'alerte à annoncer')

            # TODO : remove that line and add the save_alert in an else statement
            alert ='Ceci est la traduction d\'une alerte vide'
        save_alert(alert, alert_path, channel)


############################################################################
############################################################################

if __name__ == "__main__":

    # Load config file
    config = f.read_json(f.CONFIG_PATH)
    now = f.get_now()

    # Configuration
    FOLDER_PROCESSED = config['processing']['processed_folder_path']
    TITLE_PROCESSED = config['processing']['processed_all_data_filename']
    CRITERE_PATH = config['general']['critere_path']
    ALERT_PATH = config['general']['alert_data_path']
    SLACK_CHANNEL = config['alerting']['channel']

    # Loading content
    criteres = f.read_json(CRITERE_PATH)

    # Create alerts
    processed_path = FOLDER_PROCESSED + TITLE_PROCESSED
    manage_alerts(processed_path, criteres, ALERT_PATH, SLACK_CHANNEL, real=True)

