# Python libraries
import pandas as pd

# Personal functions
import functions as f
import database_connection as db_connection

import realize_scraping
import clean_and_concatenate
import process_data
import make_analysis_of_new
import alerting

if __name__ == "__main__":

    ###################
    # 0. Config
    ###################
    
    # Load config file
    config = f.read_json(f.CONFIG_PATH)
    db = db_connection.ImmoDB(config['database'])
    now = f.get_now()

    # Extract necessary information from config
    SOURCES = config['general']['scraping_list']
    FOLDER_LOCAL_DATA = config['general']['data_path']
    FOLDER_SCRAPING_CORNER = config['general']['scraping_corner_path']
    FOLDER_ALERT = config['general']['alert_data_path']
    CRITERE_PATH = config['general']['critere_path']
    SLACK_CHANNEL = config['alerting']['channel']
    SLACK_TOKEN = config['alerting']['slack_token']
    
    column_dict = {
        'LBC':['id', 'date_scrap', 'id_annonce', 'url_annonce', 'titre', 'prix', 'date_annonce', 'auteur', 'ville', 'code_postal', 'is_msg', 'is_num', 'categorie', 'critere', 'nb_pict', 'descr', 'processed'],
        'PV':['id', 'date_scrap', 'id_annonce', 'url_annonce', 'titre', 'prix', 'surface', 'date_annonce', 'auteur', 'ville', 'code_postal', 'nb_pieces', 'nb_pict', 'agence', 'processed']
        }

    ###################
    # 1. Scrapping
    ###################
    
    # Realize scrapings
    for project in SOURCES:
        realize_scraping.manage(project_name=project,
               spider_name='spider{}'.format(project),
               scraping_corner_folder=FOLDER_SCRAPING_CORNER,
               local_data_folder=FOLDER_LOCAL_DATA,
               db=db,
               scrapping=False
               )

    ###################
    # 2. Cleaning
    ###################

    # list for new references dataFrames
    df_list = []

    # Appending each datasource
    for source in SOURCES:
        
        # Select tratment to apply
        clean_function = clean_and_concatenate.clean_lbc if source == 'LBC' else clean_and_concatenate.clean_pv if source == 'PV' else clean_and_concatenate.clean_sl
        
        # Preapre data from database
        table_name = f.get_raw_tablename(source)
        sql_request = 'SELECT * FROM {} WHERE processed=0'.format(table_name)
        df_raw = db.sql_to_df(sql_request) #, with_col=True, index='id')
        print('SOURCE', source)
        print(df_raw)
        if df_raw.shape[0]>0:
            df_raw.columns = column_dict[source]
            # df_raw = pd.DataFrame(columns=column_dict[source])
            df_raw = df_raw.set_index('id')
            df_clean = df_raw.pipe(clean_function)
            df_list.append(df_clean)

    # Make aggregation
    if len(df_list) >0:

        # Aggregate data
        df_agg = pd.concat(df_list).dropna()
    
        # Save new data into database
        db.execute_sql_insert(df_agg, 'tmp_cleaned')

    # Precise into raw database that these data has been processed
    db.update_sql('UPDATE raw_lbc SET processed=1 WHERE processed=0')
    db.update_sql('UPDATE raw_pv SET processed=1 WHERE processed=0')

    ###################
    # 3. Processing
    ###################

    # Loading
    table_name = 'tmp_cleaned'
    sql_request = 'SELECT * FROM {}'.format(table_name)
    df = db.sql_to_df(sql_request, with_col=True, index='id') # TODO : precise column names to avoid switches

    # Processing
    process = False
    if process: df = df.pipe(process_data.processing)

    # Saving results in database
    df['alert'] = 0
    db.execute_sql_insert(df, 'processed_annonce')
    
    # Cleaning tmp database
    db.delete_sql(table='tmp_cleaned')

    ###################
    # 4. Analysing
    ###################

    # Loading content
    criteres = f.read_json(CRITERE_PATH)

    # Create alerts
    make_analysis_of_new.manage_alerts(criteres, SLACK_CHANNEL, db=db, real_split=False)

    ###################
    # 5. Alerting
    ###################

    # Create slack instance
    slack = alerting.create_slack(SLACK_TOKEN)
    last_analyse = f.load_ts_alert() # TODO : use information from database instead of local
  
    # Extract necessary alert files
    df_alerts = alerting.load_alerts(db, last_analyse)

    for alert in df_alerts.iterrows():
        # Displayon something

        # Get alert informations    
        message = alert[1].loc['alerte']
        channel = alert[1].loc['channel_name']
        emoji = alert[1].loc['emoji']
        
        # Send slack message
        msg_return = slack.chat.post_message(channel=channel,
                                             text=message,
                                             username='Alert',
                                             icon_emoji=emoji)

        # Saving execution date for next analysis
        if msg_return.successful:
            print('> Everything OK : message sent to slack')
            f.save_ts_alert() # TODO : use information from database instead of local
        else:
            print('> An error occured while sending slack message')