# Python libraries
import pandas as pd

# Personal functions
import functions as f
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
    now = f.get_now()

    SOURCES = config['general']['scraping_list']
    SLACK_TOKEN = config['alerting']['slack_token']
    SLACK_CHANNEL = config['alerting']['channel']
    CRITERE_PATH = config['general']['critere_path']

    FOLDER_LOCAL_DATA = config['general']['data_path']
    FOLDER_SCRAPING_CORNER = config['general']['scraping_corner_path']
    FOLDER_TMP = config['general']['tmp_folder_path']
    FOLDER_HISTORY = config['processing']['folder_history']
    FOLDER_PROCESSED = config['processing']['processed_folder_path']
    FOLDER_ALERT = config['general']['alert_data_path']

    TITLE_CLEAN_TMP = config['processing']['clean_data_filename']
    TITLE_READ_TMP = config['processing']['clean_data_filename']
    TITLE_SAVE_TMP = config['processing']['process_data_filename']
    TITLE_SAVE_HISTORY = config['processing']['history_process_filename']
    TITLE_PROCESSED = config['processing']['processed_all_data_filename']


    ###################
    # 1. Scrapping
    ###################

    # Delete tmp data
    realize_scraping.clean_tmp_folder(FOLDER_TMP)

    # Realize scrapings
    for project in SOURCES:
        realize_scraping.manage(
            project_name=project,
            spider_name='spider{}'.format(project),
            scraping_corner_folder=FOLDER_SCRAPING_CORNER,
            local_data_folder=FOLDER_LOCAL_DATA
        )

    ###################
    # 2. Cleaning
    ###################

    # list for new references dataFrames
    df_list = []

    # Appending each datasource
    for source in SOURCES:
        clean_csv_name = FOLDER_TMP + 'new_{}.csv'.format(source)
        clean_function = clean_and_concatenate.clean_lbc if source == 'LBC' else clean_and_concatenate.clean_pv if source == 'PV' else clean_and_concatenate.clean_sl
        df_clean = clean_function(clean_csv_name)
        df_list.append(df_clean)

    # Make aggregation
    df_agg = pd.concat(df_list).dropna()

    # Save new data
    path = FOLDER_TMP + TITLE_CLEAN_TMP
    df_agg.to_csv(path, header=True, index=False)

    ###################
    # 3. Processing
    ###################

    # Loading
    path = FOLDER_TMP + TITLE_READ_TMP
    df = pd.read_csv(path)

    # Processing
    df = df.pipe(process_data.processing)

    # Saving results into tmp
    path = FOLDER_TMP + TITLE_SAVE_TMP
    df.to_csv(path, header=True, index=False)

    # Saving results into history
    path = FOLDER_HISTORY + TITLE_SAVE_HISTORY.format(now)
    df.to_csv(path, header=True, index=False)

    # Add to processed data
    path = FOLDER_PROCESSED + TITLE_PROCESSED
    df.pipe(process_data.add_to_already_processed, path)


    ###################
    # 4. Analysing
    ###################

    # Loading content
    criteres = f.read_json(CRITERE_PATH)

    # Create alerts
    processed_path = FOLDER_PROCESSED + TITLE_PROCESSED
    make_analysis_of_new.manage_alerts(processed_path, criteres, FOLDER_ALERT, SLACK_CHANNEL, real=True)

    ###################
    # 5. Alerting
    ###################

    # Create slack instance
    slack = alerting.create_slack(SLACK_TOKEN)
    last_analyse = f.load_ts_alert()

    # Extract necessary alert files
    alert_files = alerting.get_new_alert_files(FOLDER_ALERT, last_analyse)

    for file in alert_files:
        print('> Dealing with file : {}'.format(file))

        # Load file
        path = FOLDER_ALERT + file
        alert = f.read_json(path)

        # Parse file
        message, channel, emoji = alerting.parse_alert(alert)

        # Send slack message
        msg_return = slack.chat.post_message(channel=channel,
                                             text=message,
                                             username='Alert',
                                             icon_emoji=emoji)

        # Saving execution date for next analysis
        if msg_return.successful:
            print('> Everything OK : message sent to slack')
            f.save_ts_alert()
        else:
            print('> An error occured while sending slack message')

