# Python libraries
import os
from slacker import Slacker

# Personal functions
import functions as f
import database_connection as db_connection


################################################################################
################################################################################


def create_slack(slack_api_token):
    slack = Slacker(slack_api_token)
    return slack

# def parse_alert(alert):
#     message = alert['message']
#     channel = alert['channel']
#     emoji = alert['emoji']
#     return message, channel, emoji


def get_new_alert_files(alert_path, last_analyse):

    # Transform analyse timestamp
    last_analyse = str(last_analyse)[:19].replace(' ', '_').replace(':', '-')

    # Get all the alerts
    list_alerts = os.listdir(alert_path)

    # Extract the time they were produced
    date_alerts = [i[6:-5] for i in list_alerts if 'alert' in i]

    # Get only new files
    new_alerts = [alert for alert in date_alerts if alert > last_analyse]
    new_files = ['alert_{}.json'.format(file) for file in new_alerts]

    return new_files



def get_alerts(db, last_analyse):

    # Transform analyse timestamp
    last_analyse = str(last_analyse)[:19] #.replace(':', '-')

    table_name = 'alerts'
    sql_request = 'SELECT * FROM {} WHERE date_alerte > \'{}\''.format(table_name, last_analyse)
    df = db.sql_to_df(sql_request) #, with_col=True, index='id')
    df.columns = ['id', 'id_user', 'emoji', 'channel_name', 'date_alerte', 'alerte']
    df = df.set_index('id')

    print(' >>>>>>>>>>>>>>>>>')
    print(df.head())
    print(df.shape)
    print(' >>>>>>>>>>>>>>>>>')

    return df

    # # Get only new files
    # new_alerts = [alert for alert in date_alerts if alert > last_analyse]
    # new_files = ['alert_{}.json'.format(file) for file in new_alerts]

    # return new_files


if __name__ == "__main__":

    # Read config file
    config = f.read_json(f.CONFIG_PATH)
    db = db_connection.ImmoDB(config['database'])

    # Get configuration information
    SLACK_TOKEN = config['alerting']['slack_token']
    FOLDER_ALERT = config['general']['alert_data_path']

    # Create slack instance
    # slack = create_slack(SLACK_TOKEN)
    slack = Slacker(SLACK_TOKEN)
    last_analyse = f.load_ts_alert()

    # # Extract necessary alert files
    # alert_files = get_new_alert_files(FOLDER_ALERT, last_analyse)

    # for file in alert_files:
    #     print('> Dealing with file : {}'.format(file))

    #     # Load file
    #     path = FOLDER_ALERT + file
    #     alert = f.read_json(path)

    #     # Parse file
    #     message, channel, emoji = parse_alert(alert)

    #     # Send slack message
    #     msg_return = slack.chat.post_message(channel=channel,
    #                                          text=message,
    #                                          username='Alert',
    #                                          icon_emoji=emoji)

    #     # Saving execution date for next analysis
    #     if msg_return.successful:
    #         print('> Everything OK : message sent to slack')
    #         f.save_ts_alert()
    #     else:
    #         print('> An error occured while sending slack message')



  # Extract necessary alert files
    df_alerts = get_alerts(db, last_analyse)

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
            f.save_ts_alert()
        else:
            print('> An error occured while sending slack message')

get_alerts
