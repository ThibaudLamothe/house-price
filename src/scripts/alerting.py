import os
import datetime
import pandas as pd
from slacker import Slacker

import functions as f


def create_slack(slack_api_token):
    slack = Slacker(slack_api_token)
    return slack

def save_ts_analyse():
    ts = str(datetime.datetime.now())[:19]
    with open('data/time/last_analyse.txt', 'w') as f:
        f.write(ts)

def load_ts_analyse():
    with open('data/time/last_analyse.txt', 'r') as f:
        ts = f.read()
    ts = pd.to_datetime(ts)
    return ts

def parse_alert(alert):
    message = alert['message']
    channel = alert['channel']
    emoji = alert['emoji']
    return message, channel, emoji

def get_new_alert_files():
    last_analyse = str(load_ts_analyse())[:19].replace(' ', '_').replace(':', '-')

    path = 'data/alerts/'
    list_alerts = os.listdir(path)
    date_alerts = [i[6:-5] for i in list_alerts if 'alert' in i]

    new_alerts = [alert for alert in date_alerts if alert > last_analyse]
    new_files = ['alert_{}.json'.format(file) for file in new_alerts]
    return new_files


if __name__ == "__main__":

    # Read config file
    config = f.read_json(f.CONFIG_PATH)

    # Extract slack token from config
    token = config['general']['slack_token']

    # Create slack instance
    slack = create_slack(token)

    if True:
        msg = "Bonjour Yann LeCun"
        msg_return = slack.chat.post_message(channel='immo_scrap',
                                             text=msg,
                                             username='Alert',
                                             icon_emoji=':female-firefighter:')

    # path = 'data/alerts/'
    #
    # for file in get_new_alert_files():
    #     print('> Dealing with file : {}'.format(file))
    #     with open(path + file) as json_file:
    #         alert = json.load(json_file)
    #     message, channel, emoji = parse_alert(alert)
    #     print(channel, emoji, len(message))
    #
    #     msg_return = slack.chat.post_message(channel=channel,
    #                                          text=message,
    #                                          username='Alert',
    #                                          icon_emoji=emoji)
    #
    #     # Savin execution date for next analysis
    #     if msg_return.successful:
    #         print('> Everything OK : message sent to slack')
    #         save_ts_analyse()
    #     else:
    #         print('> An error occured while sending slack message')
