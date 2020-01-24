import os
import json
import datetime
import pandas as pd

from slacker import Slacker
import credentials as c

def create_slack():
    slack_api_token = c.slack_api_token
    slack = Slacker(slack_api_token)
    return slack

def save_ts_analyse():
    ts = str(datetime.datetime.now())[:19]
    with open('data/last_analyse.txt', 'w') as f:
        f.write(ts)

def load_ts_analyse():
    with open('data/last_analyse.txt', 'r') as f:
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

    path = 'data/alert_files/'
    list_alerts = os.listdir(path)
    date_alerts = [i[6:-5] for i in list_alerts if 'alert' in i]

    new_alerts = [alert for alert in date_alerts if alert > last_analyse]
    new_files = ['alert_{}.json'.format(file) for file in new_alerts]
    return new_files


if __name__ == "__main__":
    slack = create_slack()

    if False:
        msg_return = slack.chat.post_message(channel='test_channel',
                                             text='Test blablabla',
                                             username='Alert',
                                             icon_emoji=':female-firefighter:')

    path = 'data/alert_files/'
    for file in get_new_alert_files():
        print('> Dealing with file : {}'.format(file))
        with open(path + file) as json_file:
            alert = json.load(json_file)
        message, channel, emoji = parse_alert(alert)
        print(channel, emoji, len(message))

        msg_return = slack.chat.post_message(channel=channel,
                                             text=message,
                                             username='Alert',
                                             icon_emoji=emoji)

        # Savin execution date for next analysis
        if msg_return.successful:
            print('> Everything OK : message sent to slack')
            save_ts_analyse()
        else:
            print('> An error occured while sending slack message')
