import os
import time
import datetime
import sys

sys.path.append(os.path.abspath("04_slack"))
import slack_alert as s
slack = s.create_slack()


c = 0
while True:
    c+=1   

    # Send a message on slack
    print('> Send message to Slack')
    ms = slack.chat.post_message(channel='test_channel',
                                text='<!everyone> {} *Running Spider!!*'.format(c),
                                username='Alert',
                                icon_emoji=':female-firefighter:')
    if ms.successful:
        print('Message sent')
    else:
        print('> Error happened with Slack')


    # Run spider and save data into database
    print('> Run Spider')
    os.system('cd 01_scrapping & python launch_spider.py')

    print('> Execute analysis')
    os.system('cd 02_analysis & python new_line_analysis.py')
    
    
    # Going for daily report
    duree_min = 20
    heure = datetime.datetime(2019, 4, 28, 23, 18, 33)
    heure = datetime.datetime.now()
    if (heure.hour) == 23 and (heure.minute <= duree_min):
        os.system('cd 02_analysis & python daily_report.py')
        
        # And weekly report
        today = datetime.datetime(2019, 4, 28).weekday() # fake sunday
        today = datetime.datetime.today().weekday()
        if today == 6:#Sunday is 6
            os.system('cd 02_analysis & python weekly_report.py')

    # Displaying information about next execution
    heure_str = str(heure)[:19]
    print('Sleeping now..')
    print('From  {} and for {} min'.format(heure_str, duree_min))

    # Starting sleep time
    time.sleep(duree_min*60)

