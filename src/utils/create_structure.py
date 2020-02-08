import os
import datetime


def create_folders(origin, folder_name):

    # Create main folder
    os.mkdir(origin + folder_name)

    # Create folders for processing use
    os.mkdir(origin + folder_name + '/tmp')
    os.mkdir(origin + folder_name + '/dvf')
    os.mkdir(origin + folder_name + '/time')
    os.mkdir(origin + folder_name + '/alerts')

    # Create folders for raw and processed scrapped data
    os.mkdir(origin + folder_name + '/raw')
    os.mkdir(origin + folder_name + '/raw/history')
    os.mkdir(origin + folder_name + '/processed')
    os.mkdir(origin + folder_name + '/processed/history')


def add_time_information(origin, folder_name):

    # Store creation time
    ts = str(datetime.datetime.now())[:19]

    # Select time folder
    path = origin + folder_name + '/time/'

    # Add current time as last_analyse timestamp
    with open(path + 'last_analyse.txt', 'w') as file:
        file.write(ts)

    # Add current time as last_alert timestamp
    with open(path + 'last_alert.txt', 'w') as file:
        file.write(ts)


if __name__ == "__main__":

    origin = '../../'
    folder_name = 'data'

    create_folders(origin, folder_name)
    add_time_information(origin, folder_name)
