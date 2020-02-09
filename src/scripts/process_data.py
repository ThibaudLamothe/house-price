# Python libraries
import os
import pandas as pd

# Personal functions
import functions as f
import database_connection as db_connection


################################################################################
################################################################################


def paris_decompo(df):
    df['ville'] = df[['ville', 'code_postal']].apply(paris_decompo, axis=1)
    return df


def viager(df):
    # Dealing with viager
    df['viager'] = df['contenu'].apply(lambda x: True if 'viager' in x else False)

    # Display result
    nb_viager = df['viager'].sum()
    print('Nombre d\'appartement en viager', nb_viager)

    return df


################################################################################
################################################################################


def processing(df):
    # return (df.pipe(paris_decompo)
    #         .pipe(viager)
    #         )
    return df

def add_to_already_processed(df, path):
    print(df.head())


    if os.path.exists(path):
        df_old = pd.read_csv(path)
        print(df_old.head())
        df_old['new'] = 0
        df['new'] = 1


        df_old = pd.concat([df, df_old], sort=False)
        print(df_old.head())
        df_old.to_csv(path)
    else:
        df['new'] = 1
        df.to_csv(path, header=True, index=False)


if __name__ == "__main__":

    # Load config file
    config = f.read_json(f.CONFIG_PATH)
    now = f.get_now()
    db = db_connection.ImmoDB(config['database'])

    # Configuration
    # FOLDER_TMP = config['general']['tmp_folder_path']
    # FOLDER_HISTORY = config['processing']['folder_history']
    # FOLDER_PROCESSED = config['processing']['processed_folder_path']
    # TITLE_READ_TMP = config['processing']['clean_data_filename']
    # TITLE_SAVE_TMP = config['processing']['process_data_filename']
    # TITLE_SAVE_HISTORY = config['processing']['history_process_filename']
    # TITLE_PROCESSED = config['processing']['processed_all_data_filename']

    # Loading
    # path = FOLDER_TMP + TITLE_READ_TMP
    # df = pd.read_csv(path)

    table_name = 'tmp_cleaned'
    sql_request = 'SELECT * FROM {}'.format(table_name)
    df = db.sql_to_df(sql_request, with_col=True, index='id')


    # Processing
    process = False
    if process: df = df.pipe(processing)


    df['alert'] = 0
    db.execute_sql_insert(df, 'processed_annonce')
    db.delete_sql(table='tmp_cleaned')


    # # Saving results into tmp
    # path = FOLDER_TMP + TITLE_SAVE_TMP
    # df.to_csv(path, header=True, index=False)

    # # Saving results into history
    # path = FOLDER_HISTORY + TITLE_SAVE_HISTORY.format(now)
    # df.to_csv(path, header=True, index=False)

    # # Add to processed data
    # path = FOLDER_PROCESSED + TITLE_PROCESSED
    # df.pipe(add_to_already_processed, path)
