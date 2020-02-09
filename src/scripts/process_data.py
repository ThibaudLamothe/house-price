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


################################################################################
################################################################################



if __name__ == "__main__":

    # Load config file
    config = f.read_json(f.CONFIG_PATH)
    now = f.get_now()
    db = db_connection.ImmoDB(config['database'])

    # Loading
    table_name = 'tmp_cleaned'
    sql_request = 'SELECT * FROM {}'.format(table_name)
    df = db.sql_to_df(sql_request, with_col=True, index='id') # TODO : precise column names to avoid switches

    # Processing
    process = False
    if process: df = df.pipe(processing)

    # Saving results in database
    df['alert'] = 0
    db.execute_sql_insert(df, 'processed_annonce')
    
    # Cleaning tmp database
    db.delete_sql(table='tmp_cleaned')