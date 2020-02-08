import pandas as pd
import functions as f

def paris_decompo(df):
    df['ville'] = df[['ville', 'code_postal']].apply(paris_decompo, axis=1)
    return df


def viager(df, last_analyse):
    # Dealing with viager
    df['viager'] = df['contenu'].apply(lambda x: True if 'viager' in x else False)
    df_viager = df[df['viager']]
    df_viager_new = df_viager[df_viager.index > last_analyse]
    print('Nombre d\'appartement en viager au total', df_viager.shape[0])
    print('Nombre d\'appartement en viager nouveau', df_viager_new.shape[0])
    return df


if __name__ == "__main__":

    # Configuration
    FOLDER_TMP = '../../data/tmp'
    TITLE_READ_TMP = 'new_clean_data.csv'
    TITLE_SAVE_TMP = 'new_process_data.csv'

    FOLDER_HISTORY = '../../data/processed/history'

    now = f.get_now()
    TITLE_SAVE_HISTORY = 'process_data_{}.csv'.format(now)


    # Loading
    path = '{}/{}'.format(FOLDER_TMP, TITLE_READ_TMP)
    df = pd.read_csv(path)

    # Processing
    processing = False
    if processing:
        df = (df.pipe(paris_decompo)
              .pipe(viager)
              )

    # Saving results into tmp
    path = '{}/{}'.format(FOLDER_TMP, TITLE_SAVE_TMP)
    df.to_csv(path, header=True, index=False)

    # Saving results into history
    path = '{}/{}'.format(FOLDER_HISTORY, TITLE_SAVE_HISTORY)
    df.to_csv(path, header=True, index=False)