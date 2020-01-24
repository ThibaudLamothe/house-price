import pandas as pd

if __name__ == "__main__":
    FOLDER = '../../data/new_tmp_data'
    TITLE = 'new_clean_data.csv'
    path = '{}/{}'.format(FOLDER, TITLE)
    df = pd.read_csv(path)
    df.head()

    if False:
        # Dealing with Paris
        df['ville'] = df[['ville', 'code_postal']].apply(paris_decompo, axis=1)

    if False:
        # Dealing with viager
        df['viager'] = df['contenu'].apply(lambda x: True if 'viager' in x else False)
        df_viager = df[df['viager']]
        df_viager_new = df_viager[df_viager.index > last_analyse]
        print('Nombre d\'appartement en viager au total', df_viager.shape[0])
        print('Nombre d\'appartement en viager nouveau', df_viager_new.shape[0])


    TITLE = 'new_process_data.csv'
    path = '{}/{}'.format(FOLDER, TITLE)
    df.to_csv(path, header=True, index=False)