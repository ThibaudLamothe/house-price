# Python libraries
import os
from shutil import copyfile

# Personal functions
import functions as f
import database_connection as db_connection


################################################################################
################################################################################

def filter_ids(df, col_name, list_id):
    """Used to filter ids
    - df:
    - col_name:
    - list_id:
    """
    return df.loc[df[col_name].isin(list_id)]


################################################################################
################################################################################

def clean_tmp_folder(tmp_folder):
    """ Delete files from the tmp folder as we're starting a new pipeline
    """

    # Check if tmp exists
    is_tmp = os.path.exists(tmp_folder)

    # If does not exist, create them
    if not is_tmp:
        os.mkdir(tmp_folder)

    # Get the list of files into tmp folder
    file_list = os.listdir(tmp_folder)

    # If any, we delete them all
    for file in file_list:
        path = tmp_folder + file
        os.remove(path)


def run_spider(spider_name, project_name, scraping_project_path, scrapy_data_path, file_name):
    """ Execute spider from scraping repository with all scrapping content
    - spider_name:
    - project_name:
    - scraping_project_path:
    - scrapy_data_path:
    - file_name:
    """
    print('RunSpider')

    # Prepare command to execute spider
    cmd0 = 'export SCRAPY_PROJECT={}'.format(project_name)
    cmd1 = 'cd "{}"'.format(scraping_project_path)
    cmd2 = 'scrapy crawl {} -o {}{}'.format(spider_name, scrapy_data_path, file_name)

    # Define project name
    os.environ['SCRAPY_PROJECT'] = project_name

    # Run spider
    request = "{} && {} && {}".format(cmd0, cmd1, cmd2)
    rep = os.system(request)

    # Display result
    if rep == 0:
        print("Spider {} successfully run".format(spider_name))
    else:
        print("Couldn't run spider {}".format(spider_name))


def get_immo_data(path_source, path_dest_history, path_dest_pipeline, project_name):
    """ Get the scrapped_data and store it into local folder
    - source:
    - source_link:
    - dest_fodler:
    - now:
    """
    print('GetImmo')

    # Save data for history
    copyfile(path_source, path_dest_history)

    # Save tmp data for processing pipeline
    copyfile(path_source, path_dest_pipeline)

    # Delete source file
    os.remove(path_source)

    print('> Files from {} saved.'.format(project_name))


def keep_only_new(scrapped_path, processed_path, project_name, db):
    """Read the data into the folder and prepare for saving
    - scrapped_path:
    - porcessed_path:
    - title:
    """
    print('ProcessData')

    # Read data
    df = f.read_jl_file(scrapped_path)
    new_ids = df['id_'].values

    # Getting old ids
    table_name = 'raw_{}'.format(project_name.lower())
    old_ids = db.get_ids(table_name=table_name)
    old_ids = set(old_ids)

    # # Check if there are already processed data
    # is_processed = os.path.isfile(processed_path)

    # # If there are
    # if is_processed:
    #     print('> is_processed')

    # Get new ids
    new_ids = [id_ for id_ in new_ids if id_ not in old_ids]

    # Keep only new IDs
    df = filter_ids(df, 'id_', new_ids)

    return df


# def save_data_csv(df, tmp_file_path, project_name):
#     """ Saving data into local folder
#     - df:
#     - tmp_folder:
#     - saving_name:
#     - title:
#     """

#     print('> Selection ok.')
#     df.to_csv(tmp_file_path, header=True, index=False)
#     print('> New data {} saved.'.format(project_name))


def save_db(df, db, project_name):
    """ Dispatch to the correct raw saving table
    """

    # Depending on project some structuration has to be done so that scrapped data fit database
    renaming_dict = {
        'LBC':{'id_':'id_annonce', 'url':'url_annonce', 'description':'descr', 'date_absolue':'date_annonce'},
        'PV':{'id_':'id_annonce', 'url':'url_annonce', 'date_absolue':'date_annonce'}
    }

    # Make the modification on the dataset
    now = f.get_now(original=True)
    df = df.rename(columns=renaming_dict[project_name])
    df['date_scrap'] = now
    df['processed'] = 0

    # Execute the insertion
    nb_lines = df.shape[0]
    if nb_lines > 0:
        table_name = f.get_raw_tablename(project_name) #raw_{}'.format(project_name.lower())
        db.execute_sql_insert(df, table_name)
    
    # Display results
    print('> {} lines - new data {} saved.'.format(nb_lines, project_name))



################################################################################
################################################################################


def manage(project_name, spider_name, scraping_corner_folder, local_data_folder, db, scrapping=True, now=f.get_now()):
    """ Make data available from scrapping to raw data fodler
    - project_name:
    - spider_name:
    - scraping_corner_folder:
    - local_data_folder:
    - scrapping:
    - now:
    """

    # Compute folder variables
    scraping_project_folder = scraping_corner_folder + 'scrapy_project/'
    scraping_data_folder = scraping_corner_folder + 'scrapped_data/immo_scrap/'
    tmp_folder = local_data_folder + 'tmp/'

    # Compute filenames
    scrapped_filename = '{}_immo.jl'.format(project_name)
    saving_filename = 'new_{}.csv'.format(project_name)

    # Compute path
    processed_path = local_data_folder + 'processed/processed_data.csv'
    dest_history_path = local_data_folder + 'raw/history/raw_{}_{}.jl'.format(project_name, now)
    dest_pipeline_path = local_data_folder + 'raw/raw_{}.jl'.format(project_name)
    tmp_path = tmp_folder + '{}'.format(saving_filename)
    source_path = scraping_data_folder + '{}'.format(scrapped_filename)

    # Run spider and save data into scraping_corner scrapped_data folder
    if scrapping:
        run_spider(spider_name=spider_name,
                   project_name=project_name,
                   scraping_project_path=scraping_project_folder,
                   scrapy_data_path=scraping_data_folder,
                   file_name=scrapped_filename)

        # Get data from scrapping_corner to local folder raw_data
        get_immo_data(path_source=source_path,
                    path_dest_history=dest_history_path,
                    path_dest_pipeline=dest_pipeline_path,
                    project_name=project_name)

    # Process data fr
    df = keep_only_new(scrapped_path=dest_pipeline_path,
                       processed_path=processed_path,
                       project_name=project_name, 
                       db=db)
    # # Save data
    # save_data_csv(df=df,
    #           tmp_file_path=tmp_path,
    #           project_name=project_name)

    # Save data
    save_db(df=df,
            db=db,
            project_name=project_name)


################################################################################
################################################################################

if __name__ == "__main__":

    # Load config file
    config = f.read_json(f.CONFIG_PATH)
    db = db_connection.ImmoDB(config['database'])

    # Extract necessary information from config
    SOURCES = config['general']['scraping_list']
    FOLDER_LOCAL_DATA = config['general']['data_path']
    FOLDER_SCRAPING_CORNER = config['general']['scraping_corner_path']
    FOLDER_TMP = config['general']['tmp_folder_path']

    # Delete tmp data
    clean_tmp_folder(FOLDER_TMP)

    # Realize scrapings
    for project in SOURCES:
        manage(project_name=project,
               spider_name='spider{}'.format(project),
               scraping_corner_folder=FOLDER_SCRAPING_CORNER,
               local_data_folder=FOLDER_LOCAL_DATA,
               db=db,
               scrapping=True
               )
