import os
import functions as f
from shutil import copyfile


################################################################################
################################################################################

def filter_ids(df, col_name, list_id):
    """Used to filter ids
    - df:
    - col_name:
    - list_id:
    """
    return df.loc[df[col_name].isin(list_id)]


def get_ids(spider_name):
    """Used to get older ids
    - spider_name:
    """
    return [1, 2, 3]


################################################################################
################################################################################

def run_spider(spider_name, project_name, scraping_project_path, scrapy_data_path, file_name):
    """ Execute spider from scraping repository with all scrapping content
    - scraping_corner_path :
    - scrapy_data_path :
    - spider_name :
    - project_name :
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


def process_data(scrapped_path, processed_path, title):
    """Read the data into the folder and prepare for saving
    - scrapped_path:
    - porcessed_path:
    - title:
    """
    print('ProcessData')
    # Read data
    df = f.read_jl_file(scrapped_path)
    new_ids = df['id_'].values

    # Check if new
    is_processed = os.path.isfile(processed_path)

    # If new
    if is_processed:
        print('> is_processed')
        # Get old ids
        old_ids = get_ids(title)  # functions to write
        new_ids = [id_ for id_ in new_ids if id_ not in old_ids]
        df = filter_ids(df, 'id_', new_ids)

    return df


def save_data(df, tmp_file_path, project_name):
    """ Saving data into local folder
    - df:
    - tmp_folder:
    - saving_name:
    - title:
    """
    print('> Selection ok.')
    df.to_csv(tmp_file_path, header=True, index=False)
    print('> New data {} saved.'.format(project_name))


################################################################################
################################################################################


def manage(project_name, spider_name, scraping_corner_folder, local_data_folder, scrapping=True, now=f.get_now()):
    # Compute folder variables
    scraping_project_folder = scraping_corner_folder + 'scrapy_project/'
    scraping_data_folder = scraping_corner_folder + 'scrapped_data/immo_scrap/'
    tmp_folder = local_data_folder + 'new_tmp_data/'

    # Compute filenames
    scrapped_filename = '{}_immo.jl'.format(project_name)
    saving_filename = 'new_{}.csv'.format(project_name)

    # Compute path
    processed_path = local_data_folder + 'processed_data/processed_data.csv'
    dest_history_path = local_data_folder + 'raw_data/history/{}_{}.jl'.format(project_name, now)
    dest_pipeline_path = local_data_folder + 'raw_data/raw_{}.jl'.format(project_name)
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
    df = process_data(scrapped_path=dest_pipeline_path,
                      processed_path=processed_path,
                      title=project_name)
    # Save data
    save_data(df=df,
              tmp_file_path=tmp_path,
              project_name=project_name)


################################################################################
################################################################################

if __name__ == "__main__":

    # Define import paths
    local_data_folder = '../../data/'
    scraping_corner_folder = '/Users/thibaud/Documents/Python_scripts/02_Projects/scraping_corner/'

    # Realize scrapings
    for project in ['LBC', 'PV']:  # 'SL'
        manage(project_name=project,
               spider_name='spider{}'.format(project),
               scraping_corner_folder=scraping_corner_folder,
               local_data_folder=local_data_folder)
