TO DO : 
- Check the entire workflow and update readme
- Add dashboard for data viz 
- Create the configuration file for data sotrage
- Create the script that initiate the storage structure

NB : after a restructuration all that follow might not be exact. Corrections will come soon.
________________

# Introduction

The tool was created to follow the house selling market in France.

__Main idea__
- Select some french cities and a price range
- Get data from the web about these cities
- Store all the announces in a database, once a day or as often as wanted
- Make ananlysis among announces or historical data (see dvf french open data)
- Send alerts when specific announces matching creteria are found

__Automation & Running__

The automation of these scripts might be orchestrated thanks to the `pipeline.py` script or (the way i use it) throug an `AWS` cron task on `EC2` (insert link). 

The frequence is to be congifured. Given the speed for selling houses, deefault scrapping and alerting is once a day. It could be more often, but regarding the market this is very reasonable and basically, the number of requests is not high which can prevent from being banned by scrapped websites.

As the project has been developped in `jupyter notebook`, these one are availables in the main folder. There are two possibilities to run them successively, that you might find in the  `pipeline.ipynb`.

- Transforming the notebooks in python files and executing them. Printed lines will appear in the console.
- Run the five notebooks. Notebooks with results will then be stored the `executed_notebook` folder. Each time this command is executed, previous saved notebooks will be deleted.


# 1. Script interaction : main pipeline

 All scripts are written in `python`, the first part of this documentation describe their interactions.

The whole process takes 5 scripts to run. Each of them has a specific role, takes the output of the previous one and prepare data for the following one.

## A. realize_scrapping.ipynb

- Execute spiders
- Copy data locally
- Check if IDs already in final_process.csv
- Keep only new
- Save tmp_files with new data in new_tmp_data fodler

## B. clean_and_concatenate.ipynb
- read files
- clean separately
- create a common dataFrame
- save results in new_clean_data.csv
 
## C. process_data.ipynb
- realize all sort of processing
- save results in new_processed_data.csv

## D. make_analysis_of_new.ipynb
- compare with standard DVF data
- save alert_file
- save results in processed_data/history/date.csv
- add restults to general_processed_data
- save new list_ids in processed_data (per spider)
    
## E. alerting_script.ipynb
- read_alert
- send_it
- save_hour of alert
    
# 2. Project structure

## A. Global architecture

## B. stored data

Here is the arborescence of stored data (folder data) (add link)

- alert_files : contains all the preformated alerts, resulting of all the analysis scripts. They are composed of json files with essentially two keys : `message` to post and `channel` where to post it.

Note that it can be regenerated (creating empty folders) at every moment by running the script `clean_database.py` available un the `utils` folder.



# 3. The configuration file


# 4. Other scripts    
Autres scripts pouvant intervenir :
- EDA : se plug sur processed data
- Weekly : alert

# 5. External tools

## A. Scrapping 

Use of python `scrapy` librarie.
The spiders and paramters are not part of the project.
We have 3 of them which scrap the main french websites =
- Leboncoin
- ParuVendu
- SeLoger

They all work the same way, calling the following command :

`scrapy crawl my_spider -a url="www.url_to_scrap.com" -a max_page=number_of_pages_to_scrap`

As you can see, there are three parameters :
- `Spider name` : defined inside the scrapy object.
- `Url` to scrap : Need to be set, else default values correspond to random request.
- `Number of pages` to scrap : No values will scrap until the end of the website and might be long.

## B. Alerting

Use of as python `slacker` librarie to send messages to a specific slack channel.
To use it, shou yould get a slack_token (here), create a channel on the application and then set them in the configuration file `config.json` available in the `utils`folder.



Project implementation : 

- In a folder we store all the alert files that we want to sent

## C. DVF data



# Conclusion