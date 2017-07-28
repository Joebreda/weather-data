# weather-data
builds a data-frame from scrapped data from various web addresses
test.py, the main for scraping, contains a method that first takes in the excel sheet that was manually filled with eGauge names and 
their respective longitudes and latitudes and forms a reference csv file for the egauges and their nearest airport's call IDs
There are then 3 methods used for scraping, one of which that is used for testing that scrapes specific days one at a time. 
There is another called "scrape" that scrapes every hour of every date for a specific airport and places the data into a csv file. 
The final method, "scrape_all" is used to cycle through all the airports in the reference csv file and call scrape on each airport only 
once. The full_list is necesary in running this file and is generated using the egauge solar data scraping file egauge_data.py (it tells 
the scrape method how far to go back in time for each airport. Without it the airports would be scraped all the way back to their first 
every recorded date.)

Once this is done the files can be processed using full_list_cleaner.py and data_cleaning.py which are both used for rounding timestamps, 
changing the Weather Underground given values to numeric values, removing units and creating more specific and organized csv files for
analysis of specific metrics. 
