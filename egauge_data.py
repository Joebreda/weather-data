#!/usr/bin/python
import urllib2
import csv
import pandas as pd
import pytz
import time
import datetime
import os.path
import httplib

user_input = 1498867200


'''
HOW TO RUN CODE PROPERLY 
specify the user_input as whatever start time you want. 
Run the script. If a file is already finished, it should skip it. if it is interrupted mid construction of a file, you
just have to restart and it will append it.

If the script stops before it is done, you just gotta start it again. however, the full_list file will be weird. I'm
trying to fix that now.

- upload new version of files to server***
'''

df = pd.read_csv('eGauges-nearest-airport.csv', sep=',')        # Loads df
homes_set = df['eGauge'].tolist()                               # to loop over homes
timezones = df['Airport timezone'].tolist()                     # TS
# egauge_id = []                                                  # keeps track of egauges
# earliest_record = []
# egauge_dict = {}                                                # Dictionary for full list
# counter = 0                                                     # TEST
epoch0 = pytz.utc.localize(datetime.datetime(1970, 1, 1))       # TS

for home_ in range(len(homes_set)):                             # begin loop over homes
    # counter += 1                                                # TEST
    # print counter                                               # TEST
    home_id_ = str(homes_set[home_])                            # utility
    # egauge_id.append(home_id_)                                  # Checks home as looped over
# --------------------------------------------------Webpage reader------------------------------------------------------
    # start_ = 1322697600                                       # Test for 1 hour Initial start time
    # start_ = 1322514000                                       # Test for 1 day
    # start_ = 1327708800                                         # Test for 30 days
    start_ = user_input
    end_ = start_                                               # Looping purposes
    reter = ['initial', 'junk']                                 # Initial condition for loop
    while len(reter) > 1:                                       # make sure this is a proper end condition
        timestamps_for_home = []                                # New list every month
        local_time_for_home = []                                # New list every month
        egauge_id_repeated = []                                 # New list every month
        gen_kwh_for_home = []                                   # New list every month
        egauge_dict = {}                                        # Dictionary for full list

        if os.path.isfile(home_id_ + '-data.csv'):              # if it already exists determine new start time
            previously_scraped = pd.read_csv(home_id_ + '-data.csv', sep=',')
            previous_timestamps = previously_scraped['Timestamp'].tolist()
            start_ = previous_timestamps[-1]                    # Finds the last entry to exist.
            end_ = start_                                       # Looping purposes
        # end_ = end_ - 3600                                    # Test 1 hour
        # end_ = end_ - 86400                                   # Test 1 days
        # start_ = start_ - 86400                               # part of the test for 1 day
        end_ = end_ - 2592000                                   # Test 30 days : specifically when the if is here ^^
        try:
            passto = 'http://' + home_id_ + '.egaug.es/cgi-bin/egauge-show?&c&C&h&f='+str(start_)+'&t='+str(end_) + '&Z=US/Eastern'
            # print passto                                        # TEST
            response = urllib2.urlopen(passto)                  # saves response from call
        except urllib2.HTTPError, e:
            # earliest_record.append('HTTPError')
            egauge_dict[home_id_] = 'HTTPError'
            continue
        except urllib2.URLError, e:
            # earliest_record.append('URLError')
            egauge_dict[home_id_] = 'URLError'
            continue
        except Exception, e:
            # earliest_record.append('Exception')
            egauge_dict[home_id_] = 'Exception'
            continue
        # --------------------------------------------------Webpage reader----------------------------------------------
        cr = csv.reader(response)
        reter = []
        try:
            for row in cr:
                reter.append(row)                               # Builds more understandable format
        except httplib.IncompleteRead, e:                       # I don't think this exception actually does anything
            # earliest_record.append('IncompleteRead')
            egauge_dict[home_id_] = 'IncompleteRead'
            continue
        # print len(reter), reter                                 # TEST
        for lists in range(1, len(reter)):                      # from start of data to end
            # -----------------------------------Time conversion nonsense-----------------------------------------------
            if len(reter[lists][0]) <= 16:                      # BECAUSE LOCALTIME IS SOMETIME PROVIDED IN WEIRD FORMAT
                time1 = datetime.datetime.strptime(reter[lists][0], '%Y-%m-%d %H:%M')
            else:
                time1 = datetime.datetime.strptime(reter[lists][0], '%Y-%m-%d %H:%M:%S')
            local_timezone = pytz.timezone(timezones[home_])                        # timezone object for local
            #time2 = local_timezone.normalize(local_timezone.localize(time1))        # This line doesnt change timezones
            time3 = pytz.utc.normalize(pytz.utc.localize(time1))                    # time object for utc time
            time4 = time3.astimezone(local_timezone)                                # this line converts to local time
            unix_dt = int((time3 - epoch0).total_seconds())                         # timestamp in seconds
            # -----------------------------------------APPEND LISTS-----------------------------------------------------
            timestamps_for_home.append(unix_dt)
            local_time_for_home.append(time4)
            egauge_id_repeated.append(home_id_)
            gen_kwh_for_home.append(reter[lists][2])

        # -------------------------------------------------Puts data in form--------------------------------------------

        home_stamp = pd.DataFrame(timestamps_for_home, columns=['Timestamp'])       # This bit turns lists into columns
        egauge_id_col = pd.DataFrame(egauge_id_repeated, columns=['egauge'])        # and joins then into 1 df
        local_col = pd.DataFrame(local_time_for_home, columns=['Local Time'])
        gen_col = pd.DataFrame(gen_kwh_for_home, columns=['Solar [kw]'])
        home_data = pd.concat([home_stamp, egauge_id_col, local_col, gen_col], join='outer', axis=1)
        if os.path.isfile(home_id_ + '-data.csv'):                                  # but if the df file already exists
            # previously_scraped = pd.read_csv(home_id_ + '-data.csv', sep=',')
            home_data = previously_scraped.append(home_data, ignore_index=True)     # load it and append new data
        home_data.to_csv(home_id_ + '-data.csv', sep=',', encoding='utf-8', index=False)    # either way save it back

        # time.sleep(6)                                           # this is so we don't get blacklisted again

    # earliest_record.append(unix_dt)                           # Checks earliest timestamp for home checked before loop
    egauge_dict[home_id_] = unix_dt                             #
    full_list = pd.DataFrame(egauge_dict.items(), columns=['Homes', 'Earliest Record'])
    if os.path.isfile('full_list.csv'):
        previous_full_list = pd.read_csv('full_list.csv', sep=',')
        full_list = previous_full_list.append(full_list, ignore_index=True)
    full_list.to_csv('full_list.csv', sep=',', encoding='utf-8', index=False)
    # time.sleep(60)                                              # Wait 60 seconds for next request to avoid blacklist
    # --------------------------------------------------Outside of loop-------------------------------------------------
# this is for the checklist .
'''
all_egauges = pd.DataFrame(egauge_id, columns=['eGauges'])
earliest = pd.DataFrame(earliest_record, columns=['Earliest Records'])
full_list = pd.concat([all_egauges, earliest], join='outer', axis=1)
full_list.to_csv('full_list.csv', sep=',', encoding='utf-8', index=False)
'''