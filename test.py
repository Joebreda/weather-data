import requests
from bs4 import BeautifulSoup
import pandas as pd
from geopy.distance import great_circle
import xlrd
import datetime
import timezonefinder
import pytz
import os.path
import calendar
import time
# ---------------------------------------------------- WEB SCRAPER -----------------------------------------------------


'''
                                                   PROJECT DESCRIPTION
                                                   Instructions & Features:
                                                              
                                                   
- procedurally constructs a database of weather metrics for all airports which were found using longitudes and latitudes
 which were found using egauges. This list of airports syncs with the list of earliest recorded solar dates from the 
 full_list.csv file output from collecting solar data to record just the right amount of weather metrics for each egauge
- Many egauges have common nearest airports and so Scrape_All method handles this by checking if a specific airport
has already been scrapped (in this specific run)
- safety catch is used in loop using counter to distinguish between airports that have no data and airports that only
have missing data. (100 days without data in a row: moves on to next airport)

- fetch_single_date_data method used only for testing
- find_nearest_airports method makes airport list from manually generated egauge lust

                                                    KNOWN ISSUES
- If a csv file already exists for a given airport it will append that csv file with either new or duplicate data.
I am working on a database cleaning script that will automatically remove all duplicates and will trim units .
- A few airports found using the find_nearest_airport method are not on weather underground and thus collect no data.
    As a result I need to find some way to determine which airports do not have any data faster
    I also need to find a way to move on to the next nearest airport if possible. 
'''


def fetch_single_date_data(airport_code, year, month, day, timezone):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0'}
    url = "https://www.wunderground.com/history/airport/%s/%s/%s/%s/DailyHistory.html" % (airport_code, year, month,
                                                                                          day)
    req = requests.get(url, headers=headers)
    soup = BeautifulSoup(req.content, "lxml")
    metrics = []
    # columns
    UTC = []
    hour = []
    temp = []
    dew_point = []
    humidity = []
    pressure = []
    visibility = []
    wind_direction = []
    wind_speed = []
    gust_speed = []
    percip = []
    events = []
    conditions = []
    test_local_time = []
    timezones = []
    unix_time = []
    # columns
    closer_look = soup.find_all("tr", {"class": "no-metars"})
    for stuff in closer_look:
        metrics.append(stuff.text + ',')
    for hours in range(len(metrics)):
        temp_list = []
        metrics[hours] = metrics[hours].split('\n')
        for data in metrics[hours]:
            if len(data) >= 1:
                temp_list.append(data)
        hour.append(str(month) + '/' + str(day) + '/' + str(year) + ' ' + temp_list[0])

        # ------------------------------------------ L O C A L I Z E  --------------------------------------------------

        naive_time = datetime.datetime.strptime(str(month) + '/' + str(day) + '/' + str(year) + ' ' + temp_list[0],
                                              '%m/%d/%Y %I:%M %p')
        epoch0 = pytz.utc.localize(datetime.datetime(1970, 1, 1))
        local_timezone = pytz.timezone(timezone)
        local_dt = local_timezone.normalize(local_timezone.localize(naive_time))  # , ???is_dst=True
        utc_dt = local_dt.astimezone(pytz.utc)
        unix_dt = float((utc_dt - epoch0).total_seconds())
        #unix_dt = float(utc_dt.strftime('%s'))  # can print nonUTC times as seconds
        #unix_dt = time.localtime(unix_dt) #sets dst to 1
        unix_dt = time.gmtime(unix_dt)
        #print unix_dt
        unix_dt = calendar.timegm(unix_dt)  # converts to timestamp
        UTC.append(utc_dt)
        unix_time.append(unix_dt)

        # ------------------------------------------ L O C A L I Z E  --------------------------------------------------

        temp.append(temp_list[1])
        dew_point.append(temp_list[2])
        humidity.append(temp_list[3])
        pressure.append(temp_list[4])
        visibility.append(temp_list[5])
        wind_direction.append(temp_list[6])
        wind_speed.append(temp_list[7])
        gust_speed.append(temp_list[8])
        percip.append(temp_list[9])
        events.append(temp_list[10])
        conditions.append(temp_list[11])
        test_local_time.append(local_dt)
        timezones.append(local_timezone)
    unix_column = pd.DataFrame(unix_time, columns=['timestamp'])
    UTC_column = pd.DataFrame(UTC, columns=['UTC'])
    local_time_column = pd.DataFrame(test_local_time, columns=['Local Time'])
    timezone_column = pd.DataFrame(timezones, columns=['Timezone'])
    time_column = pd.DataFrame(hour, columns=['WU time'])
    temp_column = pd.DataFrame(temp, columns=['Temp'])
    dew_column = pd.DataFrame(dew_point, columns=['Dew Point'])
    humidity_column = pd.DataFrame(humidity, columns=['Humidity'])
    pressure_column = pd.DataFrame(pressure, columns=['Pressure'])
    visibility_column = pd.DataFrame(visibility, columns=['Visibility'])
    wind_direction_column = pd.DataFrame(wind_direction, columns=['Wind Direction'])
    wind_speed_column = pd.DataFrame(wind_speed, columns=['Wind Speed'])
    gust_speed_column = pd.DataFrame(gust_speed, columns=['Gust Speed'])
    precipitation_column = pd.DataFrame(percip, columns=['Precipitation'])
    events_column = pd.DataFrame(events, columns=['Events'])
    conditions_column = pd.DataFrame(conditions, columns=['Conditions'])
    hourly_dataframe = pd.concat([unix_column, UTC_column, local_time_column, time_column, timezone_column,
                                  temp_column, dew_column, humidity_column,
                                  pressure_column, visibility_column, wind_direction_column, wind_speed_column,
                                  gust_speed_column,precipitation_column, events_column, conditions_column],
                                 join='outer', axis=1)
    hourly_dataframe.to_csv(airport_code + '-NEW.csv', sep=',', encoding='utf-8')
    return hourly_dataframe


# ------------------------------------------- AUTOMATED HTML SHIFTER ---------------------------------------------------
# ------------------------------------ utilizing web scraper and data frame --------------------------------------------


def scrape(egauge, airport_code, timezone, start_year):
    # ----------------------------------- setting end times for specific egauges ---------------------------------------
    # only works if fill_list.csv is the same size as egauge_nearest_airports.csv
    full_list = pd.read_csv('full_list.csv', sep=',')
    homes = full_list['Homes'].tolist()
    records = full_list['Earliest Record'].tolist()
    # maybe add an if statement to handle the missing links and or delete them from the list.
    index_number = homes.index(egauge)
    end_date_timestamp = records[index_number]
    end_date_str = datetime.date.fromtimestamp(int(end_date_timestamp)).strftime('%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()
    # --------------------------------- Pulling the data from respective addresses -------------------------------------
    start_date = datetime.date(start_year, 7, 1)    # datetime.date(2016, 12, 31)   2016,5,4, 12, 31
    date = start_date
    # end_date = datetime.date((start_year-1), 12, 31)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0'}
    tdelta = datetime.timedelta(days=365)   # TEST should be days=1 Defined change variable for datetime object
    url = "https://www.wunderground.com/history/airport/%s/%s/%s/%s/DailyHistory.html" % (airport_code, date.year,
                                                                                          date.month, date.day)
    req = requests.get(url, headers=headers)                    # requests download HTML specified above | header disgus
    soup = BeautifulSoup(req.content, "lxml")                   # organizes HTML
    # columns
    UTC = []
    hour = []
    timezones = []
    temp = []
    dew_point = []
    humidity = []
    pressure = []
    visibility = []
    wind_direction = []
    wind_speed = []
    gust_speed = []
    percip = []
    events = []
    conditions = []
    test_local_time = []
    unix_time = []
    # columns
    # counter = 0  # this is here as a safety catch so we can scrape even after missing days
    while soup.find_all("tr", {"class": "no-metars"}):  # loops until HTML page empty for a full month
        # or counter <= 100
        # print airport_code + ' on ' + str(date.month) + '/' + str(date.day) + '/' + str(date.year) + " exists"  # TEST
        # -----------------------------------------------Loop body------------------------------------------------------
        metrics = []                                          # I originally had it form a list of hours and metrics
        # counter = 0                                           # resets counter if loop finds a table to scrape
        closer_look = soup.find_all("tr", {"class": "no-metars"})   # Forms the list of all hours
        if len(closer_look) <= 1:    # this condition makes the loop restart for missing
            # TEST print "Date has no table"                               # dates
            # counter += 1
            date = date - tdelta  # Changes date to 1 day in the past
            if date <= end_date:
                break
            url = "https://www.wunderground.com/history/airport/%s/%s/%s/%s/DailyHistory.html" % (
                airport_code, date.year, date.month, date.day)  # repeats process of requests but for new date
            req = requests.get(url, headers=headers)
            soup = BeautifulSoup(req.content, "lxml")
            continue
        for stuff in closer_look:
            metrics.append(stuff.text + ',')  # forms a list of Strings of metrics for each hour entry in a day
        for hours in range(
                len(metrics)):  # ORIGINALLY had metrics[0] as len but i think this needs to be hours in entire day
            temp_list = []  # temporary list that resets I think for every hour
            metrics[hours] = metrics[hours].split('\n')  # splits each hour of string data into a list with empty slots
            for data in metrics[hours]:  # for all those entries including empty slots
                if len(data) >= 1:  # if the slot isnt empty
                    temp_list.append(data)  # append it to this temporary list that resets every day
                if len(temp_list) < 12:
                    continue
            hour.append(str(date.month) + '/' + str(date.day) + '/' + str(date.year) + ' ' + temp_list[0])
            # ------------------------------------- This portion strictly for UTC column -------------------------------
            naive_time = datetime.datetime.strptime(str(date.month) + '/' + str(date.day) + '/' + str(date.year) + ' '
                                                    + temp_list[0], '%m/%d/%Y %I:%M %p')
            epoch0 = pytz.utc.localize(datetime.datetime(1970, 1, 1))
            local_timezone = pytz.timezone(timezone)
            local_dt = local_timezone.normalize(local_timezone.localize(naive_time))  # , is_dst=True
            utc_dt = local_dt.astimezone(pytz.utc)
            unix_dt = float((utc_dt - epoch0).total_seconds())
            unix_dt = time.gmtime(unix_dt)  # sets dst to 0
            unix_dt = calendar.timegm(unix_dt)  # converts to timestamp
            # UTC.append(utc_dt)
            # timezones.append(timezone)
            # test_local_time.append(local_dt)
            unix_time.append(unix_dt)
            # ------------------------------------- This portion strictly for UTC column -------------------------------
            temp.append(temp_list[1])
            if len(temp_list) >= 14:
                dew_point.append(temp_list[3])
                humidity.append(temp_list[4])  # appends to all the column categories
                pressure.append(temp_list[5])
                visibility.append(temp_list[6])
                wind_direction.append(temp_list[7])
                wind_speed.append(temp_list[8])
                gust_speed.append(temp_list[9])
                percip.append(temp_list[10])
                events.append(temp_list[11])
                conditions.append(temp_list[12])
            else:
                dew_point.append(temp_list[2])
                humidity.append(temp_list[3])  # appends to all the column categories
                pressure.append(temp_list[4])
                visibility.append(temp_list[5])
                wind_direction.append(temp_list[6])
                wind_speed.append(temp_list[7])
                gust_speed.append(temp_list[8])
                percip.append(temp_list[9])
                events.append(temp_list[10])
                conditions.append(temp_list[11])
        # print "SCRAPED" + '\n'                              # used to view procedurally what is happening
        # ------------------------------------------Looping conditions--------------------------------------------------
        date = date - tdelta                                    # Changes date to 1 day in the past
        if date <= end_date:
            break
        url = "https://www.wunderground.com/history/airport/%s/%s/%s/%s/DailyHistory.html" % (
            airport_code, date.year, date.month, date.day)      # repeats process of requests but for new date
        req = requests.get(url, headers=headers)
        soup = BeautifulSoup(req.content, "lxml")
    # -------------------------------- OUTSIDE OF LOOP: Forms new Data base from these lists ---------------------------
    unix_time_column = pd.DataFrame(unix_time, columns=['Timestamp'])
    # UTC_column = pd.DataFrame(UTC, columns=['UTC'])
    time_column = pd.DataFrame(hour, columns=['WU Local time'])
    # test_local_time_column = pd.DataFrame(test_local_time, columns=['TZ aware local time'])
    # timezone_column = pd.DataFrame(timezones, columns=['timezone'])
    temp_column = pd.DataFrame(temp, columns=['Temp'])
    dew_column = pd.DataFrame(dew_point, columns=['Dew Point'])
    humidity_column = pd.DataFrame(humidity, columns=['Humidity'])
    pressure_column = pd.DataFrame(pressure, columns=['Pressure'])
    visibility_column = pd.DataFrame(visibility, columns=['Visibility'])
    wind_direction_column = pd.DataFrame(wind_direction, columns=['Wind Direction'])
    wind_speed_column = pd.DataFrame(wind_speed, columns=['Wind Speed'])
    gust_speed_column = pd.DataFrame(gust_speed, columns=['Gust Speed'])
    precipitation_column = pd.DataFrame(percip, columns=['Precipitation'])
    events_column = pd.DataFrame(events, columns=['Events'])
    conditions_column = pd.DataFrame(conditions, columns=['Conditions'])
    hourly_dataframe_full = pd.concat([unix_time_column, time_column, temp_column, dew_column,
                                       humidity_column, pressure_column, visibility_column, wind_direction_column,
                                       wind_speed_column, gust_speed_column, precipitation_column, events_column,
                                       conditions_column], join='outer', axis=1)
    # UTC_column, test_local_time_column, timezone_column

    # ----------------------------------------- A P P E N D E R  O R  N O T --------------------------------------------

    if os.path.isfile(airport_code + '-WeatherHistory.csv'):        # + str(start_year+1)
        previously_scraped = pd.read_csv(airport_code + '-WeatherHistory.csv', sep=',')
        hourly_dataframe_full = previously_scraped.append(hourly_dataframe_full, ignore_index=True)
        # hourly_dataframe_full = pd.concat(previously_scraped, hourly_dataframe_full)
    hourly_dataframe_full.to_csv(airport_code + '-WeatherHistory.csv', sep=',', encoding='utf-8', index=False)
    return hourly_dataframe_full

# -------------------------------- FINDS NEAREST AIRPORT TO GIVEN EGAUGES FROM LIST ------------------------------------


def find_nearest_airports(egauge_data):
    # -------------------------------------------Airport database creation----------------------------------------------
    df = pd.read_csv('airport_data.csv')                        # Reads in CSV File
    del df['USAF']                                              # Cleans up space
    del df['WBAN']                                              # Cleans up space
    df = df.dropna(subset=['ICAO', 'LAT', 'LON'], how='any')    # Drops all rows that have missing calls, long or lat
    longitudes = df['LON'].tolist()                             # These 3 lists are used for iteration
    latitudes = df['LAT'].tolist()                              # forms lists for 3 variables with same indices
    calls = df['ICAO'].tolist()
    # ------------------------------------------- Forming lists from excel columns -------------------------------------
    minimums = []                                               # lists that will be used for creating new database
    airports_by_index = []
    egauge_names = []
    egauge_latitudes = []
    egauge_longitudes = []
    timezones = []                                              # list of timezones to be used as a column
    tzf = timezonefinder.TimezoneFinder()                       # creates instance of timezonefinder
    workbook = xlrd.open_workbook(egauge_data)                  # Opens Excel sheet specified in parameters of method
    sheet = workbook.sheet_by_index(0)                          # selects Sheet number 1
    for rows in range(1, sheet.nrows):                          # loops over rows of the sheet excluding initial row
        egauge_names.append(sheet.cell_value(rows, 0))          # forms a list using column 1
        egauge_latitudes.append(sheet.cell_value(rows, 1))      # forms a list using column 2
        egauge_longitudes.append(sheet.cell_value(rows, 2))     # forms a list using column 3
    # --------------------------------- Tests excel columns vs long and lats of airports--------------------------------
    for gauges in range(len(egauge_names)):                     # loops over length of all 3 lists
        min_dist = 1000000000000                                # initially large value: greater than any real distance
        airport_ID = ''                                         # initializing empty string at the start of every loop
        for ports in range(len(longitudes)):                    # inner loop that compares distances to airports
            if great_circle((egauge_latitudes[gauges], egauge_longitudes[gauges]), (latitudes[ports],
                                                                                longitudes[ports])).miles < min_dist\
                    and calls[ports] != 'KDNR' \
                    and calls[ports] != 'NEJ' \
                    and calls[ports] != 'C96' \
                    and calls[ports] != 'KSEW' \
                    and calls[ports] != 'LRY' \
                    and calls[ports] != 'K1PW' \
                    and calls[ports] != 'PHIK' \
                    and calls[ports] != 'KNXX':
                min_dist = great_circle((egauge_latitudes[gauges], egauge_longitudes[gauges]), (latitudes[ports],
                                                                                            longitudes[ports])).miles
                airport_ID = calls[ports]                       # smallest distance is saved to min_dist and name to ID
                timezone = tzf.timezone_at(lng=longitudes[ports], lat=latitudes[ports])  # finds timezone of airport
        minimums.append(min_dist)                               # now OUTSIDE of the loop we form list of closest ports
        airports_by_index.append(airport_ID)                    # appends airport list
        timezones.append(timezone)                              # appends timezone list

    # ---------------------------------- Forms new Data base from these lists ------------------------------------------
    df1 = pd.DataFrame(egauge_names, columns=['eGauge'])        # forming database columns from lists
    df2 = pd.DataFrame(airports_by_index, columns=['Airport Code'])
    dftimezone = pd.DataFrame(timezones, columns=['Airport timezone'])
    df3 = pd.DataFrame(minimums, columns=['Distance between'])
    finished_dataframe = pd.concat([df1, df2, dftimezone, df3], join='outer', axis=1)
    finished_dataframe.to_csv('eGauges-nearest-airport.csv', sep=',', encoding='utf-8')         # write to CSV
    return finished_dataframe, airports_by_index


# ------------------------------------------- AUTOMATED HTML SHIFTER ---------------------------------------------------

def scrape_all_airports(egauges, start):
    df = pd.read_csv(egauges, sep=',')                       # takes in dataframe of egauges and nearest airports
    airports = df['Airport Code'].tolist()                    # uses the airport code to scrape each website
    timezones = df['Airport timezone'].tolist()               # uses timezones to make UTC column
    homes = df['eGauge'].tolist()
    scrapped = []                                             # makes sure we dont have to repeat multiple airports
    for index in range(len(airports)):
        if os.path.isfile(airports[index] + str(start) + '-WeatherHistory.csv'):
            scrapped.append(airports[index])                            # of files that already got done.
        if airports[index] in scrapped:
            continue
        else:
            scrapped.append(airports[index])        # so you dont have to scrape the same airports twice
            try:
                scrape(homes[index], airports[index], timezones[index], start)
            except requests.ConnectionError, e:
                print e
            except requests.ConnectTimeout, e:
                print e
            except requests.exceptions.RequestException as e:
                print e
    return airports


# ----------------------------------------------METHOD CALLS------------------------------------------------------------
#print fetch_single_date_data("KRDU", 2016, 12, 30, 'America/New_York')
# print find_nearest_airports('egauge_data.xlsx')
# print scrape('egauge804', 'PHOG', 'Pacific/Honolulu', 2017)  # 'KAQW', "America/Los_Angeles"
print scrape_all_airports('eGauges-nearest-airport.csv', 2017)