import requests
from bs4 import BeautifulSoup
import pandas as pd
from geopy.distance import great_circle
import xlrd
import datetime
import timezonefinder
import pytz


# ---------------------------------------------------- WEB SCRAPER -----------------------------------------------------


'''
                                                   PROJECT DESCRIPTION
                                                   
    Method: fetch_data -- takes in a given airport code, year, month and day and finds the given URL on weather 
underground associated with those parameters. scrapes the table housing hourly weather metrics and deposites them into
a pandas data frame and prints to CSV
    Method: Scrape -- Follows same procedure as previous method however takes in just the airport code and iterates 
through all dates from present to the earliest recorded hourly weather data. and returns a data frame of all hourly data
gathered for that weather station.
    Method Find_Nearest_Airports -- takes in an excel CSV file that I manually filled in with all longitudes and 
latitudes associated with the given list of eGauges and uses Geopy's great circle calculator to find the airport within
minimum distance. Also uses timezonefinder library to form a timezone column for each airport so I can form a UTC column
later in the final output. Then builds a pandas data frame with those values and prints to CSV
    Method Scrape_All_Airports -- reads in the CSV file created in the previous method and uses just the airport code 
column to form a list of airport codes. Iterates through that list using each airport code as an input for the scrape 
method such that a unique CSV file with all hourly data for every airports is created. 
'''


def fetch_data(airport_code, year, month, day):
    url = "https://www.wunderground.com/history/airport/%s/%s/%s/%s/DailyHistory.html" % (airport_code, year, month, day)
    req = requests.get(url)
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
    # columns
    closer_look = soup.find_all("tr", {"class": "no-metars"})
    for stuff in closer_look:
        metrics.append(stuff.text + ',')  # forms a list of Strings of metrics for each hour entry in a day
    for hours in range(len(metrics)):                   # ORIGINALLY had metrics[0] as len but i think this needs to be hours in entire day
        temp_list = []                                  # temporary list that resets I think for every hour
        metrics[hours] = metrics[hours].split('\n')     # splits each hour of string data into a list with empty slots
        for data in metrics[hours]:                     # for all those entries including empty slots
            if len(data) >= 1:                          # if the slot isnt empty
                temp_list.append(data)                  # append it to this temporary list that resets every day
        hour.append(str(month) + '/' + str(day) + '/' + str(year) + ' ' + temp_list[0])
        # --------------------------------------------------- T E S T --------------------------------------------------
        naive_time = datetime.datetime.strptime(str(month) + '/' + str(day) + '/' + str(year) + ' ' + temp_list[0],
                                              '%m/%d/%Y %I:%M %p')
        local_timezone = pytz.timezone("America/Los_Angeles")
        local_dt = local_timezone.localize(naive_time, is_dst=None)
        utc_dt = local_dt.astimezone(pytz.utc)
        UTC.append(utc_dt)
        # --------------------------------------------------- T E S T --------------------------------------------------
        temp.append(temp_list[1])
        dew_point.append(temp_list[2])
        humidity.append(temp_list[3])                        # appends to all the column categories
        pressure.append(temp_list[4])
        visibility.append(temp_list[5])
        wind_direction.append(temp_list[6])
        wind_speed.append(temp_list[7])
        gust_speed.append(temp_list[8])
        percip.append(temp_list[9])
        events.append(temp_list[10])
        conditions.append(temp_list[11])
    UTC_column = pd.DataFrame(UTC, columns=['UTC'])
    time_column = pd.DataFrame(hour, columns=['Local Time'])
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
    hourly_dataframe = pd.concat([UTC_column, time_column, temp_column, dew_column, humidity_column, pressure_column,
                                  visibility_column, wind_direction_column, wind_speed_column, gust_speed_column,
                                  precipitation_column, events_column, conditions_column], join='outer', axis=1)
    hourly_dataframe.to_csv(airport_code + '-NEW.csv', sep='\t', encoding='utf-8')
    return hourly_dataframe


# ------------------------------------------- AUTOMATED HTML SHIFTER ---------------------------------------------------
# ------------------------------------ utilizing web scraper and data frame --------------------------------------------


def scrape(airport_code, timezone):
    date = datetime.date(1997, 7, 27)     # 1996, 7, 30: TEST change back to .today()
    tdelta = datetime.timedelta(days=200)   # TEST should be days=1 Defined change variable for datetime object
    url = "https://www.wunderground.com/history/airport/%s/%s/%s/%s/DailyHistory.html" % (airport_code, date.year, date.month, date.day)
    req = requests.get(url)                                     # requests download HTML specified above
    soup = BeautifulSoup(req.content, "lxml")                   # organizes HTML
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
    # columns
    while soup.find_all("tr", {"class": "no-metars"}):          # loops until HTML page no longer includes hourly table
        print airport_code + ' on ' + str(date.month) + '/' + str(date.day) + '/' + str(date.year) + " exists"  # TEST
        # -----------------------------------------------Loop body------------------------------------------------------
        metrics = []                                          # I originally had it form a list of hours and metrics
        closer_look = soup.find_all("tr", {"class": "no-metars"})   # Forms the list of all hours
        for stuff in closer_look:
            metrics.append(stuff.text + ',')  # forms a list of Strings of metrics for each hour entry in a day
        for hours in range(
                len(metrics)):  # ORIGINALLY had metrics[0] as len but i think this needs to be hours in entire day
            temp_list = []  # temporary list that resets I think for every hour
            metrics[hours] = metrics[hours].split('\n')  # splits each hour of string data into a list with empty slots
            for data in metrics[hours]:  # for all those entries including empty slots
                if len(data) >= 1:  # if the slot isnt empty
                    temp_list.append(data)  # append it to this temporary list that resets every day
            hour.append(str(date.month) + '/' + str(date.day) + '/' + str(date.year) + ' -- ' + temp_list[0])
            # ------------------------------------- This portion strictly for UTC column -------------------------------
            naive_time = datetime.datetime.strptime(str(date.month) + '/' + str(date.day) + '/' + str(date.year) + ' '
                                                    + temp_list[0], '%m/%d/%Y %I:%M %p')
            local_timezone = pytz.timezone(timezone)
            local_dt = local_timezone.localize(naive_time, is_dst=None)
            utc_dt = local_dt.astimezone(pytz.utc)
            UTC.append(utc_dt)
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
        print "SCRAPED" + '\n'                              # used to view procedurally what is happening
        # ------------------------------------------Looping conditions--------------------------------------------------
        date = date - tdelta                                    # Changes date to 1 day in the past
        url = "https://www.wunderground.com/history/airport/%s/%s/%s/%s/DailyHistory.html" % (
            airport_code, date.year, date.month, date.day)      # repeats process of requests but for new date
        req = requests.get(url)
        soup = BeautifulSoup(req.content, "lxml")
    # -------------------------------- OUTSIDE OF LOOP: Forms new Data base from these lists ---------------------------
    UTC_column = pd.DataFrame(UTC, columns=['UTC'])
    time_column = pd.DataFrame(hour, columns=['Local time'])
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
    hourly_dataframe_full = pd.concat([UTC_column, time_column, temp_column, dew_column, humidity_column,
                                       pressure_column, visibility_column, wind_direction_column, wind_speed_column,
                                       gust_speed_column, precipitation_column, events_column, conditions_column],
                                      join='outer', axis=1)
    hourly_dataframe_full.to_csv(airport_code + '-WeatherHistory.csv', sep='\t', encoding='utf-8')
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
            if great_circle((egauge_latitudes[gauges], egauge_longitudes[gauges]), (latitudes[ports], longitudes[ports])).miles < min_dist:
                min_dist = great_circle((egauge_latitudes[gauges], egauge_longitudes[gauges]), (latitudes[ports], longitudes[ports])).miles
                airport_ID = calls[ports]                       # smallest distance is saved to min_dist and name to ID
                timezone = tzf.timezone_at(lng=longitudes[ports], lat=latitudes[ports])  # finds timezone of airport
        minimums.append(min_dist)                               # now OUTSIDE of the loop we form list of closest ports
        airports_by_index.append(airport_ID)                    # appends airport list
        timezones.append(timezone)                              # appends timezone list

    # ---------------------------------- Forms new Data base from these lists ------------------------------------------
    df1 = pd.DataFrame(egauge_names, columns=['eGauge'])        # forming database columns from lists
    df2 = pd.DataFrame(airports_by_index, columns=['Airport Code'])
    dftimezone = pd.DataFrame(timezones, columns=['Airport timezone'])  # **********************************************
    df3 = pd.DataFrame(minimums, columns=['Distance between'])
    finished_dataframe = pd.concat([df1, df2, dftimezone, df3], join='outer', axis=1)  # *******************************
    finished_dataframe.to_csv('eGauges nearest airport.csv', sep='\t', encoding='utf-8')         # write to CSV
    return finished_dataframe, airports_by_index


# ------------------------------------------- AUTOMATED HTML SHIFTER ---------------------------------------------------


def scrape_all_airports(egauges):
    df = pd.read_csv(egauges, sep='\t')                       # takes in dataframe of egauges and nearest airports
    print df
    airports = df['Airport Code'].tolist()                    # uses the airport code to scrape each website
    timezones = df['Airport timezone'].tolist()               # uses timezones to make UTC column
    for index in range(len(airports)):
        scrape(airports[index], timezones[index])
    return airports


'''
Finally:
remove units from columns and place them in the header. 
learn how to append a list so i dont have to do it over and over again for the entire thing
'''


# ----------------------------------------------METHOD CALLS------------------------------------------------------------
# print fetch_data("KAQW", 2017, 3, 1)
# print find_nearest_airports('egauge_data.xlsx')
# print scrape('KAQW', "America/Los_Angeles")
print scrape_all_airports('eGauges nearest airport.csv')

