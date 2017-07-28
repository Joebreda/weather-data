import pandas as pd
import datetime
import xlrd

# ---------------------------------- Creates Rounded Timestamp Column To Nearest Hour ----------------------------------

# needs final.csv which is a list of all airports
# needs all weather data files
# needs rounded-data folder to house after
def rounded_column_generator():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    for x in range(len(airports)):
        print str(airports[x])
        current_df = pd.read_csv(str('all-weather-data/' + airports[x]) + '-WeatherHistory.csv', sep=',')
        timestamp_col = current_df['Timestamp'].tolist()
        rounded_to_hour = []
        for y in range(len(timestamp_col)):
            if timestamp_col[y] % 3600 <= 1800:
                rounded_to_hour.append(timestamp_col[y] - (timestamp_col[y] % 3600))
            if timestamp_col[y] % 3600 > 1800:
                rounded_to_hour.append(timestamp_col[y] + (3600 - (timestamp_col[y] % 3600)))
        rounded_column = pd.DataFrame(rounded_to_hour, columns=['Hour Rounded Timestamp'])
        result = pd.concat([rounded_column, current_df], join='outer', axis=1)
        result.to_csv('rounded-data/' + str(airports[x]) + '-WeatherHistory.csv', sep=',', encoding='utf-8',
                      index=False)
    return result
# -------- TEST -------- TEST -------- TEST -------- TEST -------- TEST -------- TEST -------- TEST -------- TEST ------
def example_of_rounding():
    timestamp = 1498889640 # 6:14am
    six_am = 1498888800
    print six_am % 3600
    print timestamp % 3600

    six_thirty_am = 1498890600
    six_thirty_one_am = 1498890660

    print six_thirty_am % 3600
    print six_thirty_one_am % 3600
    times_list = []

    for number in range(0,3600):
        times_list.append(six_am + number)

    print times_list

    for z in range(len(times_list)):
        if times_list[z] % 3600 <= 1800:
            times_list[z] = times_list[z] - (times_list[z] % 3600)
        if times_list[z] % 3600 > 1800:
            times_list[z] = times_list[z] + (3600 - (times_list[z] % 3600))

    print times_list


# -------------------------------- Removes Units From Column and Adds Units To Header ----------------------------------

# data cleaning process must include:
# remove units from temp
# remove units from dew
# remove units from humidity
# remove units from pressure
# remove units from visibility
# exchange wind direction with number
# remove units from wind speed
# remove units from gust speed
# exchange conditions
# do all of this excluding nan, -, NaN, ' ', etc.
def remove_units():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    for x in range(len(airports)):
        print str(airports[x])
        current_df = pd.read_csv(str('rounded-data/' + airports[x]) + '-WeatherHistory.csv', sep=',')
        rts_col = current_df['Hour Rounded Timestamp'].tolist()
        ts_col = current_df['Timestamp'].tolist()
        lt_col = current_df['WU Local time'].tolist()

        temp_col = current_df['Temp'].tolist()
        dew_col = current_df['Dew Point'].tolist()
        humidity_col = current_df['Humidity'].tolist()
        pressure_col = current_df['Pressure'].tolist()
        visibility_col = current_df['Visibility'].tolist()
        wind_dir_col = current_df['Wind Direction'].tolist()
        wind_sp_col = current_df['Wind Speed'].tolist()
        gust_col = current_df['Gust Speed'].tolist()
        precip_col = current_df['Precipitation'].tolist()
        conditions_col = current_df['Conditions'].tolist()

        events_col = current_df['Events'].tolist()

        for n in range(len(temp_col)):
            temp_col[n] = temp_col[n][:-5]
            dew_col[n] = dew_col[n][:-5]
            humidity_col[n] = humidity_col[n][:-1]
            pressure_col[n] = pressure_col[n][:-4]
            visibility_col[n] = visibility_col[n][:-4]
            if wind_dir_col[n] == 'North':
                wind_dir_col[n] = 0
            if wind_dir_col[n] == 'NNE':
                wind_dir_col[n] = 22.5
            if wind_dir_col[n] == 'NE':
                wind_dir_col[n] = 45
            if wind_dir_col[n] == 'ENE':
                wind_dir_col[n] = 67.5
            if wind_dir_col[n] == 'East':
                wind_dir_col[n] = 90
            if wind_dir_col[n] == 'ESE':
                wind_dir_col[n] = 112.5
            if wind_dir_col[n] == 'SE':
                wind_dir_col[n] = 135
            if wind_dir_col[n] == 'SSE':
                wind_dir_col[n] = 157.5
            if wind_dir_col[n] == 'South':
                wind_dir_col[n] = 180
            if wind_dir_col[n] == 'SSW':
                wind_dir_col[n] = 202.5
            if wind_dir_col[n] == 'SW':
                wind_dir_col[n] = 225
            if wind_dir_col[n] == 'WSW':
                wind_dir_col[n] = 247.5
            if wind_dir_col[n] == 'West':
                wind_dir_col[n] = 270
            if wind_dir_col[n] == 'WNW':
                wind_dir_col[n] = 292.5
            if wind_dir_col[n] == 'NW':
                wind_dir_col[n] = 315
            if wind_dir_col[n] == 'NNW':
                wind_dir_col[n] = 337.5
            if wind_sp_col[n] != 'Calm':
                if type(wind_sp_col[n]) == str:
                    wind_sp_col[n] = wind_sp_col[n][:-5]
            if gust_col != 'Calm':
                if type(gust_col[n]) == str:
                    gust_col[n] = gust_col[n][:-5]
            if type(precip_col[n]) == str:
                precip_col[n] = precip_col[n][:-4]

            # Cloud Options 0's
            if conditions_col[n] == 'Clear':
                conditions_col[n] = 0
            if conditions_col[n] == 'Scattered Clouds':
                conditions_col[n] = 3
            if conditions_col[n] == 'Partly Cloudy':
                conditions_col[n] = 4
            if conditions_col[n] == 'Mostly Cloudy':
                conditions_col[n] = 6
            if conditions_col[n] == 'Overcast':
                conditions_col[n] = 8
            # Rain Options 10's
            if conditions_col[n] == 'Light Drizzle':
                conditions_col[n] = 7
            if conditions_col[n] == 'Light Freezing Drizzle':
                conditions_col[n] = 7
            if conditions_col[n] == 'Drizzle':
                conditions_col[n] = 7
            if conditions_col[n] == 'Heavy Drizzle':
                conditions_col[n] = 8
            if conditions_col[n] == 'Light Rain':
                conditions_col[n] = 8
            if conditions_col[n] == 'Light Freezing Rain':
                conditions_col[n] = 8
            if conditions_col[n] == 'Light Rain Showers':
                conditions_col[n] = 8
            if conditions_col[n] == 'Rain':
                conditions_col[n] = 8
            if conditions_col[n] == 'Squalls':
                conditions_col[n] = 8
            if conditions_col[n] == 'Rain Showers':
                conditions_col[n] = 8
            if conditions_col[n] == 'Heavy Rain':
                conditions_col[n] = 8
            if conditions_col[n] == 'Heavy Rain Showers':
                conditions_col[n] = 8
            if conditions_col[n] == 'Thunderstorm':
                conditions_col[n] = 8
            if conditions_col[n] == 'Thunderstorms with Small Hail':
                conditions_col[n] = 8
            if conditions_col[n] == 'Heavy Thunderstorm':
                conditions_col[n] = 8
            if conditions_col[n] == 'Funnel Cloud':
                conditions_col[n] = 8
            # snow 20's
            if conditions_col[n] == 'Low Drifting Snow':
                conditions_col[n] = '-'
            if conditions_col[n] == 'Blowing Snow':
                conditions_col[n] = '-'
            if conditions_col[n] == 'Heavy Blowing Snow':
                conditions_col[n] = '-'
            if conditions_col[n] == 'Light Snow':
                conditions_col[n] = 8
            if conditions_col[n] == 'Snow Grains':
                conditions_col[n] = 8
            if conditions_col[n] == 'Light Snow Grains':
                conditions_col[n] = 8
            if conditions_col[n] == 'Ice Crystals':
                conditions_col[n] = 8
            if conditions_col[n] == 'Light Snow Showers':
                conditions_col[n] = 8
            if conditions_col[n] == 'Light Hail':
                conditions_col[n] = 8
            if conditions_col[n] == 'Small Hail':
                conditions_col[n] = 8
            if conditions_col[n] == ' Light Ice Pellets':
                conditions_col[n] = 8
            if conditions_col[n] == 'Snow':
                conditions_col[n] = 8
            if conditions_col[n] == 'Ice Pellets':
                conditions_col[n] = 8
            if conditions_col[n] == 'Hail':
                conditions_col[n] = 8
            if conditions_col[n] == 'Snow Showers':
                conditions_col[n] = 8
            if conditions_col[n] == 'Heavy Snow':
                conditions_col[n] = 8
            if conditions_col[n] == 'Heavy Ice Pellets':
                conditions_col[n] = 8
            # Fog and Dust
            if conditions_col[n] == 'Light Freezing Fog':
                conditions_col[n] = 7
            if conditions_col[n] == 'Patches of Fog':
                conditions_col[n] = 7
            if conditions_col[n] == 'Haze':
                conditions_col[n] = 7
            if conditions_col[n] == 'Shallow Fog':
                conditions_col[n] = '-'
            if conditions_col[n] == 'Light Fog':
                conditions_col[n] = 7
            if conditions_col[n] == 'Widespread Dust':
                conditions_col[n] = '-'
            if conditions_col[n] == 'Light Sand':
                conditions_col[n] = '-'
            if conditions_col[n] == 'Sand':
                conditions_col[n] = '-'
            if conditions_col[n] == 'Dust Whirls':
                conditions_col[n] = '-'
            if conditions_col[n] == 'Blowing Sand':
                conditions_col[n] = '-'
            if conditions_col[n] == 'Light Haze':
                conditions_col[n] = '-'
            if conditions_col[n] == 'Smoke':
                conditions_col[n] = '-'
            if conditions_col[n] == 'Mist':
                conditions_col[n] = 7
            if conditions_col[n] == 'Fog':
                conditions_col[n] = 7

            # Useless info
            if conditions_col[n] == 'Unknown':
                conditions_col[n] = '-'
            if conditions_col[n] == '\t,':
                conditions_col[n] = '-'
            if conditions_col[n] == '\t':
                conditions_col[n] = '-'
            if conditions_col[n] == '\t\xc2\xa0':
                conditions_col[n] = '-'
            if conditions_col[n] == ',':
                conditions_col[n] = '-'

        c1 = pd.DataFrame(rts_col, columns=['Hour Rounded Timestamp'])
        c2 = pd.DataFrame(ts_col, columns=['Timestamp'])
        c3 = pd.DataFrame(lt_col, columns=['WU Local time'])
        c4 = pd.DataFrame(temp_col, columns=['Temp (degrees F)'])
        c5 = pd.DataFrame(dew_col, columns=['Dew Point (degrees F)'])
        c6 = pd.DataFrame(humidity_col, columns=['Humidity (%)'])
        c7 = pd.DataFrame(pressure_col, columns=['Pressure (in)'])
        c8 = pd.DataFrame(visibility_col, columns=['Visibility (mi)'])
        c9 = pd.DataFrame(wind_dir_col, columns=['Wind Direction'])
        c10 = pd.DataFrame(wind_sp_col, columns=['Wind Speed (mph)'])
        c11 = pd.DataFrame(gust_col, columns=['Gust Speed (mph)'])
        c12 = pd.DataFrame(precip_col, columns=['Precipitation (in)'])
        c13 = pd.DataFrame(events_col, columns=['Events'])
        c14 = pd.DataFrame(conditions_col, columns=['Conditions'])
        cleaned = pd.concat([c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14], join='outer', axis=1)
        cleaned.to_csv('cleaned-data/' + str(airports[x]) + '-CleanedWeatherHistory.csv', sep=',', encoding='utf-8',
                       index=False)
    return cleaned

# -------- A N A L Y S I S ---------------- A N A L Y S I S ---------------- A N A L Y S I S ---------------- A N A L Y


def whats_in_that_column():
    test = pd.read_csv('rounded-data/KBJC-WeatherHistory.csv', sep=',')
    print test
    con = test['Wind Speed'].tolist()
    choices = []
    float_index = []
    types = []
    for m in range(len(con)):
        if con[m] not in choices:
            choices.append(con[m])
        if type(con[m]) == float:
            float_index.append(m)
        types.append(type(con[m]))
    df = pd.DataFrame(choices)
    print choices
    print df
    print float_index
    print con[10515]
    print con[19287]
    print con[35486]
    print con[45442]
    print float('nan') == type(con[10515])
    df2 = pd.DataFrame(types)
    print df2


def conditions_that_exist():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    possible_conditions = []
    amount = [0] * 56
    for x in range(len(airports)):
        print str(airports[x])
        current_df = pd.read_csv(str('rounded-data/' + airports[x]) + '-WeatherHistory.csv', sep=',')
        conditions_col = current_df['Conditions'].tolist()
        for n in range(len(conditions_col)):
            if conditions_col[n] not in possible_conditions:
                possible_conditions.append(conditions_col[n])
            if conditions_col[n] in possible_conditions:
                index = possible_conditions.index(conditions_col[n])
                amount[index] += 1
    datatype = pd.DataFrame(possible_conditions, columns=['Condition'])
    amountofdata = pd.DataFrame(amount, columns=['amount'])
    dfa = pd.concat([datatype, amountofdata], join='outer', axis=1)
    dfa = dfa.sort_values(['amount'], ascending=[False])
    dfa.to_csv('conditions.csv', sep=',', encoding='utf-8', index=False)
    print dfa


def temp_analysis():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    tempmax_col = []
    tempmin_col = []
    mostfrequent = []
    amount = []
    for x in range(len(airports)):
        tempmax = 0
        tempmin = 1000000000
        frequencies = {}
        print str(airports[x])
        current_df = pd.read_csv(str('cleaned-data/' + airports[x]) + '-CleanedWeatherHistory.csv', sep=',')
        temp_col = current_df['Temp (degrees F)'].tolist()
        for n in range(len(temp_col)):
            if temp_col[n] >= 125:
                continue
            # adding ticks to frequency column
            if temp_col[n] not in frequencies:
                frequencies[temp_col[n]] = 1
            else:
                frequencies[temp_col[n]] += 1
            # finding max and min
            if temp_col[n] <= tempmin:
                tempmin = temp_col[n]
            if temp_col[n] >= tempmax:
                tempmax = temp_col[n]
        final_df = pd.DataFrame(frequencies.items(), columns=['Temps', 'Frequencies'])
        final_df = final_df.sort_values(['Frequencies'], ascending=[False])
        # final_df = final_df.drop('NaN', axis=0)
        # print final_df
        c1 = final_df['Temps'].tolist()
        c2 = final_df['Frequencies'].tolist()
        tempmax_col.append(tempmax)
        tempmin_col.append(tempmin)
        mostfrequent.append(c1[0])
        amount.append(c2[0])
    d1 = pd.DataFrame(airports, columns=['Airports'])
    d2 = pd.DataFrame(tempmax_col, columns=['Max degrees F'])
    d3 = pd.DataFrame(tempmin_col, columns=['Min degrees F'])
    d4 = pd.DataFrame(mostfrequent, columns=['Most Frequent Temp'])
    d5 = pd.DataFrame(amount, columns=['Frequency'])
    df2 = pd.concat([d1, d2, d3, d4, d5], join='outer', axis=1)
    df2.to_csv('temp_analysis.csv', sep=',', encoding='utf-8', index=False)


def dew_analysis():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    dewmax_col = []
    dewmin_col = []
    mostfrequent = []
    amount = []
    for x in range(len(airports)):
        dewmax = 0
        dewmin = 1000000000
        frequencies = {}
        print str(airports[x])
        current_df = pd.read_csv(str('cleaned-data/' + airports[x]) + '-CleanedWeatherHistory.csv', sep=',')
        dew_col = current_df['Dew Point (degrees F)'].tolist()
        for n in range(len(dew_col)):
            if dew_col[n] >= 125:
                continue
            # adding ticks to frequency column
            if dew_col[n] not in frequencies:
                frequencies[dew_col[n]] = 1
            else:
                frequencies[dew_col[n]] += 1
            # finding max and min
            if dew_col[n] <= dewmin:
                dewmin = dew_col[n]
            if dew_col[n] >= dewmax:
                dewmax = dew_col[n]
        final_df = pd.DataFrame(frequencies.items(), columns=['Dew', 'Frequencies'])
        final_df = final_df.sort_values(['Frequencies'], ascending=[False])
        # final_df = final_df.drop('NaN', axis=0)
        # print final_df
        c1 = final_df['Dew'].tolist()
        c2 = final_df['Frequencies'].tolist()
        dewmax_col.append(dewmax)
        dewmin_col.append(dewmin)
        mostfrequent.append(c1[0])
        amount.append(c2[0])
    d1 = pd.DataFrame(airports, columns=['Airports'])
    d2 = pd.DataFrame(dewmax_col, columns=['Max dewpoint F'])
    d3 = pd.DataFrame(dewmin_col, columns=['Min dewpoint F'])
    d4 = pd.DataFrame(mostfrequent, columns=['Most Frequent dewpoint'])
    d5 = pd.DataFrame(amount, columns=['Frequency'])
    df2 = pd.concat([d1, d2, d3, d4, d5], join='outer', axis=1)
    df2.to_csv('dew_analysis.csv', sep=',', encoding='utf-8', index=False)


def humidity_analysis():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    hmax_col = []
    hmin_col = []
    mostfrequent = []
    amount = []
    for x in range(len(airports)):
        hmax = 0
        hmin = 1000000000
        frequencies = {}
        print str(airports[x])
        current_df = pd.read_csv(str('cleaned-data/' + airports[x]) + '-CleanedWeatherHistory.csv', sep=',')
        h_col = current_df['Humidity (%)'].tolist()
        for n in range(len(h_col)):
            #if type(h_col[n]) == str:
                #continue
            # adding ticks to frequency column
            if h_col[n] not in frequencies:
                frequencies[h_col[n]] = 1
            if h_col[n] in frequencies:
                frequencies[h_col[n]] += 1
            # finding max and min
            if float(h_col[n]) <= float(hmin):
                hmin = float(h_col[n])
            if h_col[n] >= hmax:
                hmax = h_col[n]
        final_df = pd.DataFrame(frequencies.items(), columns=['Humidity', 'Frequencies'])
        final_df = final_df.sort_values(['Frequencies'], ascending=[False])
        # final_df = final_df.drop('NaN', axis=0)
        # print final_df
        c1 = final_df['Humidity'].tolist()
        c2 = final_df['Frequencies'].tolist()
        hmax_col.append(hmax)
        hmin_col.append(hmin)
        mostfrequent.append(c1[0])
        amount.append(c2[0])
    d1 = pd.DataFrame(airports, columns=['Airports'])
    d2 = pd.DataFrame(hmax_col, columns=['Max Humidity %'])
    d3 = pd.DataFrame(hmin_col, columns=['Min Humidity %'])
    d4 = pd.DataFrame(mostfrequent, columns=['Most Frequent humidity'])
    d5 = pd.DataFrame(amount, columns=['Frequency'])
    df2 = pd.concat([d1, d2, d3, d4, d5], join='outer', axis=1)
    df2.to_csv('humidity_analysis.csv', sep=',', encoding='utf-8', index=False)


def pressure_analysis():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    pmax_col = []
    pmin_col = []
    mostfrequent = []
    amount = []
    for x in range(len(airports)):
        pmax = 0
        pmin = 1000000000
        frequencies = {}
        print str(airports[x])
        current_df = pd.read_csv(str('cleaned-data/' + airports[x]) + '-CleanedWeatherHistory.csv', sep=',')
        p_col = current_df['Pressure (in)'].tolist()
        for n in range(len(p_col)):
            #if p_col[n] >= 125:
                #continue
            # adding ticks to frequency column
            if p_col[n] not in frequencies:
                frequencies[p_col[n]] = 1
            else:
                frequencies[p_col[n]] += 1
            # finding max and min
            if p_col[n] <= pmin:
                pmin = p_col[n]
            if p_col[n] >= pmax:
                pmax = p_col[n]
        final_df = pd.DataFrame(frequencies.items(), columns=['Pressure', 'Frequencies'])
        final_df = final_df.sort_values(['Frequencies'], ascending=[False])
        # final_df = final_df.drop('NaN', axis=0)
        # print final_df
        c1 = final_df['Pressure'].tolist()
        c2 = final_df['Frequencies'].tolist()
        pmax_col.append(pmax)
        pmin_col.append(pmin)
        mostfrequent.append(c1[0])
        amount.append(c2[0])
    d1 = pd.DataFrame(airports, columns=['Airports'])
    d2 = pd.DataFrame(pmax_col, columns=['Max pressure in'])
    d3 = pd.DataFrame(pmin_col, columns=['Min pressure in'])
    d4 = pd.DataFrame(mostfrequent, columns=['Most Frequent pressure'])
    d5 = pd.DataFrame(amount, columns=['Frequency'])
    df2 = pd.concat([d1, d2, d3, d4, d5], join='outer', axis=1)
    df2.to_csv('pressure_analysis.csv', sep=',', encoding='utf-8', index=False)



def visibility_analysis():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    vmax_col = []
    vmin_col = []
    mostfrequent = []
    amount = []
    for x in range(len(airports)):
        vmax = 0
        vmin = 1000000000
        frequencies = {}
        print str(airports[x])
        current_df = pd.read_csv(str('cleaned-data/' + airports[x]) + '-CleanedWeatherHistory.csv', sep=',')
        v_col = current_df['Visibility (mi)'].tolist()
        for n in range(len(v_col)):
            #if v_col[n] >= 125:
                #continue
            # adding ticks to frequency column
            if v_col[n] not in frequencies:
                frequencies[v_col[n]] = 1
            else:
                frequencies[v_col[n]] += 1
            # finding max and min
            if v_col[n] <= vmin:
                vmin = v_col[n]
            if v_col[n] >= vmax:
                vmax = v_col[n]
        final_df = pd.DataFrame(frequencies.items(), columns=['Visibility', 'Frequencies'])
        final_df = final_df.sort_values(['Frequencies'], ascending=[False])
        # final_df = final_df.drop('NaN', axis=0)
        # print final_df
        c1 = final_df['Visibility'].tolist()
        c2 = final_df['Frequencies'].tolist()
        vmax_col.append(vmax)
        vmin_col.append(vmin)
        mostfrequent.append(c1[0])
        amount.append(c2[0])
    d1 = pd.DataFrame(airports, columns=['Airports'])
    d2 = pd.DataFrame(vmax_col, columns=['Max Visibility mi'])
    d3 = pd.DataFrame(vmin_col, columns=['Min Visibility mi'])
    d4 = pd.DataFrame(mostfrequent, columns=['Most Frequent Visibility'])
    d5 = pd.DataFrame(amount, columns=['Frequency'])
    df2 = pd.concat([d1, d2, d3, d4, d5], join='outer', axis=1)
    df2.to_csv('visibility_analysis.csv', sep=',', encoding='utf-8', index=False)


def wind_speed_analysis():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    wmax_col = []
    wmin_col = []
    mostfrequent = []
    amount = []
    for x in range(len(airports)):
        wmax = 0
        wmin = 1000000000
        frequencies = {}
        print str(airports[x])
        current_df = pd.read_csv(str('cleaned-data/' + airports[x]) + '-CleanedWeatherHistory.csv', sep=',')
        w_col = current_df['Wind Speed (mph)'].tolist()
        for n in range(len(w_col)):
            #if w_col[n] >= 125:
                #continue
            # adding ticks to frequency column
            if w_col[n] not in frequencies:
                frequencies[w_col[n]] = 1
            else:
                frequencies[w_col[n]] += 1
            # finding max and min
            if w_col[n] == 'Calm':
                wmin = w_col[n]
            if w_col[n] >= wmax and w_col[n] != 'Calm':
                wmax = w_col[n]
        final_df = pd.DataFrame(frequencies.items(), columns=['ws', 'Frequencies'])
        final_df = final_df.sort_values(['Frequencies'], ascending=[False])
        # final_df = final_df.drop('NaN', axis=0)
        # print final_df
        c1 = final_df['ws'].tolist()
        c2 = final_df['Frequencies'].tolist()
        wmax_col.append(wmax)
        wmin_col.append(wmin)
        mostfrequent.append(c1[0])
        amount.append(c2[0])
    d1 = pd.DataFrame(airports, columns=['Airports'])
    d2 = pd.DataFrame(wmax_col, columns=['Max Wind Speed mph'])
    d3 = pd.DataFrame(wmin_col, columns=['Min Wind Speed mph'])
    d4 = pd.DataFrame(mostfrequent, columns=['Most Frequent wind speed'])
    d5 = pd.DataFrame(amount, columns=['Frequency'])
    df2 = pd.concat([d1, d2, d3, d4, d5], join='outer', axis=1)
    df2.to_csv('ws_analysis.csv', sep=',', encoding='utf-8', index=False)



def find_date_of_odd_(airportcode, temp):
    current_df = pd.read_csv('cleaned-data/' + airportcode + '-CleanedWeatherHistory.csv', sep=',')
    temps = current_df['Temp (degrees F)'].tolist()
    times = current_df['WU Local time'].tolist()
    dew = current_df['Dew Point (degrees F)'].tolist()
    tempmax = 0
    for y in range(len(temps)):
        if temps[y] == temp:
            index = temps.index(temp)
            break
    print temp
    print times[index]
    print dew[index]


def average_distance_from_airport():
    current_df = pd.read_csv('eGauges-nearest-airport.csv', sep=',')
    current_df = current_df.drop(80)
    print current_df
    distance = current_df['Distance between'].tolist()
    total = sum(distance)
    average = total/40
    print average


def count_false_rows():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    counter = 0
    strings = []
    for x in range(len(airports)):
        print str(airports[x])
        current_df = pd.read_csv(str('cleaned-data/' + airports[x]) + '-CleanedWeatherHistory.csv', sep=',')
        w_col = current_df['Wind Direction'].tolist()
        v_col = current_df['Visibility (mi)'].tolist()
        h_col = current_df['Humidity (%)'].tolist()
        for m in range(len(h_col)):
            if type(h_col[m]) == str:
                counter += 1
                strings.append(h_col[m])
    d1 = pd.DataFrame(strings, columns=['strings'])
    d1.to_csv('strings.csv', sep=',', encoding='utf-8', index=False)

    return counter

# ---------------------------- M A K I N G P L O T T A B L E 2 C O L U M N C S V F I L E S -----------------------------

def plottable_temp_vs_condition():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    temps = []
    conditions = []
    for x in range(len(airports)):
        print str(airports[x])
        current_df = pd.read_csv(str('cleaned-data/' + airports[x]) + '-CleanedWeatherHistory.csv', sep=',')
        cond_col = current_df['Conditions'].tolist()
        temp_col = current_df['Temp (degrees F)'].tolist()
        for n in range(len(cond_col)):
            if temp_col[n] >= 125:
                continue
           #  if cond_col[n] == 0 or cond_col[n] == 3 or cond_col[n] == 4 or cond_col[n] == 6 or cond_col[n] == 7 or cond_col[n] == 8:
            conditions.append(cond_col[n])
            temps.append(temp_col[n])
    d1 = pd.DataFrame(conditions)
    d2 = pd.DataFrame(temps)
    df2 = pd.concat([d1, d2], join='outer', axis=1)
    df2.to_csv('tempvcond.csv', sep=' ', encoding='utf-8', header=None, index=False)


def plottable_temp_vs_time_rounded():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    temps = []
    times = []
    for x in range(len(airports)):
        print str(airports[x])
        current_df = pd.read_csv(str('cleaned-data/' + airports[x]) + '-CleanedWeatherHistory.csv', sep=',')
        time_col = current_df['WU Local time'].tolist()
        temp_col = current_df['Temp (degrees F)'].tolist()
        for n in range(len(time_col)):
            if temp_col[n] >= 125:
                continue
            localdatetime = datetime.datetime.strptime(time_col[n], '%m/%d/%Y %I:%M %p')
            localdatetime = localdatetime.replace(minute=0, second=0)
            localtime = localdatetime.strftime('%H:%M')
            times.append(localtime)
            temps.append(temp_col[n])
    d1 = pd.DataFrame(times)
    d2 = pd.DataFrame(temps)
    df2 = pd.concat([d1, d2], join='outer', axis=1)
    df2.to_csv('tempvtimer.csv', sep=' ', encoding='utf-8', header=None, index=False)

def plottable_temp_vs_time():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    temps = []
    times = []
    for x in range(len(airports)):
        print str(airports[x])
        current_df = pd.read_csv(str('cleaned-data/' + airports[x]) + '-CleanedWeatherHistory.csv', sep=',')
        time_col = current_df['WU Local time'].tolist()
        temp_col = current_df['Temp (degrees F)'].tolist()
        for n in range(len(time_col)):
            if temp_col[n] >= 125:
                continue
            localdatetime = datetime.datetime.strptime(time_col[n], '%m/%d/%Y %I:%M %p')
            localtime = localdatetime.strftime('%H:%M')
            times.append(localtime)
            temps.append(temp_col[n])
    d1 = pd.DataFrame(times)
    d2 = pd.DataFrame(temps)
    df2 = pd.concat([d1, d2], join='outer', axis=1)
    df2.to_csv('tempvtime.csv', sep=' ', encoding='utf-8', header=None, index=False)

plottable_temp_vs_time()

def plottable_temp_vs_pressure():
    final = pd.read_csv('final.csv', sep=',')
    final = final[final.ports != 'ZLIC']
    airports = final['ports'].tolist()
    temps = []
    pressure = []
    for x in range(len(airports)):
        print str(airports[x])
        current_df = pd.read_csv(str('cleaned-data/' + airports[x]) + '-CleanedWeatherHistory.csv', sep=',')
        pres_col = current_df['Pressure (in)'].tolist()
        temp_col = current_df['Temp (degrees F)'].tolist()
        for n in range(len(pres_col)):
            if temp_col[n] >= 125:
                continue
           #  if cond_col[n] == 0 or cond_col[n] == 3 or cond_col[n] == 4 or cond_col[n] == 6 or cond_col[n] == 7 or cond_col[n] == 8:
            pressure.append(pres_col[n])
            temps.append(temp_col[n])
    d1 = pd.DataFrame(pressure)
    d2 = pd.DataFrame(temps)
    df2 = pd.concat([d1, d2], join='outer', axis=1)
    df2.to_csv('tempvpressure.csv', sep=' ', encoding='utf-8', header=None, index=False)

def plottable_longitude_latitude():
    egauge_latitudes = []
    egauge_longitudes = []
    workbook = xlrd.open_workbook('egauge_data.xlsx')  # Opens Excel sheet specified in parameters of method
    sheet = workbook.sheet_by_index(0)  # selects Sheet number 1
    for rows in range(1, sheet.nrows):  # loops over rows of the sheet excluding initial row
        egauge_latitudes.append(sheet.cell_value(rows, 1))  # forms a list using column 2
        egauge_longitudes.append(sheet.cell_value(rows, 2))  # forms a list using column 3
    d1 = pd.DataFrame(egauge_latitudes)
    d2 = pd.DataFrame(egauge_longitudes)
    df2 = pd.concat([d2, d1], join='outer', axis=1)
    df2.to_csv('longlat.csv', sep=' ', encoding='utf-8', header=None, index=False)





plottable_temp_vs_time()



#test = pd.read_csv('cleaned-data/KCHD-CleanedWeatherHistory.csv', sep=',')
#print test






