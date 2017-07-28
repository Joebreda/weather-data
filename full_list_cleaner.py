import pandas as pd
import datetime


def declutter():
    full_list = pd.read_csv('full_list.csv', sep=',')
    print full_list

    homes = full_list['Homes'].tolist()
    records = full_list['Earliest Record'].tolist()

    new_homes = []
    new_records = []

    for home in range(len(homes)):
        if homes[home] not in new_homes:
            new_homes.append(homes[home])
            new_records.append(records[home])

    col1 = pd.DataFrame(new_homes, columns=['Homes'])
    col2 = pd.DataFrame(new_records, columns=['Earliest Record'])
    full_list2 = pd.concat([col1, col2], join='outer', axis=1)
    full_list2.to_csv('full_list2.csv', sep=',', encoding='utf-8', index=False)


def check_proper_length():
    full_list = pd.read_csv('full_list.csv', sep=',')
    homes = full_list['Homes'].tolist()
    records = full_list['Earliest Record'].tolist()
    print len(homes)

    df = pd.read_csv('eGauges-nearest-airport.csv', sep=',')
    airports = df['eGauge'].tolist()  # uses the airport code to scrape each website
    check = []
    for home in range(len(airports)):
        if airports[home] not in homes:
            check.append(airports[home])
    print check


def size():
    df = pd.read_csv('eGauges-nearest-airport.csv', sep=',')
    homes = df['eGauge'].tolist()
    for egauge in homes:
        full_list = pd.read_csv('full_list.csv', sep=',')
        homes2 = full_list['Homes'].tolist()
        records = full_list['Earliest Record'].tolist()
        index_number = homes2.index(egauge)
        end_date_timestamp = records[index_number]
        print end_date_timestamp
        end_date_str = datetime.date.fromtimestamp(int(end_date_timestamp)).strftime('%Y-%m-%d')
        print end_date_str
        print type(end_date_str)
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()
        print end_date
        print type(end_date)

def finished_ports():
    df2 = pd.read_csv('ports.csv')
    df = pd.read_csv('eGauges-nearest-airport.csv', sep=',')
    ports = df['Airport Code'].tolist()
    done = df2['eGauges-nearest-airport.csv'].tolist()
    notdone = []
    for port in ports:
        if str(port)+'-WeatherHistory.csv' not in done:
            notdone.append(port)
    print ports
    # print done
    print notdone
    return notdone
# scrape the remaining dates

'''
full_list = pd.read_csv('full_list.csv')
airport_list = pd.read_csv('eGauges-nearest-airport.csv', sep=',')

n_ports = airport_list['Airport Code'].tolist()
n_egauges = airport_list['eGauge'].tolist()
f_egauges = full_list['Homes'].tolist()
f_records = full_list['Earliest Record'].tolist()

w_dates = []
w_ports = []

for x in range(len(n_ports)):
    if n_ports[x] not in w_ports:
        w_ports.append(n_ports[x])
        the_index = f_egauges.index(n_egauges[x])
        w_dates.append(f_records[the_index])
    if n_ports[x] in w_ports:
        port_index = w_ports.index(n_ports[x])
        if w_dates[port_index] >= f_records[x]
'''
def full_list_ascending():
    full_list = pd.read_csv('full_list.csv', sep=',')
    print full_list
    test = full_list.sort_values(['Earliest Record'], ascending=[True])
    print test
    test.to_csv('full_list2.csv', sep=',', encoding='utf-8', index=False)




def organized_final_df():
    full_list = pd.read_csv('full_list.csv', sep=',')
    full_list = full_list.sort_values(['Homes'], ascending=[True])
    #print full_list
    airport_list = pd.read_csv('eGauges-nearest-airport.csv', sep=',')
    airport_list = airport_list.sort_values(['eGauge'], ascending=[True])
    #print airport_list
    n_ports = airport_list['Airport Code'].tolist()
    n_egauges = airport_list['eGauge'].tolist()
    f_egauges = full_list['Homes'].tolist()
    f_records = full_list['Earliest Record'].tolist()
    longest_ports = []
    longest_egauge = []
    longest_times = []
    # print n_egauges  # good they are the exact same
    # print f_egauges
    # print len(n_ports)  # same lengths as well
    # print len(n_egauges)
    # print len(f_records)
    # print len(f_egauges)

    # forms an organized dataframe for the egauges that go back the farthest in solar data and their airports
    for x in range(len(n_ports)):
        if n_ports[x] not in longest_ports:
            longest_ports.append(n_ports[x])
            longest_egauge.append(n_egauges[x])
            longest_times.append(f_records[x])
        else:
            ind = longest_ports.index(n_ports[x])
            if f_records[x] < longest_times[ind]:
                longest_times[ind] = f_records[x]
    port = pd.DataFrame(longest_ports, columns=['ports'])
    time = pd.DataFrame(longest_times, columns=['time'])
    egauge = pd.DataFrame(longest_egauge, columns=['egauge'])
    final = pd.concat([port, time, egauge], join='outer', axis=1)
    final.to_csv('final.csv', sep=',', encoding='utf-8', index=False)
    print final
    # now replaces the earliest times for the airports that dont go back as far with the later time.
    print f_records
    print f_egauges
    print n_egauges
    print n_ports
    for y in range(len(n_ports)):
        for t in range(len(longest_times)):
            if n_ports[y] == longest_ports[t] and longest_times[t] < f_records[y]:
                f_records[y] = longest_times[t]
    col1 = pd.DataFrame(n_egauges, columns=['Homes'])
    col2 = pd.DataFrame(f_records, columns=['Earliest Record'])
    full_list2 = pd.concat([col1, col2], join='outer', axis=1)
    full_list2.to_csv('full_list[adjusted].csv', sep=',', encoding='utf-8', index=False)
    return final, full_list2


def compare_check():
    full_list = pd.read_csv('full_list[unadjusted].csv', sep=',')
    full_list_adj = pd.read_csv('full_list.csv', sep=',')

    test1 = full_list.sort_values(['Homes'], ascending=[True])
    test2 = full_list_adj.sort_values(['Homes'], ascending=[True])
    test3 = full_list_adj.sort_values(['Earliest Record'], ascending=[True])


    test1.to_csv('flc.csv', sep=',', encoding='utf-8', index=False)
    test2.to_csv('flac.csv', sep=',', encoding='utf-8', index=False)
    print test1
    print test2
    print test3
    compare1 = test1['Earliest Record'].tolist()
    compare2 = test2['Earliest Record'].tolist()
    counter1 = 0
    counter2 = 0
    for n in compare1:
        for w in compare1:
            if n == w:
                counter1 += 1

    for a in compare2:
        for z in compare2:
            if a == z:
                counter2 += 1



    print counter1
    print counter2



# organized_final_df()
# compare_check()



def how_many_same_airport():
    df = pd.read_csv('eGauges-nearest-airport.csv', sep=',')
    ports = df['Airport Code'].tolist()
    nd = finished_ports()
    d ={}
    for x in range(len(ports)):
        if ports[x] not in d:
            d[ports[x]] = 1
        else:
            d[ports[x]] += 1
    for n in range(len(nd)):
        print str(nd[n]) + ' appears: ' + str(d[nd[n]])
    print d


how_many_same_airport()

'''
change everything to be numerical
find max and min and most frequent 5% is good
1000 hours 
STC
get extra data
solar anywhere
which ones are bad for solar?
bar graph with various ranges and amounts per 
minimize % range from most common to output the most points '''