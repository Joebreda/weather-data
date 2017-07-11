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



df = pd.read_csv('eGauges-nearest-airport.csv', sep=',')
homes = df['eGauge'].tolist()
for egauge in homes:
    full_list = pd.read_csv('full_list.csv', sep=',')
    homes2 = full_list['Homes'].tolist()
    records = full_list['Earliest Record'].tolist()
    index_number = homes2.index(egauge)
    end_date_timestamp = records[index_number]
    print end_date_timestamp
    end_date = datetime.date.fromtimestamp(int(end_date_timestamp)).strftime('%Y-%m-%d')
    print end_date
