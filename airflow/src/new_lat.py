import json 
import csv
import os
import requests
import re

from typing import Dict

def read_json():
    cwd = os.getcwd()
    print(cwd)
    os.chdir('../../data')
    filename = 'metropolitan_2020-12'
    new_filename = 'new_metropolitan_2020-12'
    print(os.getcwd())
    # print( os.listdir() )

    file = f'{filename}.json'
    with open(file) as jf:
        data = json.load(jf)
    # print(stop_and_searches_data[0].keys())
    new_data = []
    for row in data:

        if row['location'] is not None:
            # print(row['location'])
            row['latitude'] = row['location']['latitude']
            row['longitude'] = row['location']['longitude']
            row['street_id'] = row['location']['street']['id']
            row['street_name'] = row['location']['street']['name']
        else:
            row['latitude'] = None
            row['longitude'] = None
            row['street_id'] = None
            row['street_name'] = None
        new_data.append(row)
    json_to_file(new_data, new_filename)

def json_to_file(json_data, filename: str) -> None:
    with open(f'{filename}.json', 'w') as outfile:
        json.dump(json_data, outfile)
    json_to_csv(filename)

def json_to_csv(path: str) -> None:

    with open(f'{path}.json') as jf:
        data = json.load(jf)
    stop_and_searches_data = data
    print(stop_and_searches_data[0].keys())

    with open(f'{path}.csv', 'w') as csv_file:
        csv_writer = csv.writer(csv_file, quotechar = '"')
        count = 0
        for row in stop_and_searches_data:
            if count == 0:
                header = row.keys()
                csv_writer.writerow(header)
                count += 1
            csv_writer.writerow(row.values())


def get_force_and_neighbourhood(lat: str, long: str):
    ''' https://data.police.uk/docs/method/neighbourhood-locate/ '''
    # https://data.police.uk/api/locate-neighbourhood?q=51.500617,-0.124629
    URL = f'https://data.police.uk/api/locate-neighbourhood?q={lat},{long}'
    print(f'Requesting data from {URL}')
    r = requests.get(URL, timeout=None)
    # print('status code: ', r.status_code)
    try:
        r_dict = r.json()
    except Exception as e:
        print(e)
        return None
    return r_dict['neighbourhood']
    
if __name__ == '__main__':
    # my_str = "{'latitude': '51.624028', 'street': {'id': 985509, 'name': 'On or near Shopping Area'}, 'longitude': '-0.057842'}"
    # latlong_dict = str_to_dict(my_str)
    # # lat, long = latlong_dict['latitude'], latlong_dict['longitude']
    # # print(lat, long)
    # lat, long =  get_lat_long(latlong_dict)
    # print(lat, long)
    read_json()


# def json_to_csv(date: str) -> None:

#     with open(f'{path}.json') as jf:
#         data = json.load(jf)
#     stop_and_searches_data = data
#     print(stop_and_searches_data[0].keys())

#     with open(f'{path}.csv', 'w') as csv_file:
#         csv_writer = csv.writer(csv_file, quotechar = '"')
#         count = 0
#         for row in stop_and_searches_data:
#             if count == 0:
#                 header = row.keys()
#                 csv_writer.writerow(header)
#                 count += 1
#             csv_writer.writerow(row.values())