import json 
import csv
import os
import requests
import re

from typing import Dict

def read_csv():
    cwd = os.getcwd()
    print(cwd)
    os.chdir('../../data')
    filename = 'metropolitan_2020-12'
    print(os.getcwd())
    # print( os.listdir() )

    file = f'{filename}.csv'
    with open(file, newline='') as csvfile:
        row_reader = csv.reader(csvfile, delimiter=',')
        for i, row in enumerate(row_reader):
            if i > 1 and i < 10:
                # print(i , row[10])
                latlong_dict = str_to_dict(row[10])
                print(latlong_dict)
                lat, long = latlong_dict['latitude'], latlong_dict['longitude']
                print(lat, long)
            # lat, long =  get_lat_long(latlong_dict)
            # print(lat, long)
            # # print(row[10])

def str_to_dict(the_string: str) -> Dict:
    '''returns a dictionary from a json like string that uses single quotes'''
    json_accectable_string = string_to_acceptable(the_string)
    # print(json_accectable_string)
    try:
        the_dict = json.loads(json_accectable_string)
        # print(the_dict)
        return the_dict
    except:
        print(f'An exception occurred {json_accectable_string}')
        pass
    # return the_dict

def string_to_acceptable(the_string: str) -> str:
    # print(the_string)
    # the_new_string = re.sub("'", "\"", the_string)
    the_new_string = the_string.replace("'", "\"")
    return the_new_string

def get_lat_long(the_dict: Dict):
    lat, long = the_dict['latitude'], the_dict['longitude']
    return lat, long

def get_force_and_neighbourhood(lat: str, long: str):
    ''' https://data.police.uk/docs/method/neighbourhood-locate/ '''
    # https://data.police.uk/api/locate-neighbourhood?q=51.500617,-0.124629
    URL = f'https://data.police.uk/api/locate-neighbourhood?q={lat},{long}'
    print(f'Requesting data from {URL}')
    r = requests.get(URL, timeout=None)
    print('status code: ', r.status_code)
    stop_and_searches = r.json()
    # stop_and_searches = r.status_code
    return stop_and_searches


if __name__ == '__main__':
    # my_str = "{'latitude': '51.624028', 'street': {'id': 985509, 'name': 'On or near Shopping Area'}, 'longitude': '-0.057842'}"
    # latlong_dict = str_to_dict(my_str)
    # # lat, long = latlong_dict['latitude'], latlong_dict['longitude']
    # # print(lat, long)
    # lat, long =  get_lat_long(latlong_dict)
    # print(lat, long)
    read_csv()