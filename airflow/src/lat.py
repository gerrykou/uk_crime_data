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
    filename = 'metropolitan_2020-12_new'
    print(os.getcwd())
    # print( os.listdir() )

    file = f'{filename}.csv'
    with open(file) as csvfile:
        row_reader = csv.DictReader(csvfile)
        # for i, row in enumerate(row_reader):
        #     if i > 1 and i < 10:
        #         print(i , row[10])
        #         latlong_dict = str_to_dict(row[10])
        #         print(latlong_dict)
        #         if latlong_dict is not None:
        #             lat, long = latlong_dict['latitude'], latlong_dict['longitude']
        #             print(lat, long)
        for row in row_reader:
            the_string = row['location']
            the_string = the_string.replace("'",'"')
            try:
                latlong_dict = json.loads(the_string)
            except Exception as e:
                print(f"Exception {e} {the_string}")
            
            # lat, long =  get_lat_long(latlong_dict)
            # print(lat, long)
            # # print(row[10])

def str_to_dict(the_string: str) -> Dict:
    '''returns a dictionary from a json like string that uses single quotes'''
    try:
        json.loads(the_string)
    except:
        print(f'An exeption happened {the_string}')
    else :
        the_dict = json.loads(the_string)
        return the_dict
    # return the_dict if the_dict is not None else "null"


# def string_to_acceptable(the_string: str) -> str:
#     # print(the_string)
#     # the_new_string = re.sub("'", "\"", the_string)
#     the_new_string = the_string.replace("'", "\"")
#     return the_new_string

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