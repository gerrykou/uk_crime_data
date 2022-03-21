import requests
import functools
from typing import List, Dict
import csv, json


def get_forces_list() -> List[Dict[str, str]]:
    r = requests.get('https://data.police.uk/api/forces')
    forces = r.json()
    # for i in range(len(forces)):
    #     print(i, forces[i])
    return forces

def get_neighbourhoods_list(force_id: str) -> List[Dict[str, str]]:
    r = requests.get(f'https://data.police.uk/api/{force_id}/neighbourhoods')
    neighbourhoods = r.json()
    # for i in range(len(neighbourhoods)):
    #     print(i, neighbourhoods[i])
    return neighbourhoods

def _sorted_neighborhoods(neighbourhoods_list: List[Dict[str, str]]) -> List[str]:
    neighbourhoods_names_list = [f"{v['name']};{v['id']};{i}" for i, v in enumerate(neighbourhoods_list)]
    neighbourhoods_names_list.sort()
    return neighbourhoods_names_list

def _export_to_csv(neighbourhoods_list: List, filename: str) -> None:
    with open(filename , mode="w") as csv_file:
        fieldnames = ["id", "name", "list_index"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for n in _sorted_neighborhoods(neighbourhoods_list):
            i = n.split(';')
            print(i)
            writer.writerow({ "id" : i[0] , "name": i[1], "list_index": i[2]})
    return None

def get_outcomes_at_location(location_id: str):
    r = requests.get(f'https://data.police.uk/api/{force_id}/neighbourhoods')
    neighbourhoods = r.json()
    # for i in range(len(neighbourhoods)):
    #     print(i, neighbourhoods[i])
    return neighbourhoods

def stop_and_searches_by_force(force_id: str, date: str) -> Dict:
    '''https://data.police.uk/docs/method/stops-force/ '''
    # https://data.police.uk/api/stops-force?force=metropolitan&date=2021-01
    URL = f'https://data.police.uk/api/stops-force?force={force_id}&date={date}'
    print(f'Requesting data from {URL}')
    r = requests.get(URL, timeout=None)
    print('status code: ', r.status_code)
    stop_and_searches = r.json()
    # stop_and_searches = r.status_code
    return stop_and_searches

def json_to_file(json_data, filename: str):
    with open(f'data/{filename}.json', 'w') as outfile:
        json.dump(json_data, outfile)

def json_to_csv(filename: str):
    with open(f'{filename}.json') as jf:
        data = json.load(jf)
    police_data = data
    print(police_data[0].keys())

    with open(f'{filename}.csv', 'w') as csv_file:
        csv_writer = csv.writer(csv_file)
        
        count = 0
        
        for row in police_data:
            if count == 0:
                header = row.keys()
                csv_writer.writerow(header)
                count += 1
        
            csv_writer.writerow(row.values())


#https://data.police.uk/api/metropolitan/E05000564

# https://data.police.uk/api/outcomes-at-location?date=2021-01&location_id=883498

if __name__ == '__main__':
    forces_list = get_forces_list()
    force_id = forces_list[24]['id'] # metropolitan force id 24
    print(force_id)
    neighbourhoods_list = get_neighbourhoods_list(force_id)
    # print(neighbourhoods_list)

    # print(_sorted_neighborhoods(neighbourhoods_list))
    # filename_neighbourhoods = 'data/neighbourhoods.csv'
    # _export_to_csv(neighbourhoods_list, filename_neighbourhoods)

    neighbourhood_dict = dict()
    for i in neighbourhoods_list:
        neighbourhood_dict[i['id']] = i['name']
    # print(neighbourhood_dict)

    # neighbourhood_dict = neighbourhoods_list[415] # ['Canonbury', 'E05000369', '415']
    # neighbourhood_id = neighbourhood_dict['id'] 
    # print(neighbourhood_dict, type(neighbourhood_id))

    date = '2020-12'
    # response_data = stop_and_searches_by_force(force_id, date)

    #https://data.police.uk/api/stops-force?force=avon-and-somerset&date=2020-01

    # filename = f'{force_id}_{date}'
    # json_to_file(response_data, filename)

    json_file = f'{force_id}_{date}'
    
    json_to_csv(f'data/{json_file}')
    