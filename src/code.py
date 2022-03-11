import requests
from typing import List, Dict
import csv


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


# https://data.police.uk/api/outcomes-at-location?date=2021-01&location_id=883498

if __name__ == '__main__':
    forces_list = get_forces_list()
    force_id = forces_list[24]['id'] # metropolitan force id 24
    print(force_id)
    neighbourhoods_list = get_neighbourhoods_list(force_id)

    # print(_sorted_neighborhoods(neighbourhoods_list))
    # filename_neighbourhoods = 'data/neighbourhoods.csv'
    # _export_to_csv(neighbourhoods_list, filename_neighbourhoods)

    neighbourhood_dict = neighbourhoods_list[415] # ['Canonbury', 'E05000369', '415']
    neighbourhood_id = neighbourhood_dict['id'] 
    print(neighbourhood_dict, type(neighbourhood_id))
    