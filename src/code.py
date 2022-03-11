import requests
from typing import List, Dict



def get_forces_list() -> List[Dict[str, str]]:
    r = requests.get('https://data.police.uk/api/forces')
    forces = r.json()
    # for i in range(len(forces)):
    #     print(i, forces[i])
    return forces


if __name__ == '__main__':
    forces_list = get_forces_list()
    metropolitan_id = forces_list[24]['id']
    print(metropolitan_id)