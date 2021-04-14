import json
import os

from pystac import Item


def convert(json_path: str):
    try:
        with open(json_path) as f:
            item = Item.from_dict(json.load(f))
            print(item)
            print(item.to_dict())
    except Exception as error:
        print(error)


if __name__ == "__main__":
    json_file = os.path.join(
        "C:/Users/cario/work/deafrica-airflow/scripts/", "test.json"
    )
    convert(json_file)
