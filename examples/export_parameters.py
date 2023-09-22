import argparse
import csv
import os
from pathlib import Path

from vaultspeed_sdk.client import Client, UserPasswordAuthentication
from vaultspeed_sdk.system import System


def main(project: str, csv_path: Path):
    auth = UserPasswordAuthentication(api_url=os.environ.get("VS_URL"), username=os.environ.get("VS_USER"),
                                      password=os.environ.get("VS_PASSWORD"))
    client = Client(base_url=os.environ.get("VS_URL"), auth=auth, retries=1, caller="examples")
    system = System(client=client)

    p = system.get_project(project)

    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=",")
        header = ["name", "value", "type", "description"]
        writer.writerow(header)
        for param in p.parameters:
            data = [param.name, param.value, param.type, param.description]
            writer.writerow(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Export Project Parameters",
        description="Exports the parameters from a project to a CSV file."
    )
    parser.add_argument(
        "project",
        help="Name of the Project.",
    )
    parser.add_argument(
        "csv_path",
        help="path of the CSV file to export to.",
        type=Path
    )
    args = parser.parse_args()

    main(args.project, args.csv_path)
