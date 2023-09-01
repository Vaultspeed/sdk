#  import signature config from CSV file
import argparse
import logging
import os
from pathlib import Path
import csv

from vaultspeed_sdk.client import Client, UserPasswordAuthentication
from vaultspeed_sdk.system import System


def main(project: str, dv: str, csv_path: Path, dv_release_number: str = None, bv_release_number: str = None):
    # initialise VaultSpeed connection
    logging.basicConfig(level=logging.INFO)
    auth = UserPasswordAuthentication(api_url=os.environ.get("VS_URL"), username=os.environ.get("VS_USER"), password=os.environ.get("VS_PASSWORD"))
    client = Client(base_url=os.environ.get("VS_URL"), auth=auth, retries=1, caller="examples")
    system = System(client=client)

    data_vault = system.get_project(project).get_data_vault(name=dv)

    # get requested releases or the latest one if none are specified
    if dv_release_number is not None:
        dv_release = data_vault.get_release(dv_release_number)
    else:
        dv_release = sorted(data_vault.releases, key=lambda x: x.date, reverse=True)[0]

    if bv_release_number is not None:
        bv_release = dv_release.get_business_vault_release(str(bv_release_number))
    else:
        bv_release = sorted(dv_release.business_vault_releases, key=lambda x: x.release_date, reverse=True)[0]

    with open(csv_path / "object_signatures.csv") as csvfile:
        object_signatures = csv.reader(csvfile, delimiter=",")
        for row in object_signatures:
            object_name = row[0]
            signature_name = row[1]

            signature = bv_release.get_signature_object(signature_name, check=False) or bv_release.create_signature_object(name=signature_name)
            bv_release.objects[object_name].add_signature(signature)

    with open(csv_path / "attribute_signatures.csv") as csvfile:
        object_signatures = csv.reader(csvfile, delimiter=",")
        for row in object_signatures:
            object_name = row[0]
            attribute_name = row[1]
            signature_name = row[2]

            signature = bv_release.get_signature_attribute(signature_name, check=False) or bv_release.create_signature_attribute(name=signature_name)
            bv_release.objects[object_name].attributes[attribute_name].add_signature(signature)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="import signatures",
        description="Upload object and attribute signatures from a csv file",
        epilog=""
    )
    parser.add_argument(
        "project",
        help="Project name",
    )
    parser.add_argument(
        "dv",
        help="Data Vault name"
    )
    parser.add_argument(
        "CSV path",
        help="path to the folder containing the CSV files"
             "The object signatures should be stored in a CSV file called 'object_signatures.csv', "
             "where the first column is the object name, and the second column the signature object name."
             "The object signatures should be stored in a CSV file called 'attribute_signatures.csv', "
             "where the first column is the object name, the second column is the attribute name, and the third column the signature attribute name.",
        dest="csv_path",
        action="store",
        type=Path
    )
    parser.add_argument(
        "-d", "--dv",
        help="Data Vault release number",
        dest="dv_release_number",
        action="store"
    )
    parser.add_argument(
        "-b", "--bv",
        help="Business Vault release number",
        dest="bv_release_number",
        action="store"
    )
    args = parser.parse_args()

    main(args.project, args.dv, args.csv_path, args.dv_release_number, args.bv_release_number)
