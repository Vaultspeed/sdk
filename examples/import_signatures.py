#  import signature config from CSV file
import argparse
import logging
import os
from pathlib import Path
import csv

from vaultspeed_sdk.client import Client, UserPasswordAuthentication
from vaultspeed_sdk.models.util import get_last
from vaultspeed_sdk.system import System


def main(project: str, dv: str, csv_path: Path, dv_release_name: str = None, bv_release_name: str = None):
    # initialise VaultSpeed connection
    logging.basicConfig(level=logging.INFO)
    auth = UserPasswordAuthentication(api_url=os.environ.get("VS_URL"), username=os.environ.get("VS_USER"), password=os.environ.get("VS_PASSWORD"))
    client = Client(base_url=os.environ.get("VS_URL"), auth=auth, retries=1, caller="examples")
    system = System(client=client)

    data_vault = system.get_project(project).get_data_vault(name=dv)

    # get requested releases or the latest one if none are specified
    if dv_release_name:
        dv_release = data_vault.get_release(dv_release_name)
        if not dv_release.locked:
            raise Exception("The selected Data Vault release is not yet locked and thus cannot have an editable Business Vault")
        print(f"Generating for select DV release: {dv_release.name}")
    else:
        locked_dv_releases = [rel for rel in data_vault.releases if rel.locked]
        if not locked_dv_releases:
            raise Exception("No locked Data Vault releases could be found in the selected project")

        dv_release = get_last(locked_dv_releases)
        print(f"Retrieved the last locked DV Release: {dv_release.name}")

    if bv_release_name:
        bv_release = dv_release.get_business_vault_release(bv_release_name)
        if dv_release.locked:
            raise Exception("The selected Business Vault release is already locked and thus cannot be modified")
        print(f"importing for select BV release: {bv_release.name}")
    else:
        unlocked_bv_releases = [rel for rel in dv_release.business_vault_releases if not rel.locked]
        if not unlocked_bv_releases:
            raise Exception("No unlocked Business Vault releases could be found in the selected DV release")
        bv_release = get_last(unlocked_bv_releases)
        print(f"Retrieved the last unlocked BV Release: {bv_release.name}")

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
        description="""
        This script can import signature object and attribute assignments from a CSV.
        As input, provide a project, DV, and optionally a DV and BV release (otherwise the latest is used) as well as the path to 
        a directory containing a object_signatures.csv and attribute_signatures.csv file.
        
        object_signatures.csv should have 2 columns:
        - object_name
        - signature_name
        
        attribute_signatures.csv should have 3 columns:
        - object_name
        - attribute_name
        - signature_name
        
        The script will create any signatures that don't exist yet, and then assign them to the given attributes and object.
        """,
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
             "where the first column is the object name, the second column is the attribute name, "
             "and the third column the signature attribute name.",
        dest="csv_path",
        action="store",
        type=Path
    )
    parser.add_argument(
        "-d", "--dv",
        help="Data Vault release name",
        dest="dv_release_number",
        action="store"
    )
    parser.add_argument(
        "-b", "--bv",
        help="Business Vault release name",
        dest="bv_release_number",
        action="store"
    )
    args = parser.parse_args()

    main(args.project, args.dv, args.csv_path, args.dv_release_name, args.bv_release_name)
