import argparse
import logging
import os
from datetime import datetime, timezone

from vaultspeed_sdk.client import Client, UserPasswordAuthentication
from vaultspeed_sdk.fmc import FlowTypes
from vaultspeed_sdk.models.metadata.load_type import LoadTypes
from vaultspeed_sdk.system import System

"""
This script can create FMC flows for a Data Vault, this is useful when you have a lot of sources, 
so that you don't have to create an init and incr flow for each of them.
"""


def main(project_name: str, data_vault_name: str):
    """
     Preparation
    """
    logging.basicConfig(level=logging.INFO)
    auth = UserPasswordAuthentication(api_url=os.environ.get("VS_URL"), username=os.environ.get("VS_USER"),
                                      password=os.environ.get("VS_PASSWORD"))
    client = Client(base_url=os.environ.get("VS_URL"), auth=auth, retries=1, caller="examples")
    system = System(client=client)
    project = system.get_project(project_name)
    data_vault = project.get_data_vault(name=data_vault_name)

    for flow in data_vault.fmc_flows:
        data_vault.delete_fmc_flow(flow)

    """
     create FMC flows
    """
    for load_type in [LoadTypes.INIT, LoadTypes.INCR]:
        for source in project.sources:
            if source.build_flag:
                data_vault.create_fmc_flow(
                    name=f"{source.name}_{load_type.value.lower()}",
                    description=f"{source.name}_{load_type.value.lower()}",
                    start_date=datetime.now(timezone.utc).replace(minute=0, hour=0, second=0, microsecond=0),
                    concurrency=4,
                    flow_type=FlowTypes.FL,
                    load_type=load_type,
                    group_tasks=False,
                    dv_connection_name="sf",
                    source=source,
                    schedule_interval="\"@hourly\"",
                    src_connection_name="src"
                )

        data_vault.create_fmc_flow(
            name=f"{data_vault.code}_bv_{load_type.value.lower()}",
            description=f"{data_vault.code}_bv_{load_type.value.lower()}",
            start_date=datetime.now(timezone.utc).replace(minute=0, hour=0, second=0, microsecond=0),
            concurrency=4,
            flow_type=FlowTypes.BV,
            load_type=load_type,
            group_tasks=False,
            dv_connection_name="dv",
            schedule_interval="timedelta(hours=1)"
        )

    print(data_vault.fmc_flows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Create FMC Flows",
        description="""
        This script can create FMC flows for a Data Vault, this is useful when you have a lot of sources, 
        so that you don't have to create an init and incr flow for each of them.
        """
    )
    parser.add_argument(
        "project",
        help="Name of the Project"
    )
    parser.add_argument(
        "data_vault",
        help="Name of the Data Vault"
    )
    args = parser.parse_args()

    main(args.project, args.data_vault)
