import logging
from datetime import datetime, timezone

from vaultspeed_sdk.client import Client, UserPasswordAuthentication
from vaultspeed_sdk.fmc import FlowTypes
from vaultspeed_sdk.models.metadata.load_type import LoadTypes
from vaultspeed_sdk.system import System

import env as env

"""
 Preparation
"""
logging.basicConfig(level=logging.INFO)
authentication = UserPasswordAuthentication(api_url=env.base_url, username=env.user_name, password=env.password)
client = Client(base_url=env.base_url, auth=authentication, retries=1, caller="examples")
system = System(client=client)
project = system.get_project("moto")
data_vault = project.get_data_vault(name="moto_no_af")

for flow in data_vault.fmc_flows:
    data_vault.delete_fmc_flow(flow)

"""
 create FMC flows
"""
for load_type in [LoadTypes.INIT, LoadTypes.INCR]:
    for source in project.sources:
        if source.build_flag and source.name in ("moto_sales", "moto_mktg"):
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
                src_connection_name="src",
                etl_connection_name="dbt"
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
