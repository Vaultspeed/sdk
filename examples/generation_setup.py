import argparse
import logging
import os
from pathlib import Path
from typing import List

from vaultspeed_sdk.exceptions.internal_server_error import InternalServerError
from vaultspeed_sdk.client import Client, UserPasswordAuthentication, TaskConfig
from vaultspeed_sdk.models.base_generation import Generation
from vaultspeed_sdk.models.metadata.etl_generation_type import EtlGenerationTypes
from vaultspeed_sdk.models.metadata.generation_type import GenerationTypes
from vaultspeed_sdk.models.metadata.load_type import LoadTypes
from vaultspeed_sdk.models.util import get_last
from vaultspeed_sdk.system import System


def generate_code(system: System, project_name: str, dv_name: str, generation_type: EtlGenerationTypes, dv_release_name: str = None,
                  bv_release_name: str = None, force_generation: bool = False) -> List[Generation]:
    data_vault = system.get_project(project_name).get_data_vault(name=dv_name)

    # get requested releases or the latest one if none are specified
    if dv_release_name:
        dv_release = data_vault.get_release(dv_release_name)
        if not dv_release.locked:
            raise Exception("The selected Data Vault release is not yet locked and thus cannot be used to generate code")
        print(f"Generating for select DV release: {dv_release.name}")
    else:
        locked_dv_releases = [rel for rel in data_vault.releases if rel.locked]
        if not locked_dv_releases:
            raise Exception("No locked Data Vault releases could be found in the selected project")

        dv_release = get_last(locked_dv_releases)
        print(f"Retrieved the last locked DV Release: {dv_release.name}")

    if bv_release_name:
        bv_release = dv_release.get_business_vault_release(bv_release_name)
        if not dv_release.locked:
            raise Exception("The selected Business Vault release is not yet locked and thus cannot be used to generate code")
        print(f"Generating for select BV release: {bv_release.name}")
    else:
        locked_bv_releases = [rel for rel in dv_release.business_vault_releases if rel.locked]
        if not locked_bv_releases:
            raise Exception("No locked Business Vault releases could be found in the selected DV release")
        bv_release = get_last(locked_bv_releases)
        print(f"Retrieved the last locked BV Release: {bv_release.name}")

    # Check if there are production releases. If there are, then we will generate code using the delta generation.
    # We only look at releases which occurred before the selected release in case the selected release is a production release.
    prod_releases = [rel for rel in data_vault.releases if not rel.prototype_flag and rel.date < dv_release.date]

    all_generations = system.generations()

    generations: List[Generation] = []
    etl_generation_id: int

    if not prod_releases:
        # If there are no production releases before the selected release, then generate the full DDL ETL.
        print("No previous production releases found, doing a Full generation")
        ddl_gen: Generation = None
        etl_gen: Generation = None

        if not force_generation:
            ddl_gen = get_last([gen for gen in all_generations if
                                gen.gen_type == GenerationTypes.DDL and gen.bv_identifier == bv_release.identifier])
            etl_gen = get_last([gen for gen in all_generations if
                                gen.gen_type == GenerationTypes.ETL and gen.bv_identifier == bv_release.identifier])

        if not ddl_gen:
            print("Generating new DDL code")
            ddl_gen = system.generate_ddl(
                bv_release=bv_release,
                etl_generation_type=generation_type,
                load_type=LoadTypes.ALL
            )[0]
        if not etl_gen:
            print("Generating new ETL code")
            etl_gen = system.generate_etl(
                bv_release=bv_release,
                etl_generation_type=generation_type,
                load_type=LoadTypes.ALL
            )[0]

        generations.append(ddl_gen)
        generations.append(etl_gen)
        etl_generation_id = etl_gen.identifier

    else:
        last_prod_release = get_last(prod_releases)
        print(f"Production release found: {last_prod_release.name}")
        prev_bv_release = get_last(last_prod_release.business_vault_releases)
        delta_gen: Generation = None

        if not force_generation:
            # check if there was already a generation done before for the selected release that we can reuse
            delta_gen = get_last([gen for gen in all_generations if
                                  gen.gen_type == GenerationTypes.DELTA and gen.bv_identifier == bv_release.identifier])

        if not delta_gen:
            print("Generating new DELTA code")
            delta_gen = system.generate_delta(
                old_bv_release=prev_bv_release,
                new_bv_release=bv_release,
                etl_generation_type=generation_type
            )[0]

        generations.append(delta_gen)
        etl_generation_id = delta_gen.identifier

    # Generate FMC code
    for flow in data_vault.fmc_flows:
        print(f"Checking generations for Flow {flow.name}")
        # check if there is a previous generation
        fmc_generations = get_last([gen for gen in flow.generations if gen.etl_generation_id == etl_generation_id])

        if fmc_generations and not force_generation:
            # reuse existing generation
            print("Found an existing FMC generation")
            fmc_gen = get_last([gen for gen in all_generations if
                                gen.gen_type == GenerationTypes.FMC and gen.filename == fmc_generations.file_name])
            generations.append(fmc_gen)

        else:
            # generate FMC code for the ETL generation
            print("Generating new FMC code")
            etl_generations = [gen for gen in flow.etl_generations if gen.generation_id == etl_generation_id]
            if etl_generations:
                try:
                    fmc_generation = flow.generate(get_last(etl_generations))
                    generations.append(fmc_generation)
                except InternalServerError as e:
                    # If a flow is empty, and thus there is nothing to generate, then it raises an internal server error.
                    print(e)
            else:
                print("No valid ETL generations where found for this FMC workflow")

    return generations


def main():
    parser = argparse.ArgumentParser(
        prog="Generate",
        description="""
            This script can generate and deploy all the code in 1 go (DDL + ETL + FMC).
            You have to pass a project, a data vault and an ETL language.
            Optionally you can specify a DV and/or BV release, if no releases are specified, then the last one will be used.
            The other options are to provide a path, this will cause the code to be downloaded to that path after generation, and a link, 
            if a link is specified, then it will be used to deploy the generated code via the agent.
            
            If there is a production release, then the script will use delta generations starting from the latest production release.
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
        "generation_type",
        help="ETL generation type",
        type=str,
        choices=[i.name for i in EtlGenerationTypes]
    )
    parser.add_argument(
        "-f", "--force",
        help="Generate new code even when there is already a generation available for the selected release",
        dest="force_generation",
        action="store_true",
        default=False
    )
    parser.add_argument(
        "-p", "--path",
        help="Directory where the generated code will be stored",
        dest="code_target_path",
        action="store",
        type=Path
    )
    parser.add_argument(
        "-l", "--deploy", "--link",
        help="The name of the link that should be used to deploy the generated code",
        dest="deploy_link",
        action="store"
    )
    parser.add_argument(
        "-d", "--dv",
        help="Data Vault release name",
        dest="dv_release",
        action="store"
    )
    parser.add_argument(
        "-b", "--bv",
        help="Business Vault release name",
        dest="bv_release",
        action="store"
    )
    args = parser.parse_args()

    # initialise VaultSpeed connection
    logging.basicConfig(level=logging.INFO)
    auth = UserPasswordAuthentication(api_url=os.environ.get("VS_URL"), username=os.environ.get("VS_USER"),
                                      password=os.environ.get("VS_PASSWORD"))
    client = Client(base_url=os.environ.get("VS_URL"), auth=auth, retries=1, caller="examples",
                    task_config=TaskConfig(polling_interval=10, timeout=0, queue_timeout=600, show_progress=True))
    system = System(client=client)

    generations = generate_code(system=system, project_name=args.project, dv_name=args.dv, generation_type=args.generation_type,
                                dv_release_name=args.dv_release, bv_release_name=args.bv_release, force_generation=args.force_generation)

    if args.code_target_path:
        # retrieve the generated files and store them locally
        for gen in generations:
            gen.download_files_to(path=args.code_target_path, keep_zip=False)

    if args.deploy_link:
        # deploy the generated files
        db_link = system.get_database_link(args.deploy_link)
        for gen in generations:
            if gen.can_autodeploy:
                gen.deploy_to_target(db_link=db_link)


if __name__ == "__main__":
    main()
