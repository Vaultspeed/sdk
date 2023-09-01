import argparse
import logging
import os
from pathlib import Path
from typing import List

from exceptions.internal_server_error import InternalServerError
from models.util import get_last
from vaultspeed_sdk.client import Client, UserPasswordAuthentication
from vaultspeed_sdk.models.base_generation import Generation
from vaultspeed_sdk.models.metadata.etl_generation_type import EtlGenerationTypes
from vaultspeed_sdk.models.metadata.generation_type import GenerationTypes
from vaultspeed_sdk.models.metadata.load_type import LoadTypes
from vaultspeed_sdk.system import System


def generate_code(system: System, project_name: str, dv_name: str, generation_type: EtlGenerationTypes, dv_release_name: str = None,
                  bv_release_name: str = None, force_generation: bool = False) -> List[Generation]:
    data_vault = system.get_project(project_name).get_data_vault(name=dv_name)

    # get requested releases or the latest one if none are specified
    if dv_release_name:
        dv_release = data_vault.get_release(dv_release_name)
    else:
        dv_release = get_last(data_vault.releases)

    if bv_release_name:
        bv_release = dv_release.get_business_vault_release(bv_release_name)
    else:
        bv_release = get_last(dv_release.business_vault_releases)

    # Check if there are production releases. If there are, then we will generate code using the delta generation.
    # We only look at releases which occurred before the selected release in case the selected release is a production release.
    prod_releases = [rel for rel in data_vault.releases if not rel.prototype_flag and rel.date < dv_release.date]

    all_generations = system.generations()

    generations: List[Generation] = []
    etl_generation_id: int

    if not prod_releases:
        # if there are no production releases before the selected release, then generate hte full DDL ETL
        ddl_gen: Generation = None
        etl_gen: Generation = None

        if not force_generation:
            # check if there wa already a generation done before for the selected release that we can reuse
            ddl_gen = get_last([gen for gen in all_generations if
                                gen.gen_type == GenerationTypes.DDL and gen.bv_identifier == bv_release.identifier])
            etl_gen = get_last([gen for gen in all_generations if
                                gen.gen_type == GenerationTypes.ETL and gen.bv_identifier == bv_release.identifier])

        if not ddl_gen:
            ddl_gen = system.generate_ddl(
                bv_release=bv_release,
                etl_generation_type=generation_type,
                load_type=LoadTypes.ALL
            )[0]
        if not etl_gen:
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
        prev_bv_release = get_last(last_prod_release.business_vault_releases)
        delta_gen: Generation = None

        if not force_generation:
            # check if there was already a generation done before for the selected release that we can reuse
            delta_gen = get_last([gen for gen in all_generations if
                                  gen.gen_type == GenerationTypes.DELTA and gen.bv_identifier == bv_release.identifier])

        if not delta_gen:
            delta_gen = system.generate_delta(
                old_bv_release=prev_bv_release,
                new_bv_release=bv_release,
                etl_generation_type=generation_type
            )[0]

        generations.append(delta_gen)
        etl_generation_id = delta_gen.identifier

    # Generate FMC code
    for flow in data_vault.fmc_flows:
        # check if there is a previous generation
        fmc_generations = get_last([gen for gen in flow.generations if gen.etl_generation_id == etl_generation_id])

        if fmc_generations and not force_generation:
            # reuse existing generation
            fmc_gen = get_last([gen for gen in all_generations if
                                gen.gen_type == GenerationTypes.FMC and gen.filename == fmc_generations.file_name])
            generations.append(fmc_gen)

        else:
            # generate FMC code for the ETL generation
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
        description="Generate code for a certain data vault",
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
    client = Client(base_url=os.environ.get("VS_URL"), auth=auth, retries=1, caller="examples")
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
