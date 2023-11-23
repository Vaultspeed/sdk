import argparse
import logging
import os
from pathlib import Path

from vaultspeed_sdk.client import Client, UserPasswordAuthentication
from vaultspeed_sdk.models.metadata.etl_generation_type import EtlGenerationTypes
from vaultspeed_sdk.models.util import get_last
from vaultspeed_sdk.system import System

"""
This script can upload a new version of a template and generate example code for it.
"""

def main(project: str, dv: str, template_path: Path, object_name: str, generation_type: EtlGenerationTypes,
         dv_release_name: str = None, bv_release_name: str = None, deploy_link: str = None):
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

    template_file_name = template_path.stem

    if template_file_name.endswith("_ddl"):
        template = bv_release.get_template(template_file_name.rstrip("_ddl"), check=False)
        template.template_ddl = template_path.read_text()
    else:
        template = bv_release.get_template(template_file_name.rstrip("_etl"), check=False)
        template.template_etl = template_path.read_text()

    example_code, generations = system.generate_template_example(bv_release=bv_release, template=template,
                                                                 base_object=template.dependencies[object_name],
                                                                 etl_type=generation_type)
    print(example_code)

    with Path(f"{template.name}_result_{object_name}").open(mode="w") as f:
        f.write(example_code)

    if deploy_link:
        # deploy the generated files
        db_link = system.get_database_link(deploy_link)
        for gen in generations:
            if gen.can_autodeploy:
                gen.deploy_to_target(db_link=db_link)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="template test",
        description="""
        This script uploads a new version of a template and executes a test generation for a specific object.
        It also contains an explanation for how this can be integrated into VSCode, so that it can quickly be tested 
        while editing a template file there.
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
        "template",
        help="path to the template file",
        dest="template_path",
        action="store",
        type=Path
    )
    parser.add_argument(
        "object",
        help="name of the object to generate for",
        dest="object_name",
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

    main(args.project, args.dv, args.template_path, args.object_name, EtlGenerationTypes[args.generation_type], args.dv_release_name,
         args.bv_release_name, args.deploy_link)


"""
This script can be called from VS Code in order to quickly test a template you are working on, create the following launch configuration in launch.json:

// Use IntelliSense to learn about possible attributes.
// Hover to view descriptions of existing attributes.
// For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
{
    "configurations": [
        {
            "name": "template",
            "type": "python",
            "request": "launch",
            "program": "<path to script file>/update_template_and_test.py",
            "args": ["${input:project}", "${input:dv}", "${input:generation_type}", "${file}", "${input:object}"],
            "console": "integratedTerminal",
            "justMyCode": true
        }
    ],
    "inputs": [
        {
            "id": "project",
            "type": "promptString",
            "description": "VaultSpeed Project"
        },
        {
            "id": "dv",
            "type": "promptString",
            "description": "VaultSpeed Data Vault"
        },
        {
            "id": "generation_type",
            "type": "pickString",
            "description": "ETL technology to use for generation",
            "options": [
                "SNOWFLAKEDBT",
                "ORACLESQL",
                "ORACLEGROOVY",
                "POSTGRESQL",
                "SQLSERVERSQL",
                "GREENPLUM",
                "POSTGRESJOBSCRIPT",
                "ORACLEJOBSCRIPT",
                "GREENPLUMJOBSCRIPT",
                "SQLSERVERGROOVY",
                "SNOWFLAKESQL",
                "BIGQUERYSQL",
                "SQLSERVERJOBSCRIPT",
                "AZUREDWHSQL",
                "APACHESPARK",
                "DATABRICKSSQL",
                "SNOWFLAKEMATILLION",
                "AZUREDWHMATILLION",
                "SNOWFLAKEJOBSCRIPT",
                "SINGLESTORESQL",
                "DATABRICKSMATILLION"
            ]
        },
        {
            "id": "object",
            "type": "promptString",
            "description": "name of the object to generate code for"
        }
    ]
}
"""
