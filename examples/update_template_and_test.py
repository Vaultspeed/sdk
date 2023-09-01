# upload a new version of a template and generate example code for it
import argparse
import logging
import os
from pathlib import Path

from vaultspeed_sdk.client import Client, UserPasswordAuthentication
from vaultspeed_sdk.models.metadata.etl_generation_type import EtlGenerationTypes
from vaultspeed_sdk.system import System


def main(project: str, dv: str, template_path: Path, object_name: str, generation_type: EtlGenerationTypes,
         dv_release_number: str = None, bv_release_number: str = None, deploy_link: str = None):
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

    template_file_name = template_path.stem

    if template_file_name.endswith("_ddl"):
        template = bv_release.get_template(template_file_name.rstrip("_ddl"), check=False)
        template.template_ddl = template_path.read_text()
    else:
        template = bv_release.get_template(template_file_name.rstrip("_etl"), check=False)
        template.template_etl = template_path.read_text()

    example_code, generations = system.generate_template_example(bv_release=bv_release, template=template, base_object=template.dependencies[object_name],
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
        description="Update a template and test it",
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

    main(args.project, args.dv, args.template_path, args.object_name, EtlGenerationTypes[args.generation_type], args.dv_release_number, args.bv_release_number,
         args.deploy_link)


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
