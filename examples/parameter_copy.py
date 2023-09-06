import argparse
import os

from vaultspeed_sdk.client import Client, UserPasswordAuthentication
from vaultspeed_sdk.system import System


def main(project_a: str, source_a: str, project_b: str, source_b):
    auth = UserPasswordAuthentication(api_url=os.environ.get("VS_URL"), username=os.environ.get("VS_USER"),
                                      password=os.environ.get("VS_PASSWORD"))
    client = Client(base_url=os.environ.get("VS_URL"), auth=auth, retries=1, caller="examples")
    system = System(client=client)

    pa = system.get_project(project_a)
    pb = system.get_project(project_b)

    sa = pa.get_source(source_a)
    sb = pb.get_source(source_b)

    param_a = sa.parameters
    param_b = sb.parameters

    for param in param_a:
        param_b[param.name].value = param.value

    param_b.save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="copy source Parameters",
        description="Copies the source parameters from one source to another, the sources can be in different projects."
    )
    parser.add_argument(
        "project_a",
        help="Name of the Project to copy from",
    )
    parser.add_argument(
        "source_a",
        help="Name of the Source to copy from"
    )
    parser.add_argument(
        "project_b",
        help="Name of the Project to copy to",
    )
    parser.add_argument(
        "source_b",
        help="Name of the Source to copy to"
    )
    args = parser.parse_args()

    main(args.project_a, args.source_a, args.project_b, args.source_b)
