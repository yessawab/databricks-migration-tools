import sys, argparse


def arg_parser():
    parser = argparse.ArgumentParser(
        description="Migrate everything between two databricks workspaces"
    )
    parser.add_argument("--src_name", help="Name of the source workspace")
    parser.add_argument("--src_url", help="Url of the source workspace")
    parser.add_argument("--src_token", help="Token for the source workspace")
    parser.add_argument("--dest_name", help="Name of the destination workspace")
    parser.add_argument("--dest_url", help="Url of the destination workspace")
    parser.add_argument("--dest_token", help="Url of the destination workspace")
    parser.add_argument("--o", help="Overwrite (True/False): Default is False")
    parser.add_argument("--env_name", help="Name of the environment")

    args = parser.parse_args()

    return args
