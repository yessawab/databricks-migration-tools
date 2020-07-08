import logging

logging.getLogger().setLevel(logging.INFO)


from .common.databricks_cli.cli import configure_profile
from .common.command_helper import arg_parser
from .workspace import migrate_workspace
from .jobs import migrate_jobs
from .clusters import migrate_clusters
from .libraries import migrate_libraries


def main(argv):

    try:
        logging.info("Configuring the source and destination profiles")
        source_profile = configure_profile(argv.src_name, argv.src_url, argv.src_token)
        destination_profile = configure_profile(
            argv.dest_name, argv.dest_url, argv.dest_token
        )
    except BaseException as e:
        logging.error("Failed to create new profiles")
        logging.exception(e)
        raise

    try:
        logging.info("Step 1: Workspace content migration")
        migrate_workspace(source_profile,destination_profile,argv.o)
    except BaseException as e:
        logging.error("Failed to migrate workspace")
        logging.exception(e)
        raise

    try:
        logging.info("Step 2: Clusters migration")
        cluster_old_new_mappings = migrate_clusters(
            source_profile,
            destination_profile,
            argv.dest_url,
            argv.dest_token,
        )
    except BaseException as e:
        logging.error("Failed to migrate clusters")
        logging.exception(e)
        raise

    try:
        logging.info("Step 3: Jobs migration")
        migrate_jobs(
            source_profile, destination_profile, cluster_old_new_mappings
        )
    except BaseException as e:
        logging.error("Failed to migrate jobs")
        logging.exception(e)
        raise

    try:
        logging.info("Step 3: Libraries migration")
        migrate_libraries(source_profile,destination_profile,cluster_old_new_mappings)
    except BaseException as e:
        logging.error("Failed to migrate libraries")
        logging.exception(e)
        raise


if __name__ == "__main__":
    args = arg_parser()
    main(args)
