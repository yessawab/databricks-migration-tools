import logging

from subprocess import call
from .common.databricks_cli.cli import list_users, export_dir, import_dir


def migrate_workspace(source_profile, dest_profile, overwrite="False"):
    user_list = list_users(source_profile)

    for user in user_list:
        call("mkdir -p " + user, shell=True)

        try:
            export_exit_status = export_dir(source_profile, user)
        except BaseException as e:
            logging.error(
                "Failed to export content for user {} from: {} ".format(
                    user, source_profile
                )
            )
            logging.exception(e)
            call("rm -r " + user, shell=True)
            raise

        if export_exit_status == 0:
            logging.info(
                "Successfully exported workspace content for user {} from: {} ".format(
                    user, source_profile
                )
            )
            try:
                import_exit_status = import_dir(dest_profile, user, overwrite)
            except BaseException as e:
                logging.error("Failed to import content from: " + dest_profile)
                logging.exception(e)
                call("rm -r " + user, shell=True)
                raise

            if import_exit_status == 0:
                logging.info(
                    "Successfully imported workspace content for user {} to: {} ".format(
                        user, dest_profile
                    )
                )

            else:
                logging.info(
                    "Failed to import workspace content for user {} to: {}".format(
                        user, dest_profile
                    )
                )
                logging.info("Step 1 failed")
                call("rm -r " + user, shell=True)
                break

            call("rm -r " + user, shell=True)
    logging.info("Step 1 finished successfully")
