from subprocess import call, check_output
import json, os
from .common.databricks_cli.cli import (
    list_libraries,
    install_library
)
import logging


def migrate_libraries(source_profile, dest_profile, cluster_old_new_mappings):

    for cluster_id in list(cluster_old_new_mappings.values()):    
        libraries_list = list_libraries(cluster_id,source_profile)

        if not libraries_list:
            logging.info("No libraries found in the primary site")
        else:
            logging.info(str(len(libraries_list)) + " libraries found in the primary site for cluster id: "+ cluster_id)
            for library in libraries_list:
                logging.info(library)
                if library["status"] in  ["INSTALLED", "PENDING"] :
                    install_status = install_library(cluster_id, source_profile, dest_profile, library)
                    logging.info(install_status)
            logging.info("Libraries installed successfully for cluster {} ".format(cluster_id))