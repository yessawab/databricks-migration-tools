from subprocess import call, check_output
import json, os
from .common.databricks_cli.cli import (
    list_jobs,
    get_job,
    create_job,
    check_if_job_exists,
)
import logging


def migrate_jobs(source_profile, dest_profile, cluster_old_new_mappings):

    jobs_list = list_jobs(source_profile)

    if not jobs_list:
        logging.info("No jobs found in the primary site")
    else:
        logging.info(str(len(jobs_list)) + " jobs found in the primary site")
        jobs_list_ids = []
        for jobs in jobs_list:
            jobs_list_ids.append((jobs.split(None, 1)[0].decode()))

        i = 0
        for job_id in jobs_list_ids:
            i += 1

            job_conf = get_job(source_profile, job_id)

            job_conf_json = json.loads(job_conf)
            job_settings_json = job_conf_json["settings"]
            job_name = job_settings_json["name"]

            if check_if_job_exists(dest_profile, job_name):
                logging.warn(
                    "Skipping job {} as it already exists".format(job_name)
                )
                continue

            logging.info(
                "Migrating job "
                + "*"
                + job_name
                + "* "
                + str(i)
                + "/"
                + str(len(jobs_list_ids))
                + " : "
                + job_id
            )

            job_settings_json.pop("schedule", None)

            if "existing_cluster_id" in job_settings_json:
                if job_settings_json["existing_cluster_id"] in cluster_old_new_mappings:
                    logging.info("Mapping available for old cluster id")
                    job_settings_json["existing_cluster_id"] = cluster_old_new_mappings[
                        job_settings_json["existing_cluster_id"]
                    ]
                else:
                    logging.error(
                        "Mapping not available for old cluster id "
                        + job_settings_json["existing_cluster_id"]
                    )
                    continue
            else:
                logging.info("Job {} doesn't use an existing cluster".format(job_name))

            create_job(dest_profile, json.dumps(job_settings_json))

            logging.info("Job {} created successfully".format(job_name))

        logging.info("Step 3 finished successfully")
