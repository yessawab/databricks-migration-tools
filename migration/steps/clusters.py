from subprocess import call, check_output
import json, os
from .common.databricks_cli.cli import (
    list_clusters,
    get_cluster,
    create_cluster,
    check_if_cluster_exists,
    terminate_cluster,
)
import logging


def migrate_clusters(source_profile, dest_profile, host_url, host_token):
    clusters_info_list = list_clusters(source_profile)

    clusters_list_ids = []

    for cluster_info in clusters_info_list:
        if cluster_info != "":
            clusters_list_ids.append(cluster_info.split(None, 1)[0].decode())

    cluster_req_elems = [
        "cluster_name",
        "num_workers",
        "autoscale",
        "cluster_name",
        "spark_version",
        "spark_conf",
        "node_type_id",
        "driver_node_type_id",
        "custom_tags",
        "cluster_log_conf",
        "spark_env_vars",
        "autotermination_minutes",
        "enable_elastic_disk",
    ]

    logging.info(str(len(clusters_list_ids)) + " clusters found in the primary site")

    cluster_old_new_mappings = {}
    i = 0
    for cluster_id in clusters_list_ids:
        i += 1
        cluster_conf = get_cluster(source_profile, cluster_id)

        cluster_req_json = json.loads(cluster_conf)
        cluster_json_keys = list(cluster_req_json.keys())

        if cluster_req_json["cluster_source"] == u"JOB":
            logging.info(
                "Skipping cluster as it is a Job cluster"
                + cluster_req_json["cluster_id"]
            )
            continue

        cluster_name = cluster_req_json["cluster_name"]

        (cluster_exists, existing_cluster_id) = check_if_cluster_exists(
            dest_profile, cluster_name
        )
        if cluster_exists:
            logging.warn(
                "Skipping cluster *{}* as it already exists".format(cluster_name)
            )
            cluster_old_new_mappings[cluster_id] = existing_cluster_id
            continue

        logging.info(
            "Migrating cluster "
            + "*"
            + cluster_name
            + "* "
            + str(i)
            + "/"
            + str(len(clusters_list_ids))
            + " : "
            + cluster_id
        )

        for key in cluster_json_keys:
            if key not in cluster_req_elems:
                cluster_req_json.pop(key, None)

        strCurrentClusterFile = "tmp_cluster_info.json"

        if os.path.exists(strCurrentClusterFile):
            os.remove(strCurrentClusterFile)

        fClusterJSONtmp = open(strCurrentClusterFile, "w+")
        fClusterJSONtmp.write(json.dumps(cluster_req_json))
        fClusterJSONtmp.close()

        cluster_create_result = create_cluster(dest_profile, strCurrentClusterFile)
        cluster_create_out_json = json.loads(cluster_create_result)
        cluster_old_new_mappings[cluster_id] = cluster_create_out_json["cluster_id"]
        logging.info(
            "Cluster *{}* created successfully with new cluster id {}".format(
                cluster_name, cluster_create_out_json["cluster_id"]
            )
        )

        if os.path.exists(strCurrentClusterFile):
            os.remove(strCurrentClusterFile)

    logging.info("Cluster mappings: " + json.dumps(cluster_old_new_mappings))
    for cluster_id in cluster_old_new_mappings.keys():
        if existing_cluster_id != "":
            terminate_cluster(
                dest_profile, cluster_old_new_mappings[cluster_id], host_url, host_token
            )
    logging.info("Stopped all created clusters")
    logging.info("Step 2 finished successfully")
    return cluster_old_new_mappings
