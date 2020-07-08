import logging
import json

logging.getLogger().setLevel(logging.INFO)

from subprocess import call, check_output
from subprocess import Popen, PIPE
from ..io import setNonBlocking
from databricks_cli.sdk import ApiClient
from databricks_cli.sdk.service import ClusterService


def configure_profile(profile, host_url, token):
    p = Popen(
        ["databricks", "configure", "--profile", "{}".format(profile), "--token"],
        stdin=PIPE,
        stdout=PIPE,
        stderr=PIPE,
        bufsize=1,
    )
    setNonBlocking(p.stdout)
    setNonBlocking(p.stderr)

    p.stdin.write("{}\n".format(host_url).encode())
    while True:
        try:
            out1 = p.stdout.read()
        except IOError as e:
            logging.exception(e)
            continue
        else:
            break

    p.stdin.write("{}\n".format(token).encode())
    while True:
        try:
            out2 = p.stdout.read()
        except IOError as e:
            logging.exception(e)
            continue
        else:
            break
    p.communicate()

    logging.info("Created new profile: " + profile)

    return profile


def list_users(profile) -> list:
    user_list_out = check_output(
        ["databricks", "workspace", "ls", "/Users", "--profile", profile]
    )
    user_list = ["{}".format(user.decode()) for user in user_list_out.splitlines()]
    return user_list


def list_clusters(profile) -> list:
    clusters_out = check_output(
        ["databricks", "clusters", "list", "--profile", profile]
    )
    clusters_info_list = clusters_out.splitlines()
    return clusters_info_list


def list_jobs(profile) -> list:
    jobs_out = check_output(["databricks", "jobs", "list", "--profile", profile])
    jobs_info_list = jobs_out.splitlines()
    return jobs_info_list


def list_dbfs_content(profile) -> list:
    dbfs_out = check_output(["databricks", "fs", "ls", "--profile", profile])
    dbfs_directories_list = ["{}".format(dir.decode()) for dir in dbfs_out.splitlines()]
    return dbfs_directories_list

def list_libraries(cluster_id, profile) -> list:
    libraries_out = json.loads(check_output(["databricks", "libraries", "list", "--profile", profile,"--cluster-id",cluster_id]).decode())
    libraries_info_list = libraries_out["library_statuses"]
    return libraries_info_list


def export_dir(profile, user):
    export_status = call(
        "databricks workspace export_dir /Users/"
        + user
        + " ./"
        + user
        + " --profile "
        + profile,
        shell=True,
    )
    return export_status


def import_dir(profile, user, overwrite):
    if overwrite == "True":
        p = Popen(
            [
                "databricks",
                "workspace",
                "import_dir",
                "./{}".format(user),
                "/Users/{}".format(user),
                "--profile",
                "{}".format(profile),
                "-o",
            ],
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
            bufsize=1,
        )
    else:
        p = Popen(
            [
                "databricks",
                "workspace",
                "import_dir",
                "./{}".format(user),
                "/Users/{}".format(user),
                "--profile",
                "{}".format(profile),
            ],
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
            bufsize=1,
        )
    output = p.communicate()[0]
    if p.returncode != 0:
        if output.decode().startswith("Error: {"):
            error = json.loads(output.decode()[7:])  # Skip "error: "
            logging.error("Error code: " + error["error_code"])
            logging.info("Error message: " + error["message"])
    return p.returncode


def get_cluster(profile, cluster_id):
    cluster_conf = check_output(
        [
            "databricks",
            "clusters",
            "get",
            "--cluster-id",
            cluster_id,
            "--profile",
            profile,
        ]
    )
    return cluster_conf.decode()


def create_cluster(profile, cluster_conf):
    creating_result = check_output(
        [
            "databricks",
            "clusters",
            "create",
            "--json-file",
            cluster_conf,
            "--profile",
            profile,
        ]
    )
    return creating_result.decode()


def terminate_cluster(profile, cluster_id, host_url, host_token):
    dbks_api = ApiClient(host=host_url, token=host_token)
    terminate_status = ClusterService(dbks_api).delete_cluster(cluster_id)
    return terminate_status


def check_if_cluster_exists(profile, cluster_name):
    clusters_list = list_clusters(profile)
    cluster_found = False
    cluster_id = ""
    for cluster_info in clusters_list:
        if cluster_info != b"":
            if " ".join(cluster_info.decode().split()[1:-1]) == cluster_name:
                cluster_found = True
                cluster_id = cluster_info.split(None, 1)[0].decode()
    return (cluster_found, cluster_id)


def get_job(profile, job_id):
    job_conf = check_output(
        ["databricks", "jobs", "get", "--job-id", job_id, "--profile", profile]
    )
    return job_conf.decode()


def create_job(profile, job_conf):
    creating_result = call(
        ["databricks", "jobs", "create", "--json", job_conf, "--profile", profile]
    )
    return creating_result

def install_library(cluster_id, source_profile,dest_profile, library_info):
    library_type = library_info["library"]
    if "jar" in library_type :
        check_output(["databricks", "fs", "cp", "--profile", source_profile, library_info["library"]["jar"], "." ])
        check_output(["databricks", "fs", "cp", "--profile", dest_profile, ".", library_info["library"]["jar"] ])
        installation_status = check_output(["databricks", "libraries", "install", "--profile", dest_profile,"--cluster-id",cluster_id, "--jar", library_info["library"]["jar"] ])
    elif "maven" in library_type:
        maven_type = "--maven-" + list(library_type["maven"].keys())[0]
        installation_status = check_output(["databricks", "libraries", "install", "--profile", dest_profile,"--cluster-id",cluster_id, maven_type, list(library_type["maven"].values())[0] ])
    elif "pypi" in library_type:
        pypi_type = "--pypi-" + list(library_type["pypi"].keys())[0]
        installation_status = check_output(["databricks", "libraries", "install", "--profile", dest_profile,"--cluster-id",cluster_id, pypi_type, list(library_type["pypi"].values())[0] ])
    elif "cran" in library_type:
        cran_type = "--cran-" + list(library_type["cran"].keys())[0]
        installation_status = check_output(["databricks", "libraries", "install", "--profile", dest_profile,"--cluster-id",cluster_id, cran_type, list(library_type["cran"].values())[0] ])
              
    return installation_status


def check_if_job_exists(profile, job_name):
    jobs_list = list_jobs(profile)
    job_status = False
    for job_info in jobs_list:
        if job_info != b"":
            if job_info.split(None, 1)[1].decode() == job_name:
                job_status = True
    return job_status
