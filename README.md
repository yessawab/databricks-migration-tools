# Databricks workspace migration tools

## Description

This repository serves a databricks tools to migrate different content before workspace as well as Terraform templates to automatically spin up workspaces.  

## Usage steps

- Configure_profile: Used to configure new profiles (Source & Destination) in Databricks cli.
- Migrate_workspace: Used to migrate users' content to the destination workspace. This concerns folders, files...
- Migrate_clusters: Used to migrate clusters. It's possible to add a prefix to cluster names. For example, migrating from Dev to Prod, the cluster name goes from <name>-dev to <name>-prod
- Migrate_jobs: Used to migrate jobs. Same logic applies for the prefix.
- Migrate_libraries: Used to migrate and install libraries in the destination workspace.

## Example: Running the migration using Vscode

List of used arguments:
```
    "--src_name"   : Name of the source workspace
    "--src_url"    : Url of the source workspace
    "--src_token"  : Token for the source workspace
    "--dest_name"  : Name of the destination workspace
    "--dest_url"   : Url of the destination workspace
    "--dest_token" : Url of the destination workspace
    "--o"          : Overwrite (True/False): Default is False
    "--env_name"   : Name of the environment
```

In Vscode, go to RUN (Ctrl+Shift+D), add a new configuration to run the main application:

```
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Module",
            "type": "python",
            "cwd": "${workspaceFolder}/migration/",
            "request": "launch",
            "module": "steps.main",
            "args": [
                "--src_name=<source_workspace_name>",
                "--src_url=<source_workspace_url>",
                "--src_token=<source_workspace_token>",
                "--dest_name=<dest_workspace_name>",
                "--dest_url=<dest_workspace_url>",
                "--dest_token=<dest_workspace_token>",
                "--o=False",
                "--env_name=prod"
            ],
        }
    ]
}

```
