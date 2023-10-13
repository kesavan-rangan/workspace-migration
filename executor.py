# Databricks notebook source
# MAGIC %md Widgets to collect Inputs from User

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_workspace_url", "https://adb-2167256452995141.1.azuredatabricks.net/", "Source Workspace URL")
dbutils.widgets.text("source_workspace_token_secret_scope", "auth", "Source Workspace Token Secret Scope")
dbutils.widgets.text("source_workspace_token_secret_name", "source_token", "Source Workspace Token Secret Name")
dbutils.widgets.text("destination_workspace_url", "https://adb-5349245795136543.3.azuredatabricks.net/", "Destination Workspace URL")
dbutils.widgets.text("destination_workspace_token_secret_scope", "auth", "Destination Workspace Token Secret Scope")
dbutils.widgets.text("destination_workspace_token_secret_name", "destination_token", "Destination Workspace Token Secret Name")

# COMMAND ----------

# MAGIC %md
# MAGIC Installing Requirements

# COMMAND ----------

# MAGIC %sh pip3 install sqlparse mlflow databricks

# COMMAND ----------

from uuid import uuid4

execution_context = str(uuid4())

source_workspace_url = dbutils.widgets.get("source_workspace_url")
source_token = dbutils.secrets.get(dbutils.widgets.get("source_workspace_token_secret_scope"), dbutils.widgets.get("source_workspace_token_secret_name"))

destination_workspace_url = dbutils.widgets.get("destination_workspace_url")
destination_token = dbutils.secrets.get(dbutils.widgets.get("destination_workspace_token_secret_scope"), dbutils.widgets.get("destination_workspace_token_secret_name"))

# COMMAND ----------

import subprocess
import os
from uuid import uuid4

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

TEMP_DIR = "/FileStore/temp/"
if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)

source_token_file_name = TEMP_DIR+"source_token"
with open(source_token_file_name, "w") as f:
    f.write(source_token)

destination_token_file_name = TEMP_DIR+"destination_token"
with open(destination_token_file_name, "w") as f:
    f.write(destination_token)
    
source_configure_command = ["databricks", "configure", "--profile", "source", "--host", source_workspace_url, "--token-file", source_token_file_name]
destination_configure_command = ["databricks", "configure", "--profile", "destination", "--host", destination_workspace_url, "--token-file", destination_token_file_name]

# COMMAND ----------

source_exec = run_cmd(source_configure_command)
destination_exec = run_cmd(destination_configure_command)

if os.path.exists(source_token_file_name):
    os.remove(source_token_file_name)
if os.path.exists(destination_token_file_name):
    os.remove(destination_token_file_name)

# COMMAND ----------

# MAGIC %md Databricks profiles are configured. Executing the export and import

# COMMAND ----------

# Executing the export command
EXPORT_DIR_PATH = f"/FileStore/exports/{execution_context}"

if not os.path.exists(EXPORT_DIR_PATH):
    os.makedirs(EXPORT_DIR_PATH)
export_command = ["python3", "-u", "./migrate_pipeline_current.py", "--azure", "--profile", "source", "--export-pipeline", "--set-export-dir", EXPORT_DIR_PATH]

run_cmd(export_command)

# COMMAND ----------

# Executing the Import command
EXPORT_DIR_PATH = f"/FileStore/exports/{execution_context}"

EXPORT_DIR_PATH_UPDATED = os.path.join(EXPORT_DIR_PATH, os.listdir(EXPORT_DIR_PATH)[0])

if not os.path.exists(EXPORT_DIR_PATH):
    os.makedirs(EXPORT_DIR_PATH)
import_command = ["python3", "-u", "./migrate_pipeline_current.py", "--azure", "--profile", "destination", "--import-pipeline", "--set-export-dir", EXPORT_DIR_PATH_UPDATED, "--no-prompt", "--include-user-import"]

run_cmd(import_command)
