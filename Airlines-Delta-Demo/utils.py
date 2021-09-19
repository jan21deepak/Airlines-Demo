# Databricks notebook source
import warnings
import json
import os
import time
warnings.filterwarnings("ignore")

import mlflow

# COMMAND ----------

def get_or_create_experiment(project_path, experiment_name):
  experiment_path = os.path.join(project_path, experiment_name)
  
  try:
    experiment_id = mlflow.create_experiment(experiment_path)
  except:
    experiment_id = mlflow.tracking.MlflowClient().get_experiment_by_name(experiment_path).experiment_id
    
  out = {'experiment_path': experiment_path, 'experiment_id': experiment_id}
  return out

# COMMAND ----------

# root project path
NOTEBOOK_PATH = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath()).replace('Some(', '').replace(')', '')
PROJECT_PATH = '/'.join(NOTEBOOK_PATH.split('/')[:-1])
