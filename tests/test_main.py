import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import time
from main import *

# Concrete task functions
def extract_data():
  return {"raw_records": [10, 25, 42, 99]}


def clean_data():
  return [10, 25, 42, 99]


def compute_metrics():
  return {"mean": 44.0, "status": "READY"}


def train_model():
  # Simulating a job run
  time.sleep(0.5)
  return "Model_v1_Trained"


def alert_on_complete():
  print("Notification: Pipeline finished successfully.")


# Instantiating the Pipeline
etl_dag = DAG(dag_id="model_preprocessing_pipeline")

# Register tasks
t_extract = Task("extract_data", python_callable=extract_data)
t_clean = Task("clean_data", python_callable=clean_data)
t_metrics = Task("compute_metrics", python_callable=compute_metrics)
t_train = Task("train_model", python_callable=train_model)
t_notify = Task("alert_on_complete", python_callable=alert_on_complete)

for t in [t_extract, t_clean, t_metrics, t_train, t_notify]:
  etl_dag.add_task(t)

# Define dependency graph using Airflow-style bitshift syntax
# Branching pipeline:
#                   -> clean_data ----> train_model
#                  /                                \
#   extract_data -                                   -> alert_on_complete
#                  \                                /
#                   -> compute_metrics -------------
t_extract >> t_clean >> t_train >> t_notify
t_extract >> t_metrics >> t_notify

# Execute pipeline
executor = SequentialExecutor(etl_dag)
executor.run()