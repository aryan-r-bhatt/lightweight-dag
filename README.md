# lightweight-dag

A zero-dependency Python DAG workflow engine for executing directed acyclic graphs of tasks with dependency resolution and state management.

## Features

* **Cycle Detection:** Kahn's algorithm (BFS-based topological sort) checks for cycles before execution.
* **Bitshift Operator Chaining:** Airflow-style syntax (`t1 >> t2 >> t3` or `t3 << t2`) to define dependencies.
* **State Management:** Tracks task states (`PENDING`, `RUNNING`, `SUCCESS`, `FAILED`, `SKIPPED`).
* **Cascade Failure Handling:** Downstream tasks are automatically skipped if an upstream task fails or is skipped.
* **Context Sharing:** Captures task outputs and stores them in a shared execution context dictionary.

## Installation

Clone the repository:

```bash
git clone [https://github.com/aryan-r-bhatt/lightweight-dag.git](https://github.com/aryan-r-bhatt/lightweight-dag.git)
cd lightweight-dag
```

No external libraries are required. Python 3.8+ is recommended.

## Quickstart

```python
from main import DAG, Task, SequentialExecutor

# 1. Define task callables
def extract():
    return [1, 2, 3, 4]

def transform(data):
    return [x * 2 for x in data]

def load():
    print("Pipeline completed successfully.")

# 2. Initialize the DAG and Tasks
dag = DAG(dag_id="sample_etl")

t1 = Task(task_id="extract", python_callable=extract)
t2 = Task(task_id="transform", python_callable=transform, op_kwargs={"data": [1, 2, 3, 4]})
t3 = Task(task_id="load", python_callable=load)

# 3. Add tasks to DAG
for task in [t1, t2, t3]:
    dag.add_task(task)

# 4. Define dependencies
t1 >> t2 >> t3

# 5. Run the workflow
executor = SequentialExecutor(dag)
executor.run()
```

## Running Tests

If you have `pytest` installed:

```bash
python -m pytest
```

Or run directly with Python:

```bash
python -m unittest discover -s tests
```