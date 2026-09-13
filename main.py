from __future__ import annotations
from enum import Enum
from typing import TypeVar,Generic
from collections import deque

T = TypeVar("T")    # represents Task

class TaskState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"

class Task(Generic[T]):

    def __init__(
            self,
            task_id:T,
            python_callable = None,
            op_kwargs = None
    ):
        self.task_id:T = task_id
        self.python_callable = python_callable
        self.op_kwargs:dict[str,any] = op_kwargs or {}
        self.state = TaskState.PENDING
        #Graph adjacency 
        self.upstreams:set[Task[T]] = set() 
        self.downstreams:set[Task[T]] = set()
        self.output = None

    def add_task(self,other):
        self.downstreams.add(other)
        other.upstreams.add(self)

    def execute(self,context: dict[str,any]):
        """Executes the task if any"""
        if self.python_callable:
            return self.python_callable(**self.op_kwargs)
        return None

    def __rshift__(self, other) -> Task:
        """Syntactic suger for Task_A >> Task_B >> Task C"""
        self.add_task(other)
        return other


    def __lshift__(self, other) -> Task:
        """Similar functionality for Task_C << Task_B << Task_A"""
        other.add_task(self)
        return other

    def __repr__(self) -> str:
        return f"<Task: {self.task_id} [{self.state.value}]>"


class DAG:

    def __init__(self,dag_id):
        self.dag_id = dag_id
        self.vertices: dict[T,Task[T]] = {}

    def add_task(self,task:Task):
        if task.task_id in self.vertices:
            raise ValueError(f"Duplicate task_id {task.task_id} detected in DAG {self.dag_id}")

        self.vertices[task.task_id] = task

    @property
    def adj_vertices(self) -> dict[T,list[T]]:

        return {
            task_id : [child.task_id for child in task.downstreams] for task_id, task in self.vertices.items()
        }


    def topo_sort(self):
        """
        Uses Kahn's Algorithm (BFS) for validating DAG
        Raises ValueError if Cyclic
        """
        in_degrees:dict[T,int] = {task_id:len(task.upstreams) for task_id,task in self.vertices.items()}
        queue = deque([task_id for task_id,i in in_degrees.items() if i==0])
        ordered=[]
        adjencies = self.adj_vertices

        while queue:
            curr = queue.popleft()
            ordered.append(curr)

            for ngbr in adjencies[curr]:
                in_degrees[ngbr] -= 1
                if in_degrees[ngbr] == 0:
                    queue.append(ngbr)


        if len(ordered) != len(self.vertices):
            unresolved = [tid for tid,val in in_degrees.items() if val>0]
            raise ValueError(f"Cycle detected involving tasks {unresolved}")

        return ordered

class SequentialExecutor:
    def __init__(self,dag):
        self.dag = dag
        self.context = {}

    def run(self):
        # verifying that the dag is acyclic
        self.dag.topo_sort()
        print(f"--- Starting DAG Run: {self.dag.dag_id} ---")
        pending_dependencies = {taskid:len(task.upstreams) for taskid,task in self.dag.vertices.items()}

        ready_queue = deque([task for task in self.dag.vertices.values() if pending_dependencies[task.task_id]==0])

        while ready_queue:
            task = ready_queue.pop()

            # checking upstream health:
            upstreamFailed = any(p.state in (TaskState.FAILED, TaskState.SKIPPED) for p in task.upstreams)

            if upstreamFailed:
                task.state = TaskState.SKIPPED
                print(f"[-] {task.task_id} -> SKIPPED (Upstream Failure)")
            else:
                task.state = TaskState.RUNNING
                print(f"[*] {task.task_id} -> RUNNING...")

                try:
                    result = task.execute(self.context)
                    task.output = result
                    self.context[task.task_id] = result
                    task.state = TaskState.SUCCESS
                    print(f"[+] {task.task_id} -> SUCCESS")

                except Exception as exc:
                    task.state = TaskState.FAILED
                    print(f"[!] {task.task_id} -> FAILED: {exc}")

            # Release downstream
            for downstream_task in task.downstreams:
                dst_id = downstream_task.task_id
                pending_dependencies[dst_id]-=1
                if pending_dependencies[dst_id] == 0:
                    ready_queue.append(downstream_task)

        print("_______________________________________________________________")
        print(f"     DAG Run Completed: {self.dag.dag_id}")