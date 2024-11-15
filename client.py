import yadtq
import time
import random
import json

yadtq_client = yadtq.YADTQ(broker="localhost:9092", backend="localhost")

task_types = ["add", "sub", "mul"]
min_val, max_val = 1, 50

task_ids = []
for _ in range(100):
    task_type = random.choice(task_types)
    args = [random.randint(min_val, max_val), random.randint(min_val, max_val)]
    task_id = yadtq_client.send_task(task_type, args)
    print(f"Submitted Task {task_type} with args {args} - Task ID: {task_id}")
    task_ids.append(task_id)

# polling tasks
completed_tasks = set()
while len(completed_tasks) < len(task_ids):
    for task_id in task_ids:
        if task_id in completed_tasks:
            continue

        status_info = yadtq_client.redis_client.get(task_id)
        if not status_info:
            continue
        
        status_info = json.loads(status_info)
        status = status_info.get("status")
        worker_id = status_info.get("worker_id", "unknown")

        if status == "success":
            result = status_info.get("result")
            print(f"Task {task_id} completed by {worker_id} with result: {result}")
            completed_tasks.add(task_id)
        elif status == "failed":
            error = status_info.get("error")
            print(f"Task {task_id} processed by {worker_id} failed with error: {error}")
            completed_tasks.add(task_id)
        else:
            print(f"Task {task_id} is currently {status}, being processed by {worker_id}")

    time.sleep(2)
