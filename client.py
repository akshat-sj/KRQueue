import yadtq
import time
import random
import json
# Initialize YADTQ client
yadtq_client = yadtq.YADTQ(broker="localhost:9092", backend="localhost")

# Define possible task types and range of arguments
task_types = ["add", "sub", "multiply"]
min_val, max_val = 1, 50

# Generate and submit 100 random tasks
task_ids = []
for _ in range(1000):
    task_type = random.choice(task_types)
    args = [random.randint(min_val, max_val), random.randint(min_val, max_val)]
    task_id = yadtq_client.send_task(task_type, args)
    print(f"Submitted Task {task_type} with args {args} - Task ID: {task_id}")
    task_ids.append(task_id)

# Track the completion of each task and print the results
print("\nPolling task statuses...\n")
completed_tasks = set()
while len(completed_tasks) < len(task_ids):
    for task_id in task_ids:
        if task_id in completed_tasks:
            continue

        # Fetch status and worker information from Redis
        status_info = yadtq_client.redis_client.get(task_id)
        if not status_info:
            continue
        
        status_info = json.loads(status_info)
        status = status_info.get("status")
        worker_id = status_info.get("worker_id", "unknown")

        # Display task results or errors along with the worker that processed it
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

    time.sleep(2)  # Polling interval

