import krq.core as core
import time
import json
import threading

def final_callback(task_ids):
    print("All tasks in the chain have been completed.")
    for task_id in task_ids:
        status_info = krq_client.redis_client.get(task_id)
        if status_info:
            status_info = json.loads(status_info)
            status = status_info.get("status")
            result = status_info.get("result")
            if status == "success":
                print(f"Task {task_id} succeeded with result: {result}")
            else:
                error = status_info.get("error", "Unknown error")
                print(f"Task {task_id} failed with error: {error}")


krq_client = core.KRQ(broker="localhost:9092", backend="localhost")


old_heartbeat_keys = krq_client.redis_client.keys("worker:*:heartbeat")
for key in old_heartbeat_keys:
    krq_client.redis_client.delete(key)


tasks = [
    ("add", [10, 5]),  
    ("mul", [15, 2]),    
    ("sub", [30, 10])   
]

task_ids = krq_client.chain_tasks(tasks, final_callback=final_callback, priority=1)

print(f"Submitted a chain of {len(task_ids)} tasks with IDs: {task_ids}")

def monitor_heartbeats():
    while True:
        keys = krq_client.redis_client.keys("worker:*:heartbeat")
        current_time = time.time()
        for key in keys:
            heartbeat_time = float(krq_client.redis_client.get(key))
            worker_id = key.decode("utf-8").split(":")[1]
            if current_time - heartbeat_time > 30:
                print(f"Worker {worker_id} is unresponsive")
                task_keys = krq_client.redis_client.keys(f"task:*:worker_id:{worker_id}")
                for task_key in task_keys:
                    task_id = task_key.decode("utf-8").split(":")[1]
                    task_info = json.loads(krq_client.redis_client.get(task_id))
                    if task_info["status"] == "processing":
                        krq_client.redis_client.set(
                            task_id, json.dumps({"status": "failed", "error": "Worker unresponsive", "worker_id": worker_id})
                        )
            else:
                print(f"Heartbeat received from worker {worker_id} at {heartbeat_time}")
        time.sleep(10)

heartbeat_thread = threading.Thread(target=monitor_heartbeats, daemon=True)
heartbeat_thread.start()


completed_tasks = set()
while len(completed_tasks) < len(task_ids):
    for task_id in task_ids:
        if task_id in completed_tasks:
            continue

        status_info = krq_client.redis_client.get(task_id)
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
    print("#############################################")

krq_client.print_final_stats()