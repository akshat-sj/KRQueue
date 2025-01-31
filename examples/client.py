from krq.core import KRQ
import time
import random
import json
import threading
import logging


krq_client = KRQ(broker="localhost:9092", backend="localhost")


old_heartbeat_keys = krq_client.redis_client.keys("worker:*:heartbeat")
for key in old_heartbeat_keys:
    krq_client.redis_client.delete(key)

task_types = ["add", "sub", "mul"]
min_val, max_val = 1, 50

task_ids = []
for _ in range(10):
    task_type = random.choice(task_types)
    args = [random.randint(min_val, max_val), random.randint(min_val, max_val)]
    task_id = krq_client.send_task(task_type, args)
    print(f"Submitted Task {task_type} with args {args} - Task ID: {task_id}")
    task_ids.append(task_id)

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

logging.basicConfig(
    filename='results.log',
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
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
            message = f"Task {task_id} completed by {worker_id} with result: {result}"
            print(message)
            logging.info(message)
            completed_tasks.add(task_id)
        elif status == "failed":
            error = status_info.get("error")
            message = f"Task {task_id} processed by {worker_id} failed with error: {error}"
            print(message)
            logging.error(message)
            completed_tasks.add(task_id)
        else:
            message = f"Task {task_id} is currently {status}, being processed by {worker_id}"
            print(message)
            # logging.info(message)

    time.sleep(2)
    print("#############################################")

krq_client.print_final_stats()