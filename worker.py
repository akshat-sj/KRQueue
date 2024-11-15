import sys
import yadtq
import time
import random
from kafka.admin import KafkaAdminClient, NewTopic

def task_func(task_type, args):
    # Simulate a 1 in 5 chance of task failure
    if random.randint(1, 5) == 1:
        raise Exception("Task failed")

    time.sleep(10)  # Add a delay to simulate a long-running task
    if task_type == "add":
        return args[0] + args[1]
    elif task_type == "sub":
        return args[0] - args[1]
    elif task_type == "mul":
        return args[0] * args[1]
    else:
        raise ValueError(f"Unknown task type: {task_type}")


worker_id = sys.argv[1]
yadtq_worker = yadtq.YADTQ(broker="localhost:9092", backend="localhost")

existing_topics = yadtq_worker.admin_client.list_topics()
if worker_id not in existing_topics:
    new_topic = NewTopic(
        name=worker_id,
        num_partitions=1,
        replication_factor=1
    )
    yadtq_worker.admin_client.create_topics([new_topic])

yadtq_worker.config_worker(group_id="worker-group", topic=worker_id, worker_id=worker_id)
yadtq_worker.redis_client.sadd("active_workers", worker_id)
yadtq_worker.run(task_func)
