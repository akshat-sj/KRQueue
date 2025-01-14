import sys
import krq.core as core
import time
import random
from kafka.admin import KafkaAdminClient, NewTopic

def task_func(task_type, args):
    if random.randint(1, 5) == 1:
        raise Exception("Task failed")

    time.sleep(5) 
    if task_type == "add":
        return args[0] + args[1]
    elif task_type == "sub":
        return args[0] - args[1]
    elif task_type == "mul":
        return args[0] * args[1]
    else:
        raise ValueError(f"Unknown task type: {task_type}")


worker_id = sys.argv[1]
krq_worker = core.KRQ(broker="localhost:9092", backend="localhost")

existing_topics = krq_worker.admin_client.list_topics()
if worker_id not in existing_topics:
    new_topic = NewTopic(
        name=worker_id,
        num_partitions=1,
        replication_factor=1
    )
    krq_worker.admin_client.create_topics([new_topic])

krq_worker.config_worker(group_id="worker-group", topic=worker_id, worker_id=worker_id)
krq_worker.redis_client.sadd("active_workers", worker_id)
krq_worker.run(task_func)