import sys
from krq.core import KRQ
import time
import random
import sys
from kafka.admin import KafkaAdminClient, NewTopic
import os
def task_func(task_type, args):
    if task_type == "add":
        return args[0] + args[1]
    elif task_type == "sub":
        return args[0] - args[1]
    elif task_type == "mul":
        return args[0] * args[1]
    else:
        raise ValueError(f"Unknown task type: {task_type}")


worker_id = sys.argv[1]
krq_worker = KRQ(broker="localhost:9092", backend="localhost")

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