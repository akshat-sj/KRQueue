import sys
import json
import time
import threading
from kafka import KafkaConsumer, TopicPartition
from kafka.consumer.subscription_state import ConsumerRebalanceListener
import yadtq

# Define task functions
def add(args):
    return args[0] + args[1]

def sub(args):
    return args[0] - args[1]

def multiply(args):
    return args[0] * args[1]

# Accept worker_id as a command-line argument for unique identification
worker_id = sys.argv[1] if len(sys.argv) > 1 else "worker-1"

# Set up YADTQ worker with Kafka and Redis configurations
yadtq_worker = yadtq.YADTQ(broker="localhost:9092", backend="localhost")

# Custom Rebalance Listener to log partition assignments
class RebalanceListener(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked_partitions):
        print(f"{worker_id}: Partitions revoked: {[p.partition for p in revoked_partitions]}")

    def on_partitions_assigned(self, assigned_partitions):
        print(f"{worker_id}: Partitions assigned: {[p.partition for p in assigned_partitions]}")

# Configuring Kafka consumer with auto-rebalance settings and assigning RebalanceListener
yadtq_worker.consumer = KafkaConsumer(
    "task_queue",
    group_id="worker-group",
    bootstrap_servers=[yadtq_worker.broker],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    enable_auto_commit=True,
    session_timeout_ms=10000,
    heartbeat_interval_ms=3000,
    auto_offset_reset="earliest"
)

# Subscribe to the topic with a rebalance listener
yadtq_worker.consumer.subscribe(["task_queue"], listener=RebalanceListener())

# Heartbeat function for tracking worker status
def send_heartbeat():
    while True:
        yadtq_worker.redis_client.set(f"worker:{worker_id}:heartbeat", time.time())
        time.sleep(5)  # Heartbeat interval

# Start a thread for sending heartbeats
heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
heartbeat_thread.start()

# Process tasks and handle rebalancing
def process_task():
    for message in yadtq_worker.consumer:
        task_data = message.value
        task_id = task_data["task-id"]
        task_type = task_data["task"]
        args = task_data["args"]
        
        # Update task status in Redis to 'processing' with the worker ID
        yadtq_worker.redis_client.set(
            task_id, json.dumps({"status": "processing", "worker_id": worker_id})
        )
        
        try:
            # Execute the task based on its type
            if task_type == "add":
                result = add(args)
            elif task_type == "sub":
                result = sub(args)
            elif task_type == "multiply":
                result = multiply(args)
            else:
                raise ValueError(f"Unknown task type: {task_type}")
            
            # Update task status to 'success' with result and worker ID
            yadtq_worker.redis_client.set(
                task_id, json.dumps({"status": "success", "result": result, "worker_id": worker_id})
            )
            print(f"Task {task_id} completed by {worker_id} with result: {result}")
        
        except Exception as e:
            # On error, update status to 'failed' with error message and worker ID
            yadtq_worker.redis_client.set(
                task_id, json.dumps({"status": "failed", "error": str(e), "worker_id": worker_id})
            )
            print(f"Task {task_id} failed on {worker_id} with error: {e}")

# Start processing tasks
process_task()

