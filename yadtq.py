import json
import uuid
import time
import threading
from typing import Callable, Optional
from kafka import KafkaProducer, KafkaConsumer
import redis

class YADTQ:
    def __init__(self, broker: str, backend: str):
        self.broker = broker
        self.backend = backend
        self.redis_client = redis.StrictRedis(host=self.backend, port=6379, db=0)
        
        # Producer to send tasks to Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=[self.broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    
    # Client-side function to send a task
    def send_task(self, task: str, args: list) -> str:
        task_id = str(uuid.uuid4())
        task_data = {"task-id": task_id, "task": task, "args": args}
        
        # Initializing the task status as 'queued' in Redis
        self.redis_client.set(task_id, json.dumps({"status": "queued"}))
        
        # Sending task to Kafka
        self.producer.send("task_queue", task_data)
        self.producer.flush()
        
        return task_id

    # Client-side function to check task status
    def status(self, task_id: str) -> str:
        task_info = self.redis_client.get(task_id)
        if task_info:
            return json.loads(task_info).get("status", "unknown")
        return "unknown"

    # Client-side function to get task result
    def result(self, task_id: str) -> Optional[str]:
        task_info = self.redis_client.get(task_id)
        if task_info:
            task_info = json.loads(task_info)
            if task_info.get("status") == "success":
                return task_info.get("result")
            elif task_info.get("status") == "failed":
                return task_info.get("error")
        return None

    # Worker-side function to configure worker
    def config_worker(self, group_id: str, topic: str):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[self.broker],
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )

    # Worker-side function to start processing tasks
    def run(self, task_func: Callable[[list], str]):
        for message in self.consumer:
            task_data = message.value
            task_id = task_data["task-id"]
            
            # Update status to 'processing' in Redis
            self.redis_client.set(task_id, json.dumps({"status": "processing"}))
            
            try:
                # Execute the task and update status to 'success' with result
                result = task_func(task_data["args"])
                self.redis_client.set(task_id, json.dumps({"status": "success", "result": result}))
            except Exception as e:
                # On error, update status to 'failed' with error message
                self.redis_client.set(task_id, json.dumps({"status": "failed", "error": str(e)}))

    # Worker heartbeat monitoring to detect active workers
    def send_heartbeat(self, worker_id: str):
        def heartbeat():
            while True:
                self.redis_client.set(f"worker:{worker_id}:heartbeat", time.time())
                time.sleep(10)
        
        heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
        heartbeat_thread.start()

