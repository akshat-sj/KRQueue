import json
import uuid
import time
import threading
from typing import Callable, Optional
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.admin import KafkaAdminClient, NewTopic
import redis

class YADTQ:
    def __init__(self, broker: str, backend: str, max_retries: int = 3):
        self.broker = broker
        self.backend = backend
        self.max_retries = max_retries
        self.redis_client = redis.StrictRedis(host=self.backend, port=6379, db=0)
        self.worker_ids = {worker_id.decode('utf-8') for worker_id in self.redis_client.smembers("active_workers")}
        self.producer = KafkaProducer(
            bootstrap_servers=[self.broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        self.admin_client = KafkaAdminClient(
    	    bootstrap_servers=[self.broker],
            client_id="admin_client"
	)
        
    def send_task(self, task: str, args: list) -> str:
        task_id = str(uuid.uuid4())
        task_data = {"task-id": task_id, "task": task, "args": args}
        self.redis_client.set(task_id, json.dumps({"status":"queued", "retries": 0}))
        loads = {worker_id: int(self.redis_client.get(f"worker:{worker_id}:load") or 0) for worker_id in self.worker_ids}
        print(loads)
        least_loaded_worker = min(loads, key=loads.get)
        print(least_loaded_worker)
        
        self.redis_client.incr(f"worker:{least_loaded_worker}:load")
   
        self.producer.send(least_loaded_worker, task_data)
        self.producer.flush()
        
        return task_id
        
    def send_failtask(self, task_id: str, task_type: str, args: list) -> str:
    	task_data = {
        	"task-id": task_id,
        	"task": task_type,
        	"args": args,
    	}
    	
    	loads = {worker_id: int(self.redis_client.get(f"worker:{worker_id}:load") or 0) for worker_id in self.worker_ids}
    	print(loads)
    	least_loaded_worker = min(loads, key=loads.get)
    	print(least_loaded_worker)
    	self.redis_client.incr(f"worker:{least_loaded_worker}:load")
    	self.producer.send(least_loaded_worker, task_data)
    	self.producer.flush()
    	
    	return task_id
    	
    def update_workers(self):
    	self.worker_ids = {worker_id.decode('utf-8') for worker_id in self.redis_client.smembers("active_workers")}
        
    def status(self, task_id: str) -> str:
        task_info = self.redis_client.get(task_id)
        if task_info:
            return json.loads(task_info).get("status", "unknown")
        return "unknown"

    def result(self, task_id: str) -> Optional[str]:
        task_info = self.redis_client.get(task_id)
        if task_info:
            task_info = json.loads(task_info)
            if task_info.get("status") == "success":
                return task_info.get("result")
            elif task_info.get("status") == "failed":
                return task_info.get("error")
        return None

    def config_worker(self, group_id: str, topic: str, worker_id: str):
        self.worker_id = worker_id
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[self.broker],
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            enable_auto_commit=True,
            session_timeout_ms=10000,
            heartbeat_interval_ms=30,
            auto_offset_reset="earliest"
        )
        self.consumer.subscribe([topic], listener=self.RebalanceListener(worker_id))
    
    class RebalanceListener(ConsumerRebalanceListener):
        def __init__(self, worker_id):
            self.worker_id = worker_id

        def on_partitions_revoked(self, revoked_partitions):
            print(f"{self.worker_id}: Partitions revoked: {[p.partition for p in revoked_partitions]}")

        def on_partitions_assigned(self, assigned_partitions):
            print(f"{self.worker_id}: Partitions assigned: {[p.partition for p in assigned_partitions]}")

    
    def send_heartbeat(self):
        def heartbeat():
            while True:
                self.redis_client.set(f"worker:{self.worker_id}:heartbeat", time.time())
                print(f"Heartbeat sent for worker {self.worker_id} at {time.time()}")
                self.update_workers()
                time.sleep(10)
        
        heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
        heartbeat_thread.start()

    def run(self, task_func: Callable[[str, list], str]):
        self.send_heartbeat()

        while True:
            for message in self.consumer:
                task_data = message.value
                task_id = task_data["task-id"]
                task_type = task_data["task"]
                args = task_data["args"]
                
                task_info = json.loads(self.redis_client.get(task_id))
                retries = task_info.get("retries", 0)
                
                self.redis_client.set(
                    task_id, json.dumps({"status": "processing", "worker_id": self.worker_id, "retries": retries})
                )
                
                try:
                    result = task_func(task_type, args)
                    self.redis_client.set(
                        task_id, json.dumps({"status": "success", "result": result, "worker_id": self.worker_id, "retries": retries})
                    )
                    
                    self.redis_client.decr(f"worker:{self.worker_id}:load")
                    print(f"Task {task_id} completed by {self.worker_id} with result: {result}")
                
                except Exception as e:
                    retries += 1
                    if retries > self.max_retries:
                        self.redis_client.set(
                            task_id, json.dumps({"status": "failed", "error": str(e), "worker_id": self.worker_id, "retries": retries})
                        )
                        print(f"Task {task_id} failed on {self.worker_id} with error: {e} after {retries} retries")
                    else:
                        self.redis_client.set(
                            task_id, json.dumps({"status": "queued", "task": task_type, "args": args, "retries": retries})
                        )
                        self.send_failtask(task_id, task_type, args)
                        print(f"Task {task_id} failed on {self.worker_id} with error: {e}. Retrying {retries}/{self.max_retries}")
