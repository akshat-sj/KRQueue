import json
import uuid
import time
import threading
import logging
import random
import re
import redis
import heapq
from logging.handlers import RotatingFileHandler
from typing import Callable, Optional, List, Tuple
from kafka import KafkaProducer, KafkaConsumer
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.admin import KafkaAdminClient, NewTopic

class KRQ:
    def __init__(self, broker: str, backend: str, max_retries: int = 3):
        self.broker = broker
        self.backend = backend
        self.max_retries = max_retries
        self.program_start_time = time.time()
        self.redis_client = redis.StrictRedis(host=self.backend, port=6379, db=0)
        self.worker_ids = {w.decode("utf-8") for w in self.redis_client.smembers("active_workers")}
        self.producer = KafkaProducer(
            bootstrap_servers=[self.broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        self.task_stats = {
            "total_tasks": 0,
            "successful_tasks": 0,
            "failed_tasks": 0,
            "total_retries": 0,
            "total_execution_time": 0
        }
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=[self.broker],
            client_id="admin_client"
        )

        self.logger = logging.getLogger("KRQ")
        self.logger.setLevel(logging.INFO)
        with open("KRQ.log", "w"):
            pass
        handler = RotatingFileHandler("KRQ.log", maxBytes=5 * 1024 * 1024, backupCount=2)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        self.last_log_time = time.time()
        self._priority_queue = []
        self._arrival_counter = 0
        self._stop_event = threading.Event()

    def log_periodically(self, message, level=logging.INFO):
        current_time = time.time()
        if current_time - self.last_log_time >= 5:
            self.last_log_time = current_time
            if level == logging.INFO:
                self.logger.info(message)
            elif level == logging.ERROR:
                self.logger.error(message)

    def exponential_backoff_with_jitter(self, retries: int) -> float:
        base_delay = 1.0
        max_delay = 30.0
        delay = min(base_delay * (2 ** (retries - 1)), max_delay)
        jitter = random.uniform(0, 1)
        return delay + jitter

    def send_task(self, task: str, args: list, priority: int = 0) -> str:
        task_id = str(uuid.uuid4())
        task_data = {
            "task-id": task_id,
            "task": task,
            "args": args,
            "priority": priority
        }
        self.redis_client.set(task_id, json.dumps({"status": "queued", "retries": 0, "priority": priority}))
        loads = {w: int(self.redis_client.get(f"worker:{w}:load") or 0) for w in self.worker_ids}
        if loads:
            least_loaded_worker = min(loads, key=loads.get)
            self.redis_client.incr(f"worker:{least_loaded_worker}:load")
            self.task_stats["total_tasks"] += 1
            self.producer.send(least_loaded_worker, task_data)
            self.producer.flush()
            self.log_periodically(f"Task {task_id} (priority {priority}) sent to worker {least_loaded_worker}")
        else:
            self.log_periodically(f"No workers available for task {task_id}", logging.ERROR)
        return task_id

    def broadcast(self, task: str, args: list, priority: int = 0) -> str:
        task_id = str(uuid.uuid4())
        task_data = {
            "task-id": task_id,
            "task": task,
            "args": args,
            "priority": priority
        }
        self.task_stats["total_tasks"] += 1
        for w in self.worker_ids:
            self.producer.send(w, task_data)
        self.producer.flush()
        self.log_periodically(f"Task {task_id} broadcasted to all workers (priority {priority})")
        return task_id

    def send_failtask(self, task_id: str, task_type: str, args: list, priority: int = 0, retries: int = 0):
        backoff = self.exponential_backoff_with_jitter(retries)
        time.sleep(2)
        task_data = {
            "task-id": task_id,
            "task": task_type,
            "args": args,
            "priority": priority
        }
        loads = {w: int(self.redis_client.get(f"worker:{w}:load") or 0) for w in self.worker_ids}
        if loads:
            least_loaded_worker = min(loads, key=loads.get)
            self.redis_client.incr(f"worker:{least_loaded_worker}:load")
            self.producer.send(least_loaded_worker, task_data)
            self.producer.flush()
            print(f"Retrying task {task_id} (priority {priority}), assigned to {least_loaded_worker}")
        else:
            print(f"No workers available to reassign failed task {task_id}", logging.ERROR)

    def update_workers(self):
        self.worker_ids = {w.decode("utf-8") for w in self.redis_client.smembers("active_workers")}
        self.log_periodically(f"Updated worker list: {self.worker_ids}")

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
            while not self._stop_event.is_set():
                self.redis_client.set(f"worker:{self.worker_id}:heartbeat", time.time())
               #self.log_periodically(f"Heartbeat sent for worker {self.worker_id}")
                self.update_workers()
                time.sleep(10)
        heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
        heartbeat_thread.start()

    def run(self, task_func: Callable[[str, list], str]):
        self.send_heartbeat()
        self.log_periodically("Worker run loop starting")
        while not self._stop_event.is_set():
            for message in self.consumer:
                if self._stop_event.is_set():
                    break
                task_data = message.value
                priority = task_data.get("priority", 0)
                heapq.heappush(self._priority_queue, (-priority, self._arrival_counter, task_data))
                self._arrival_counter += 1

                while self._priority_queue and not self._stop_event.is_set():
                    _, _, next_task_data = heapq.heappop(self._priority_queue)
                    self._process_task(next_task_data, task_func)

        self.log_periodically("Worker run loop exiting")

    def _process_task(self, t_data: dict, task_func: Callable[[str, list], str]):
        task_id = t_data["task-id"]
        task_type = t_data["task"]
        args = t_data["args"]
        priority = t_data.get("priority", 0)
        start_time = time.time()

        task_obj = json.loads(self.redis_client.get(task_id) or "{}")
        retries = task_obj.get("retries", 0)
        self.redis_client.set(task_id, json.dumps({
            "status": "processing",
            "worker_id": self.worker_id,
            "retries": retries,
            "priority": priority
        }))

        try:
            result = task_func(task_type, args)
            self.redis_client.set(task_id, json.dumps({
                "status": "success",
                "result": result,
                "worker_id": self.worker_id,
                "retries": retries,
                "priority": priority
            }))
            self.task_stats["successful_tasks"] += 1
            self.task_stats["total_execution_time"] += (time.time() - start_time)
            self.log_periodically(f"Task {task_id} completed by {self.worker_id} in {time.time() - start_time:.2f}s (priority {priority}) and result:{result}")
        except Exception as e:
            retries += 1
            self.task_stats["total_retries"] += 1
            if retries > self.max_retries:
                self.redis_client.set(task_id, json.dumps({
                    "status": "failed",
                    "error": str(e),
                    "worker_id": self.worker_id,
                    "retries": retries,
                    "priority": priority
                }))
                self.task_stats["failed_tasks"] += 1
                print(f"Task {task_id} permanently failed after {retries} retries", logging.ERROR)
                self.log_periodically(f"Task {task_id} permanently failed after {retries} retries", logging.ERROR)
            else:
                self.redis_client.set(task_id, json.dumps({
                    "status": "queued",
                    "task": task_type,
                    "args": args,
                    "retries": retries,
                    "priority": priority
                }))
                print(f"Task {task_id} failed on {self.worker_id} with error: {e}. Retry {retries}/{self.max_retries}", logging.ERROR)
                self.log_periodically(f"Task {task_id} failed on {self.worker_id} with error: {e}. Retry {retries}/{self.max_retries}", logging.ERROR)
                self.send_failtask(task_id, task_type, args, priority, retries)

    def chain_tasks(self, tasks: List[Tuple[str, list]], final_callback: Optional[Callable[[List[str]], None]] = None, priority: int = 0) -> List[str]:
        task_ids = []
        previous_task_id = None
        for (task_type, args) in tasks:
            tid = self.send_task(task_type, args, priority)
            task_ids.append(tid)
            if previous_task_id:
                self._attach_chaining_trigger(previous_task_id, tid)
            previous_task_id = tid
        if final_callback:
            self._attach_final_callback(previous_task_id, final_callback, task_ids)
        return task_ids

    def _attach_chaining_trigger(self, first_task_id: str, next_task_id: str):
        self.redis_client.set(f"chained:{first_task_id}", next_task_id)

    def _attach_final_callback(self, last_task_id: str, callback: Callable[[List[str]], None], all_task_ids: List[str]):
        self.redis_client.set(f"final_callback:{last_task_id}", json.dumps({"callback": "dummy", "task_ids": all_task_ids}))

    def group_tasks(self, tasks: List[Tuple[str, list]], final_task: Tuple[str, list], priority: int = 0) -> List[str]:
        task_ids = [self.send_task(t[0], t[1], priority) for t in tasks]
        final_task_id = self.send_task(final_task[0], final_task[1], priority)
        self.redis_client.set(f"group:{final_task_id}", json.dumps({"dependencies": task_ids}))
        return task_ids + [final_task_id]

    def stop(self):
        self._stop_event.set()

    def print_final_stats(self):
        keys = self.redis_client.keys("*")
        task_uuid_pattern = re.compile(r"^[a-f0-9-]{36}$")
        successful_tasks = 0
        failed_tasks = 0
        total_retries = 0

        for key in keys:
            task_key = key.decode("utf-8")
            if task_uuid_pattern.match(task_key):
                raw = self.redis_client.get(task_key)
                if raw:
                    info = json.loads(raw)
                    status = info.get("status", "")
                    retries = info.get("retries", 0)
                    if status == "success":
                        successful_tasks += 1
                    elif status == "failed":
                        failed_tasks += 1
                    total_retries += retries

        total_tasks = successful_tasks + failed_tasks
        program_end_time = time.time()
        total_program_time = program_end_time - self.program_start_time
        avg_execution_time = total_program_time / successful_tasks if successful_tasks > 0 else 0

        print("Final Task Statistics:")
        print(f"Total Tasks: {total_tasks}")
        print(f"Successful Tasks: {successful_tasks}")
        print(f"Failed Tasks: {failed_tasks}")
        print(f"Total Retries: {total_retries}")
        print(f"Total Execution Time: {total_program_time:.2f} seconds")
        print(
            f"Average Task Execution Time: {avg_execution_time:.2f} seconds"
            if successful_tasks > 0
            else "Average Task Execution Time: N/A"
        )