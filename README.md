# KRQueue
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
![Python](https://img.shields.io/badge/python-v3.11+-blue.svg)
[![Build Status](https://travis-ci.org/anfederico/clairvoyant.svg?branch=master)](https://travis-ci.org/anfederico/clairvoyant)
![Dependencies](https://img.shields.io/badge/dependencies-up%20to%20date-brightgreen.svg)
![Contributions welcome](https://img.shields.io/badge/contributions-welcome-orange.svg)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## Overview
A distributed task queue that uses kafka and redis that co-ordinates tasks between multiple workers in a highly distributed setup.

<br>


## Setup Instructions

Follow these steps to get started with the project:

1. **Start Kafka and Redis**  
   Ensure that Kafka and Redis are installed and running on your system. These services are essential for communication and result storage within the system.

2. **Install Dependencies**  
   Use the following command to install the required Python packages:  
   ```console
   pip install -e .
   ```

<br>

## Testing

### Running the Examples 
1.  Run the worker scripts to set up task-processing nodes. Replace `worker-x` with a unique worker ID for each worker ran.  
   ```console
   python examples/worker.py worker-x
  ```
2.  Use the client script to send tasks to the workers:
   ```console
   python examples/client.py
  ```
3. Logs and Results
   - Task results are written to results.log.
   - General logs, including task processing details, are added to krq.log.
4. Before running the examples again, ensure you clear the Redis database to avoid conflicts:
   ```console
   redis-cli flushall
   ```

### Explanation of example scripts
The `examples` directory contains scripts for testing various components of the system. Below is a breakdown of the available files and their purposes:  

1. **`client.py`**  
   - Used for testing normal client-side task submission and interactions.  

2. **`worker.py`**  
   - Used for testing the normal operation of worker nodes, including task processing.  

3. **`client2.py`**  
   - Designed for testing scenarios with **chained tasks**, where multiple dependent tasks are submitted and executed sequentially.  

4. **`worker2.py`**  
   - Simulates situations involving **failed tasks** and **delayed processing**, enabling robustness testing for error handling and retries.  

Refer to these examples to understand the usage and functionality of the system in various scenarios.

## Template code

Below are examples of how to write worker and client code using the KRQ library to interact with the system.  

### Worker Code (`worker.py`)  

This script sets up a worker that listens for tasks, processes them, and returns results.  
```python3
import sys
from krq.core import KRQ

def task_func(task_type, args):
    if task_type == "add":
        return sum(args)
    else:
        raise ValueError("Unknown task type")

worker_id = sys.argv[1]
krq_worker = KRQ(broker="localhost:9092", backend="localhost")
krq_worker.config_worker(group_id="worker-group", topic=worker_id, worker_id=worker_id)
krq_worker.run(task_func)
```

### Client Code (`client.py`)  

This script submits tasks from the client side to the workers.  
```python3
from krq.core import KRQ

# Creates a KRQ client
krq_client = KRQ(broker="localhost:9092", backend="localhost")

# Send a "add" task to the queue
task_id = krq_client.send_task("add", [1, 2])
print(f"Submitted task with ID: {task_id}")

# Print out the status of task
print(f"Task {task_id} status:", krq_client.status(task_id))
```
## Features  

The distributed task queue includes the following capabilities:  

1. **Fault Tolerance**  
   - Automatically detects worker failures and reassigns tasks to ensure continuity.  

2. **Exponential Backoff and Retries**  
   - Implements a retry mechanism with exponential backoff for failed tasks to avoid overwhelming the system.  

3. **Task Reassignment**  
   - Dynamically reassigns tasks from inactive or failed workers to active ones, ensuring no task is left unprocessed.  

4. **Load Balancer**  
   - Evenly distributes tasks across available workers to maximize system efficiency and prevent overload.  

5. **Distributed Logger**  
   - Logs results, statuses, and worker activities in a distributed manner for easy debugging and monitoring.  
   - Results are stored in `results.log`, and general logs are stored in `krq.log`.  

6. **Task Chaining**  
   - Allows multiple tasks to be linked in a sequence where each task triggers the execution of the next.  
   - A final callback can be attached to handle all results after the last task completes. Example:  
     ```python
     tasks = [("add", [1, 2]), ("mul", [3, 4])]
     krq_client.chain_tasks(tasks, final_callback=my_callback_function)
     ```
   - Supports self-balancing and organization of task dependencies.  

7. **Task Broadcasting**  
   - Enables a single task to be broadcasted to all workers for parallel execution.  

8. **Group Tasks**  
   - Groups multiple tasks with dependencies on a final task. The group is executed such that the final task waits for all dependencies to complete. Example:  
     ```python
     tasks = [("add", [1, 2]), ("mul", [3, 4])]
     final_task = ("sub", [10, 5])
     krq_client.group_tasks(tasks, final_task)
     ```


