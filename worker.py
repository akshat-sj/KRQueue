import sys
import yadtq


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
yadtq_worker = yadtq.YADTQ(broker="localhost:9092", backend="localhost")
yadtq_worker.config_worker(group_id="worker-group", topic="task_queue", worker_id=worker_id)

yadtq_worker.run(task_func)

