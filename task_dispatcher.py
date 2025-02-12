import redis
import utils
import sys
import uuid
# import zmq
import concurrent.futures
import time
import heapq
import constants
import custom_exceptions

r = redis.Redis(
    host=constants.localhost,
    port=constants.rport,
    decode_responses=True,
)


def execute_task(
    task_id: uuid.UUID, ser_fn: str, ser_params: str
) -> tuple[str, str, str]:
    """Function to deserialize and run the function payload.

    Args:
        task_id (uuid.UUID): ID of the task with the function payload
        ser_fn (str): Serialized function payload
        ser_params (str): Serialized function parameters

    Returns:
        tuple(task_id, status, result): (ID of the task executed,
                                         Status after the function payload run,
                                         Result of the function run)
    """
    # Deserialize function and parameters
    fn = utils.deserialize(ser_fn)
    params = utils.deserialize(ser_params)
    args, kwargs = params

    # Execute the function
    try:
        result_payload = fn(*args, **kwargs)
        status = constants.COMPLETED

    except Exception as exp:
        result_payload = f"Exception occured while executing the function. ERROR: {exp}"
        status = constants.FAILED

    # Serialize and return the results
    return (task_id, status, utils.serialize(result_payload))


def LocalTaskDispatcher(num_worker_processors=8) -> None:
    """Task Dispatcher to manage Local Workers

    Args:
        num_worker_processors (int, optional): Number of local workers/processors. Defaults to 8.
    """

    def update_redis_local_task_dispatcher(future: concurrent.futures.Future):
        """Function to update Redis with result of the future object.

        Args:
            future (concurrent.futures.Future): Future object
        """
        task_id, status, result_payload = future.result()

        task = utils.deserialize(r.get(task_id))
        task["status"] = status
        task["result"] = result_payload

        r.set(task_id, utils.serialize(task))
        print(f"Updated task {task_id}: status={status}")

    with concurrent.futures.ProcessPoolExecutor(
        max_workers=num_worker_processors
    ) as executor:
        print("Local dispatcher started. Waiting for tasks...")
        while True:
            # Listen for task ids and assign the workload to a local worker
            if r.llen("tasks") > 0:
                task_id = r.lpop("tasks")

                if r.exists(task_id):
                    task = utils.deserialize(r.get(task_id))
                    ser_fn = task["fn_payload"]
                    ser_params = task["param_payload"]

                    future = executor.submit(execute_task, task_id, ser_fn, ser_params)

                    task["status"] = constants.RUNNING
                    r.set(task_id, utils.serialize(task))
                    print(f"Task {task_id} set to RUNNING")
                    future.add_done_callback(update_redis_local_task_dispatcher)

                else:
                    raise custom_exceptions.TaskNotFound(
                        f"TASK ID '{task_id}' NOT FOUND", task_id
                    )
            else:
                # No tasks available, sleep briefly to avoid busy waiting
                time.sleep(0.1)


def main():
    """
    Main method to start the local task dispatcher.
    You can optionally specify the number of worker processors as a command-line argument.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Local Task Dispatcher")
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=8,
        help="Number of worker processors (default: 8)",
    )
    args = parser.parse_args()

    print(f"Starting Local Task Dispatcher with {args.workers} worker processors...")
    LocalTaskDispatcher(num_worker_processors=args.workers)


if __name__ == "__main__":
    main()