import redis
import uuid
import utils
import constants
from my_functions import dummy_function  # Import the dummy function from our separate module

# Connect to Redis (ensure the host and port match your configuration)
r = redis.Redis(host=constants.localhost, port=constants.rport, decode_responses=True)

# Serialize the dummy function once.
# Using a separate module for the function avoids issues with multiprocessing.
ser_fn = utils.serialize(dummy_function)

def submit_task(task_number, x, y):
    """
    Creates and submits a task to Redis.
    
    Args:
        task_number (int): Identifier for logging.
        x (int): First parameter for dummy_function.
        y (int): Second parameter for dummy_function.
    """
    # Serialize the parameters.
    # Our convention is that parameters are a tuple: ([args], {kwargs}).
    ser_params = utils.serialize(([x, y], {}))
    
    # Create a unique task ID.
    task_id = str(uuid.uuid4())
    
    # Construct the task data.
    task_data = {
        "fn_payload": ser_fn,
        "param_payload": ser_params,
        "status": "QUEUED",
        "result": None
    }
    
    # Store the task data in Redis under the key given by the task_id.
    r.set(task_id, utils.serialize(task_data))
    
    # Push the task_id onto the Redis list "tasks".
    r.lpush("tasks", task_id)
    
    print(f"Submitted task {task_number}: dummy_function({x}, {y}) with task_id: {task_id}")

def main():
    """
    Main function to submit several tasks for testing the multiprocessing dispatcher.
    """
    # For testing, let's submit 5 tasks with different parameters.
    for i in range(1, 6):
        x = i
        y = i * 2  # Example: different tasks with different arguments.
        submit_task(i, x, y)
    
    print("All test tasks have been submitted.")

if __name__ == "__main__":
    main()