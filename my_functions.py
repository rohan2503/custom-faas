# my_functions.py
import time
import os

def dummy_function(x, y):
    """
    A dummy function that simulates a long-running task by sleeping for 5 seconds,
    then returns the sum of x and y.
    """
    print(f"Executing dummy_function in process {os.getpid()} with args: {x}, {y}")
    time.sleep(5)  # Simulate a time-consuming task
    return x + y