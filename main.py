import time
import random
import threading
import queue

# --- Configuration ---
NUM_TASKS = 19 # As mentioned in the article: "Zirve Yük: 19 İş" (Peak Load: 19 Jobs)
NUM_WORKERS = 5 # A reasonable number of workers for queue mode simulation
TASK_MIN_DURATION = 0.5 # Minimum time a task takes (seconds)
TASK_MAX_DURATION = 1.5 # Maximum time a task takes (seconds)

def simulate_n8n_task(task_id):
    """
    Simulates a single n8n workflow execution.
    In a real n8n scenario, this would be an actual workflow running.
    """
    duration = random.uniform(TASK_MIN_DURATION, TASK_MAX_DURATION)
    print(f"[Task {task_id}] Starting task (simulated work for {duration:.2f}s)...")
    time.sleep(duration) # Simulate work being done
    print(f"[Task {task_id}] Finished task.")
    return f"Task {task_id} completed in {duration:.2f}s"

def run_normal_mode():
    """
    Simulates n8n's 'Normal' (synchronous) execution mode.
    Each workflow runs one after another.
    """
    print("\n--- Simulating n8n Normal Mode (Synchronous) ---")
    start_time = time.time()
    results = []
    for i in range(1, NUM_TASKS + 1):
        # In normal mode, tasks are processed sequentially.
        # If one task is long, it blocks the next from starting.
        result = simulate_n8n_task(i)
        results.append(result)
    end_time = time.time()
    print(f"\n--- Normal Mode Summary ---")
    print(f"Total tasks processed: {NUM_TASKS}")
    print(f"Total time taken: {end_time - start_time:.2f} seconds")
    print("Results:", results)

def worker(task_queue, results_list, worker_id):
    """
    Worker function for queue mode. Continuously processes tasks from the queue.
    """
    print(f"[Worker {worker_id}] Started.")
    while True:
        try:
            task_id = task_queue.get(timeout=1) # Get a task from the queue, with a timeout
            print(f"[Worker {worker_id}] Picking up Task {task_id}...")
            result = simulate_n8n_task(task_id)
            results_list.append(result)
            task_queue.task_done() # Signal that the task is done
        except queue.Empty:
            # If the queue is empty for a while, the worker can exit or wait longer.
            # For this example, we'll let the main thread join to ensure all tasks are done.
            pass
        except Exception as e:
            print(f"[Worker {worker_id}] Error processing task: {e}")
            task_queue.task_done() # Still mark as done to prevent deadlock

def run_queue_mode():
    """
    Simulates n8n's 'Queue' (asynchronous with workers) execution mode.
    Tasks are added to a queue, and a pool of workers processes them concurrently.
    """
    print("\n--- Simulating n8n Queue Mode (Asynchronous with Workers) ---")
    task_queue = queue.Queue()
    results_list = [] # Shared list to store results from workers

    # Start worker threads
    workers = []
    for i in range(NUM_WORKERS):
        worker_thread = threading.Thread(target=worker, args=(task_queue, results_list, i + 1))
        worker_thread.daemon = True # Allow main program to exit even if workers are still running
        worker_thread.start()
        workers.append(worker_thread)

    # Add tasks to the queue
    start_time = time.time()
    for i in range(1, NUM_TASKS + 1):
        # In queue mode, tasks are quickly added to a queue.
        # Workers pick them up as soon as they are free, allowing parallel execution.
        task_queue.put(i)
        print(f"Main: Added Task {i} to queue.")

    # Wait for all tasks to be processed
    print("\nMain: All tasks added to queue. Waiting for workers to finish...")
    task_queue.join() # Blocks until all items in the queue have been gotten and processed.
    end_time = time.time()

    print(f"\n--- Queue Mode Summary ---")
    print(f"Total tasks processed: {NUM_TASKS}")
    print(f"Total workers: {NUM_WORKERS}")
    print(f"Total time taken: {end_time - start_time:.2f} seconds")
    print("Results:", results_list)

if __name__ == "__main__":
    run_normal_mode()
    print("\n" + "="*50 + "\n") # Separator
    run_queue_mode()
    print("\n" + "="*50 + "\n")
    print("Comparison: Queue mode typically finishes faster for I/O-bound or concurrent tasks,")
    print("as multiple tasks can run in parallel, unlike normal (synchronous) mode.")
    print(f"This is especially true under 'peak load' like {NUM_TASKS} jobs.")
