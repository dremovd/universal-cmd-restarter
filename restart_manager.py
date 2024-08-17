import argparse
import subprocess
import time
import threading
import re
import signal
import os
from datetime import datetime, timedelta

stop_flag = threading.Event()

def terminate_process(process, worker_id):
    if process is None:
        return

    print(f"Worker {worker_id}: Terminating process...")
    try:
        process.terminate()
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        print(f"Worker {worker_id}: Force killing process...")
        process.kill()

    print(f"Worker {worker_id}: Process terminated")

def run_worker(command, worker_id, silent, no_output_timeout, restart_pattern):
    if not silent:
        print(f"Starting Worker {worker_id}")
    else:
        print(f"Worker {worker_id}: Started")

    process = None
    last_output_time = datetime.now()
    
    while not stop_flag.is_set():
        if process is None or process.poll() is not None:
            if process is not None:
                print(f"Worker {worker_id}: Restarting")
                terminate_process(process, worker_id)
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=1,
                universal_newlines=True
            )

            last_output_time = datetime.now()

        try:
            output = process.stdout.readline()
            if output:
                last_output_time = datetime.now()
                if not silent:
                    print(f"Worker {worker_id}: {output.strip()}")
                if re.search(restart_pattern, output):
                    last_output_time = datetime.now()

            elif datetime.now() - last_output_time > timedelta(minutes=no_output_timeout):
                print(f"Worker {worker_id}: No output detected for {no_output_timeout} minutes. Restarting...")
                terminate_process(process, worker_id)
                process = None
                last_output_time = datetime.now()

        except Exception as e:
            print(f"Worker {worker_id}: Error - {str(e)}. Restarting...")
            terminate_process(process, worker_id)
            process = None
            time.sleep(5)

    if process:
        print(f"Worker {worker_id}: Stopping")
        terminate_process(process, worker_id)

def signal_handler(signum, frame):
    print("\nCtrl+C pressed. Stopping all workers...")
    stop_flag.set()

def main():
    parser = argparse.ArgumentParser(description="Universal restart manager")
    parser.add_argument("command", help="Command to run in each worker instance")
    parser.add_argument("instances", type=int, help="Number of instances to run in parallel")
    parser.add_argument("restart_pattern", help="Regular expression pattern to check for successful execution or heartbeat")
    parser.add_argument("--silent", action="store_true", help="Enable silent mode (only output logs about starting/restarting workers)")
    parser.add_argument("--no-output-timeout", type=int, default=5, help="Timeout in minutes for no output before restarting")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, signal_handler)

    if not args.silent:
        print(f"Starting {args.instances} workers")

    threads = []
    for i in range(args.instances):
        thread = threading.Thread(target=run_worker, args=(args.command, i, args.silent, args.no_output_timeout, args.restart_pattern))
        thread.start()
        threads.append(thread)
        time.sleep(1)

    for thread in threads:
        thread.join()

    if not args.silent:
        print("All workers have finished")

if __name__ == "__main__":
    main()
