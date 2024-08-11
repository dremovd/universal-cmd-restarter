import argparse
import subprocess
import time
import threading
import re
import signal
import select
import os
import psutil
from datetime import datetime, timedelta
import fcntl

stop_flag = threading.Event()

def set_nonblocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

def terminate_process(process, worker_id):
    if process is None:
        return

    print(f"Worker {worker_id}: Forcefully terminating process...")
    try:
        parent = psutil.Process(process.pid)
        children = parent.children(recursive=True)
        
        # Attempt to read any remaining output before termination
        try:
            while True:
                line = process.stdout.readline()
                if not line:
                    break
                print(f"Worker {worker_id}: {line.strip()}")
        except Exception as e:
            print(f"Worker {worker_id}: Error reading output before termination - {str(e)}")
        
        for child in children:
            child.terminate()
        parent.terminate()

        gone, alive = psutil.wait_procs(children + [parent], timeout=3)
        for p in alive:
            print(f"Worker {worker_id}: Force killing process {p.pid}")
            p.kill()

    except psutil.NoSuchProcess:
        print(f"Worker {worker_id}: Process already terminated")
    except Exception as e:
        print(f"Worker {worker_id}: Error while terminating process - {str(e)}")
    
    try:
        os.kill(process.pid, 0)
        print(f"Worker {worker_id}: Process still exists. Force killing...")
        os.kill(process.pid, signal.SIGKILL)
    except OSError:
        pass
    
    print(f"Worker {worker_id}: Process termination completed")

def continuously_read_output(process, worker_id, silent):
    set_nonblocking(process.stdout.fileno())
    output_buffer = []
    try:
        while True:
            time.sleep(0.1)  # Adjust timing based on the expected update frequency
            try:
                output = process.stdout.read()
                if output:
                    output_buffer.append(output)
                    if '\n' in output:
                        full_lines = ''.join(output_buffer).splitlines()
                        for line in full_lines:
                            if not silent:
                                print(f"Worker {worker_id}: {line}")  # Process each line as needed
                        output_buffer = []
            except IOError:
                # Handle expected exception due to no available output
                pass
            if process.poll() is not None:
                # Process has terminated, handle remaining output
                remaining_output = process.stdout.read()
                if remaining_output:
                    print(remaining_output)
                break
    finally:
        if output_buffer:
            print(''.join(output_buffer))  # Print any remaining output

def run_worker(command, worker_id, silent, no_output_timeout, restart_pattern):
    print(f"Starting Worker {worker_id}" if not silent else f"Worker {worker_id}: Started")

    process = None
    last_output_time = datetime.now()
    
    while not stop_flag.is_set():
        if process is None or process.poll() is not None:
            if process is not None:
                print(f"Worker {worker_id}: Restarting")
                terminate_process(process, worker_id)
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1, text=True)
            last_output_time = datetime.now()

        continuously_read_output(process, worker_id, silent)

        if datetime.now() - last_output_time > timedelta(minutes=no_output_timeout):
            print(f"Worker {worker_id}: No output detected for {no_output_timeout} minutes. Restarting...")
            terminate_process(process, worker_id)
            process = None
            last_output_time = datetime.now()

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
    parser.add_argument("--no-output-timeout", type=int, default=60, help="Timeout in minutes for no output before restarting")
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
