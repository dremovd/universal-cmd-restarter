import argparse
import subprocess
import time
import threading
import re
import signal
import select
import os
import psutil
import io
from datetime import datetime, timedelta

stop_flag = threading.Event()

def terminate_process(process, worker_id):
    if process is None:
        return

    print(f"Worker {worker_id}: Forcefully terminating process...")
    try:
        parent = psutil.Process(process.pid)
        children = parent.children(recursive=True)
        
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
            ready, _, _ = select.select([process.stdout, process.stderr], [], [], 1.0)
            if ready:
                for pipe in ready:
                    buffer = io.StringIO()
                    while True:
                        char = pipe.read(1)
                        if not char:
                            break
                        if char == '\r':
                            buffer.seek(0)
                            buffer.truncate()
                        elif char == '\n':
                            line = buffer.getvalue()
                            if not silent:
                                print(f"Worker {worker_id}: {line}")
                            if re.search(restart_pattern, line):
                                last_output_time = datetime.now()
                            buffer.seek(0)
                            buffer.truncate()
                        else:
                            buffer.write(char)
                    
                    # Print any remaining content in the buffer
                    remaining = buffer.getvalue()
                    if remaining and not silent:
                        print(f"Worker {worker_id}: {remaining}", end='', flush=True)
                    
                    if buffer.tell() > 0:
                        last_output_time = datetime.now()

            else:
                if process.poll() is None:
                    if datetime.now() - last_output_time > timedelta(seconds=300):
                        print(f"Worker {worker_id}: No output for 300 seconds. Checking process...")
                        process.send_signal(signal.SIGURG)
                        time.sleep(1)
                        if process.poll() is None:
                            print(f"Worker {worker_id}: Process is still running but unresponsive. Restarting...")
                            terminate_process(process, worker_id)
                            process = None
                            last_output_time = datetime.now()
                
            if datetime.now() - last_output_time > timedelta(minutes=no_output_timeout):
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
