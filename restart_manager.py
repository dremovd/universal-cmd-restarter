import subprocess
import threading
import time
import re
import signal
import psutil
from datetime import datetime, timedelta

stop_flag = threading.Event()

def process_output(stream, worker_id, log_file, silent):
    """
    Continuously reads from a stream (stdout or stderr), handling special characters
    and logging the final state of each line.
    """
    with open(log_file, 'w') as log:
        buffer = ""
        while True:
            output = stream.read(1)  # Read one character at a time
            if output == b'' and stream.closed:
                break
            if output:
                # Decode bytes to string
                output = output.decode('utf-8', errors='replace')

                # Handle carriage return (overwrite line)
                if output == '\r':
                    buffer = ""  # Clear the line buffer on carriage return
                elif output == '\n':
                    # When we hit a newline, log and print the current buffer
                    if buffer:
                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        log_entry = f"{timestamp} - Worker {worker_id}: {buffer.strip()}\n"
                        log.write(log_entry)
                        log.flush()  # Ensure it writes to file immediately
                        if not silent:
                            print(log_entry, end='')
                    buffer = ""  # Clear the buffer after processing a complete line
                else:
                    buffer += output  # Accumulate characters into the buffer

def run_worker(command, worker_id, silent, no_output_timeout, restart_pattern):
    log_file = f"worker_{worker_id}_output.log"
    process = None
    last_output_time = datetime.now()
    
    while not stop_flag.is_set():
        if process is None or process.poll() is not None:
            if process is not None:
                print(f"Worker {worker_id}: Restarting")
                terminate_process(process, worker_id)
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=False)
            last_output_time = datetime.now()

            # Start separate threads for stdout and stderr processing
            stdout_thread = threading.Thread(target=process_output, args=(process.stdout, worker_id, log_file, silent))
            stderr_thread = threading.Thread(target=process_output, args=(process.stderr, worker_id, log_file, silent))

            stdout_thread.start()
            stderr_thread.start()

            stdout_thread.join()
            stderr_thread.join()

        if datetime.now() - last_output_time > timedelta(minutes=no_output_timeout):
            print(f"Worker {worker_id}: No output detected for {no_output_timeout} minutes. Restarting...")
            terminate_process(process, worker_id)
            process = None
            last_output_time = datetime.now()

        time.sleep(1)  # Prevent tight loop

    if process:
        print(f"Worker {worker_id}: Stopping")
        terminate_process(process, worker_id)

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

def signal_handler(signum, frame):
    print("\nCtrl+C pressed. Stopping all workers...")
    stop_flag.set()

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Process output handling for terminal-based commands")
    parser.add_argument("command", help="Command to run in each worker instance")
    parser.add_argument("instances", type=int, help="Number of instances to run in parallel")
    parser.add_argument("restart_pattern", help="Regular expression pattern to check for successful execution or heartbeat")
    parser.add_argument("--silent", action="store_true", help="Enable silent mode (only output logs about starting/restarting workers)")
    parser.add_argument("--no-output-timeout", type=int, default=60, help="Timeout in minutes for no output before restarting")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, signal_handler)

    print(f"Starting {args.instances} workers")

    threads = []
    for i in range(args.instances):
        thread = threading.Thread(target=run_worker, args=(args.command, i, args.silent, args.no_output_timeout, args.restart_pattern))
        thread.start()
        threads.append(thread)
        time.sleep(1)

    for thread in threads:
        thread.join()

    print("All workers have finished")

if __name__ == "__main__":
    main()
