import threading
import time

stop_flag = threading.Event()

def worker():
    print("Worker starting")
    while not stop_flag.is_set():
        print("Working...")
        time.sleep(1)
    print("Worker stopping cleanly")

t = threading.Thread(target=worker)
t.start()

time.sleep(5)
print("Signalling stop")
stop_flag.set()   # flips flag + wakes thread
t.join()
print("Thread has exited")


