# Demo video: https://youtu.be/Z78WjMRYs5o

import subprocess
import concurrent.futures
import time

start_time = time.time()

# specify the script you want to run
script = "peakrs.py"

# specify the argument you want to pass to the script
argument = "1M_Fact.csv"

def run_script(username):
    # create a new process to run the script with the argument and username
    process = subprocess.Popen(["python", script, argument, username])

    # wait for the process to complete
    process.wait()

# create a list of unique usernames
usernames = [f"user{i}" for i in range(1, 1001)]

# create a ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor() as executor:
    # use map to run the function in parallel for each username
    executor.map(run_script, usernames)

end_time = time.time()
print("Polars Duration (In Second): {}".format(round(end_time-start_time,3)))
