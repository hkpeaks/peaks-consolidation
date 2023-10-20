import argparse
import time
import subprocess

def print_banner(command):
    banner = '*' * 50
    print(f"\n{banner}\n*{command.center(48)}*\n{banner}")

def run_script(script, filename):
    command = f"python {script} {filename}"
    if script.endswith('.r'):
        command = f"Rscript {script} {filename}"
    print_banner(command)
    start = time.time()
    subprocess.run(command.split())
    end = time.time()
    elapsed_time = round(end - start, 3)
    
    return command, elapsed_time

def print_table(records):
    # Print the table using Python's built-in print function
    max_command_len = max(len(command) for command, _ in records)
    print("\n{:<{}}{:>20s}".format("Script", max_command_len, "Elapsed Time"))
    for command, elapsed_time in records:
        print("{:<{}}{:>20s}".format(command, max_command_len, f"{elapsed_time:.3f}s"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run scripts with a specified filename")
    parser.add_argument("filename", help="The filename to use")
    args = parser.parse_args()

    scripts = ["py-peakrs.py", "py-polars.py", "py-duckdb.py", "py-pandas.py", "r-data.table.r"]
    records = []
    for script in scripts:
        command, elapsed_time = run_script(script, args.filename)
        records.append((command, elapsed_time))
        print_table(records)
