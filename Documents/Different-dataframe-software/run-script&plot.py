import argparse
import time
import subprocess
import matplotlib.pyplot as plt

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

def plot_chart(records, filename):
    # Extract script names and elapsed times
    scripts = [record[0] for record in records]
    elapsed_times = [record[1] for record in records]

    # Create bar chart
    plt.barh(scripts, elapsed_times, color='skyblue')

    plt.xlabel('Elapsed Time (s)')
    plt.title(f'Filter InnerJoin GroupBy for {filename}')
    plt.gca().invert_yaxis()  # Invert y-axis to have the fastest script at the top

    # Display the plot
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run scripts with a specified filename")
    parser.add_argument("filename", help="The filename to use")
    args = parser.parse_args()

    scripts = ["py-peakrs.py", "py-polars.py", "py-duckdb.py", "py-pandas.py", "r-datatable.r"]
    records = []
    for script in scripts:
        command, elapsed_time = run_script(script, args.filename)
        short_name = script.split('.')[0].split('-')[1]  # Extract short name from script name
        records.append((short_name, elapsed_time))
        print_table(records)
    
    # Plot chart after all scripts have run
    plot_chart(records, args.filename)
