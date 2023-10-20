## Current folder must have folder "Inbox" which contain the zip file 1M_Fact.zip".

import os
import csv
import sys
import zipfile

def expand_file(original_file_path, expand_factor):
    print(f"\nProcessing: expand_file('{original_file_path}', {expand_factor})")
    
    # Separate the directory and file name
    dir_name, file_name = os.path.split(original_file_path)

    # Check if it's a zip file
    if zipfile.is_zipfile(original_file_path):
        with zipfile.ZipFile(original_file_path, 'r') as zip_ref:
            # Extract all files to the same directory
            zip_ref.extractall(dir_name)

            # Find the largest file in the zip file
            largest_file = max(zip_ref.namelist(), key=lambda f: zip_ref.getinfo(f).file_size)
            original_file_path = os.path.join(dir_name, largest_file)

    # Remove existing numeric prefix from file name
    while file_name[0].isdigit():
        file_name = file_name[1:]

    # Read the original file into memory as a byte array
    with open(original_file_path, 'rb') as f:
        data = f.read()

    # Check if it's a CSV file by reading the first 10000 bytes
    is_csv = False
    with open(original_file_path, 'rb') as f:
        sample = f.read(10000)
        try:
            dialect = csv.Sniffer().sniff(sample.decode('utf-8'))
            if dialect.delimiter == ',':
                is_csv = True
        except csv.Error:
            pass

    # Create the new file name with the same extension as the unzipped file
    new_file_name = os.path.join(dir_name, str(expand_factor) + os.path.splitext(file_name)[0] + os.path.splitext(original_file_path)[1])

    # Write the data to the new file expand_factor times
    with open(new_file_name, 'wb') as f:
        for i in range(expand_factor):
            if is_csv and i > 0:  # Skip column names for 2nd or subsequent datasets
                f.write(data[data.index(b'\n')+1:])
            else:
                f.write(data)
            print(i+1, end=' ')
            sys.stdout.flush()  # Force print to screen

# Usage
expand_file('Inbox/1M_Fact.zip', 10)
expand_file('Inbox/1M_Fact.zip', 100)
expand_file('Inbox/1M_Fact.zip', 1000)

