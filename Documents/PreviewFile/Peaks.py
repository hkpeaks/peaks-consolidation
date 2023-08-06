from typing import List, Tuple
import os
import sys
import polars as pl
from io import BytesIO

""" On August 4, 2023, I created my first Python app. This app is
    used to validate and preview CSV files. For every 1% position
    of a CSV file, it will extract one row for validation and 
    preview. On the screen, it will display 20 rows but will 
    output all validated rows to a disk file. If you have any
    issues with this app, please leave your message at 
    https://github.com/hkpeaks/pypeaks/issues. 

    The app will gradually expand to become an ETL Framework for 
    Polars (and/or Pandas, DuckDB, NumPy) with the implementation
    of the newly designed SQL statements.

    The ETL script will be compatible with the Peaks Consolidation
    https://github.com/hkpeaks/peaks-consolidation, 
    meaning you can run the script using Python with Polars 
    or purely using the Peaks runtime without Python.
    
    How to use this app:

    Python Peaks.py File or File Path

    e.g. python peaks data.csv

         python peaks "d:\your folder\data.csv"

         
    I have tested this app and found that the minimum number of 
    data rows it can handle is 1 (excluding the column name). 
    I also tested its ability to process a large file with 
    10 billion rows and a size of 41GB. However, I cannot confirm
    the upper limit of rows it can handle. Since the Python timer
    reported a time elapsed of 0.0 for many cases, I removed this
    measurement as it was meaningless. 

    How to change number of output sample rows to disk file:

    At the bottom of this code, you can change the default number 
    from 100. However, it is not recommended to increase this 
    value too much, as the app is designed to retrieve sample rows
    for validation rather than a large population of your dataset.
    Additionally, this app is written in Python and does not use 
    any third-party libraries. In the future, however, third-party
    libraries will be used to handle data transformation of large 
    datasets more efficiently. 

    csv_info, validate_byte, err = get_csv_info(file_path, 100)    

    To see demo please refer to https://youtu.be/71GHzDnEYno
    
    """




class CSV_Info:
    def __init__(self):
        self.total_column: int = 0
        self.validate_row: int = 0
        self.estimate_row: int = 0
        self.is_line_br_13_exist: bool = False
        self.is_line_br_10_exist: bool = False
        self.column_name: List[str] = []
        self.file_size: int = 0
        self.delimiter: bytes = b''

def get_byte_array_frequency_distribution(byte_array: bytes) -> dict:
    
    frequency_distribution = {}
    for item in byte_array:
        if item in frequency_distribution:
            frequency_distribution[item] += 1
        else:
            frequency_distribution[item] = 1
    return frequency_distribution

def get_current_row_frequency_distribution(filepath: str, file, start_byte: int):
    
    frequency_distribution = {}
    is_valid_row_exist = False
    sample_size = 0
    double_quote_count = 0
    current_row = []   

    while not is_valid_row_exist and sample_size < 100000:
        sample_size += 100       

        byte_array = file.read(sample_size)
        file.seek(start_byte)

        n = 0
        current_row = []
        is_first_line_break_exist = False
        is_second_line_break_exist = False

        if start_byte == 0:            
            is_first_line_break_exist = True

        while n < sample_size and not is_second_line_break_exist:

            if not is_first_line_break_exist and byte_array[n] == 10:
                is_first_line_break_exist = True

            elif is_first_line_break_exist and byte_array[n] == 10:
                is_second_line_break_exist = True

            if is_first_line_break_exist:
                if len(current_row) == 0 and byte_array[n] == 10:
                    pass
                else:
                    if byte_array[n] == 34:
                        double_quote_count += 1

                    if double_quote_count % 2 == 0:
                        current_row.append(byte_array[n])
            n += 1

        if is_second_line_break_exist and len(current_row) > 0:
            frequency_distribution = get_byte_array_frequency_distribution(current_row)
            is_valid_row_exist = True  
   
    return len(current_row), frequency_distribution, current_row

def skip_white_space(byte_array: bytes, start_byte: int, end_byte: int) -> Tuple[int, int]:
   
    while start_byte < end_byte and byte_array[start_byte] == 32:
        start_byte += 1

    while end_byte > start_byte and byte_array[end_byte - 1] == 32:
        end_byte -= 1

    return start_byte, end_byte

def get_column_name(filepath: str, file, delimiter: bytes) -> List[str]:
   
    isValidRowExist = False
    sample_size = 0   
    
    _delimiter = int.from_bytes(delimiter, byteorder='big', signed=False)

    while not isValidRowExist:
        column_count = 0
        double_quote_count = 0
        cell_address = []
        column_name = []

        sample_size += 100       

        file2 = open(filepath, 'rb')  

        byte_array = file2.read(sample_size)
        file2.seek(0)       

        n = 0

        cell_address.append(0)     

        while n < sample_size:          
            if byte_array[n] == _delimiter:
                if double_quote_count % 2 == 0:
                    cell_address.append(n + 1)
                    column_count += 1
            elif byte_array[n] == 10:
                cell_address.append(n + 1)
                isValidRowExist = True
                column_count += 1
                break
            elif byte_array[n] == 13:
                cell_address.append(n + 1)
            elif byte_array[n] == 34:
                double_quote_count += 1
            n += 1

        start_byte, end_byte = (0, 0)       

        for i in range(column_count):
            start_byte = cell_address[i]
            end_byte = cell_address[i + 1] - 1
            start_byte, end_byte = skip_white_space(byte_array, start_byte, end_byte)          
            column_name.append(byte_array[start_byte:end_byte].decode('utf-8'))            
   
    return column_name

def int_to_byte(n: int) -> bytes:
    return n.to_bytes((n.bit_length() + 7) // 8, 'big')

def get_csv_info(filepath: str, sample_row: int) -> Tuple[CSV_Info, bytes, Exception]:
    
    error_message = ''
    csv_info = CSV_Info()

    try:
        file = open(filepath, 'rb')        
        file2 = os.stat(filepath)

        frequency_distribution = {}      
        current_row_byte = bytearray()      
        validate_byte = bytearray()
        start_byte = 0
        n = 0
        current_row_byte_count = 0
        sample_byte_count = 0

        frequency_distribution_by_sample = {}
        delimiter_scenario = {}

        csv_info.file_size = file2.st_size

        if sample_row <= 0 or csv_info.file_size < 10000:
            sample_row = 10

        if csv_info.file_size < 1000 or sample_row < 2:
            sample_row = 2

        ## Column Name
        current_row_byte_count, frequency_distribution, current_row_byte = get_current_row_frequency_distribution(filepath, file, 0)                   
        delimiter_scenario = frequency_distribution 

        ## Data Row
        while n <= sample_row - 1:
            start_byte += 1
            current_row_byte_count, frequency_distribution, current_row_byte = get_current_row_frequency_distribution(filepath, file, start_byte)                               
            validate_byte.extend(current_row_byte)
            sample_byte_count += current_row_byte_count             
            frequency_distribution_by_sample[n] = frequency_distribution
            temp_delimiter_scenario = {}

            if n > 0:

                for key in delimiter_scenario:
                    if key in frequency_distribution_by_sample[n]:
                        if frequency_distribution_by_sample[n][key] == delimiter_scenario[key]:
                            temp_delimiter_scenario[key] = delimiter_scenario[key]

                delimiter_scenario = temp_delimiter_scenario

            start_byte = csv_info.file_size * n // sample_row
            n += 1

        csv_info.validate_row = n

    except FileNotFoundError as e:
        error_message += f'** File "{filepath}" not found **'
        return csv_info, validate_byte, e

    delimiter_exclude_line_br = {}

    for key in delimiter_scenario.keys():
        if key != 10 and key != 13:
            delimiter_exclude_line_br[key] = delimiter_scenario[key]
        else:
            if key == 10:
                csv_info.is_line_br_10_exist = True
            elif key == 13:
                csv_info.is_line_br_13_exist = True

    delimiter_exclude_abc123 = {}    

    if 44 in delimiter_exclude_line_br:
        csv_info.delimiter = b','
        csv_info.total_column = delimiter_exclude_line_br[44] + 1

    elif len(delimiter_exclude_line_br) == 1:
        
        for key in delimiter_exclude_line_br:
            if (0 <= key <= 47) or (58 <= key <= 64) or (91 <= key <= 96) or (key >= 123):
                csv_info.delimiter = int_to_byte(key)
                csv_info.total_column = delimiter_exclude_line_br[key] + 1

    elif len(delimiter_exclude_line_br) > 1:
        for key in delimiter_exclude_line_br.keys():
            if (key >= 0 and key <= 47) or (key >= 58 and key <= 64) or (key >= 91 and key <= 96) or (key >= 123):
                delimiter_exclude_abc123[key] = delimiter_exclude_line_br[key]

    if len(delimiter_exclude_abc123) == 1:
        for key in delimiter_exclude_abc123.keys():
            csv_info.delimiter = int_to_byte(key)
            csv_info.total_column = delimiter_exclude_line_br[key] + 1

    elif len(delimiter_exclude_abc123) > 1:
        error_message += '** More than one possible delimiter ** \n'
        for key in delimiter_exclude_abc123.keys():
            error_message += f'   ASCII{int(key)} ({chr(key)})\n'

        return csv_info, validate_byte, Exception(error_message)   
   
   
    if error_message == '':
        csv_info.column_name = get_column_name(filepath, file, csv_info.delimiter)
       
        csv_info.estimate_row = csv_info.file_size // sample_byte_count * sample_row

        if len(delimiter_exclude_line_br) == 0:
            error_message += '** Fail to find delimiter ** \n'
        else:
            if csv_info.total_column == 0:
                error_message += '** Fail to count number of column ** \n'

            if csv_info.estimate_row == 0:
                error_message += '** Fail to estimate number of row ** \n'

            if len(csv_info.column_name) == 0:
                error_message += '** Fail to find any column name ** \n'

            if len(csv_info.column_name) != csv_info.total_column:
                error_message += f'** Number of column name is {len(csv_info.column_name)}, but number of column is {csv_info.total_column} ** \n'

    print(error_message)

    if len(error_message) > 0:
        raise Exception(error_message)

    return csv_info, validate_byte, None

def number_display_format(num: float) -> str:
    
    num_str = f'{num:.16g}'
    parts = num_str.split('.')
    num_of_digits = len(parts[0])
    num_of_commas = (num_of_digits - 1) // 3

    for i in range(1, num_of_commas + 1):
        comma_index = num_of_digits - i * 3
        parts[0] = parts[0][:comma_index] + ',' + parts[0][comma_index:]

    return '.'.join(parts)

def cell_address(byte_array: bytes, csv_info: CSV_Info) -> List[int]:
   
    read_csv_delimiter = int.from_bytes(csv_info.delimiter, byteorder='big', signed=False)

    cell_address = []
    double_quote_count = 0

    cell_address.append(0)
    row = 0

    for i in range(len(byte_array)):
        if byte_array[i] == read_csv_delimiter:
            if double_quote_count != 1:
                cell_address.append(i + 1)
                double_quote_count = 0
        elif byte_array[i] == 10:
            cell_address.append(i + 1)
            row += 1
        elif byte_array[i] == 13:
            cell_address.append(i + 1)
        elif byte_array[i] == 34:
            double_quote_count += 1     
 
    return cell_address

def current_view(byte_array: bytearray, csv_info: CSV_Info, start_column: int, end_column: int, total_row: int):
    _cell_address = cell_address(byte_array, csv_info)

    extra_line_br_char = 1 if csv_info.is_line_br_13_exist else 0

    start_byte, end_byte = 0, 0
    max_col = csv_info.total_column + extra_line_br_char - 1
    cell = len(_cell_address) - (max_col + 1)
    n = 0
    current_row = 0
    temp_bytes = bytearray()
    read_csv_delimiter = int.from_bytes(csv_info.delimiter, byteorder='big', signed=False)
    max_text_length = {}
    max_integer_length = {}
    max_decimal_length = {}
    max_column_width = {}
    is_column_real_number = {}   

    for current_column in range(start_column, end_column):
        max_text_length[current_column] = 1
        max_integer_length[current_column] = 1
        max_decimal_length[current_column] = 1
        max_column_width[current_column] = len(csv_info.column_name[current_column])      
        is_column_real_number[current_column] = True     

    while n < cell:
        for current_column in range(start_column, end_column):
            if current_column > 0:
                temp_bytes.append(read_csv_delimiter)

            current_cell = (max_col + 1) * current_row + current_column
            start_byte = _cell_address[current_cell]
            end_byte = _cell_address[current_cell + 1] - 1
            start_byte, end_byte = skip_white_space(byte_array, start_byte, end_byte)

            is_real_number = True
            is_float = False
            is_negative_sign_exist = False
            is_open_bracket = False
            is_close_bracket = False
            integer_length = 0
            decimal_length = 0
            is_dot_exist = False
            text_length = 0

            for current_byte in range(start_byte, end_byte):
                if byte_array[current_byte] == ord('.'):
                    if is_float:
                        is_float = False
                        is_real_number = False
                    is_float = True
                    if current_byte == start_byte or current_byte == end_byte - 1:
                        is_real_number = False
                elif byte_array[current_byte] == ord('-'):
                    if is_negative_sign_exist:
                        is_negative_sign_exist = False
                        is_real_number = False
                    if current_byte == start_byte:
                        is_negative_sign_exist = True
                    else:
                        is_real_number = False
                elif byte_array[current_byte] == ord('('):
                    if current_byte == start_byte:
                        is_open_bracket = True
                    else:
                        is_real_number = False
                elif byte_array[current_byte] == ord(')'):
                    if current_byte == end_byte - 1:
                        is_close_bracket = True
                    else:
                        is_real_number = False
                else:
                    if byte_array[current_byte] < 48 or byte_array[current_byte] > 57:
                        is_real_number = False

            if is_negative_sign_exist:
                is_real_number = True

            if is_open_bracket and is_close_bracket:
                is_real_number = True

            if is_real_number:
                for current_byte in range(start_byte, end_byte):
                    if byte_array[current_byte] == ord('.'):
                        is_dot_exist = True
                    else:
                        if is_dot_exist:
                            decimal_length += 1
                        else:
                            integer_length += 1
              
                if integer_length > max_integer_length[current_column]:
                    max_integer_length[current_column] = integer_length

                if decimal_length >max_decimal_length[current_column]:
                    max_decimal_length[current_column] = decimal_length

            is_column_real_number[current_column] &= is_real_number

            text_length = int(end_byte - start_byte)

            if text_length > max_text_length[current_column]:
                max_text_length[current_column] = text_length

            temp_bytes.extend(byte_array[start_byte:end_byte])

        n += max_col + 1
        current_row += 1
        if current_row >= total_row:
            break
        temp_bytes.clear()       

    for current_column in range(start_column, end_column):
        current_length = max_integer_length[current_column] + max_decimal_length[current_column] + 1

        if is_column_real_number[current_column]:
            if max_column_width[current_column] < current_length:
                max_column_width[current_column] = current_length
        else:
            if max_column_width[current_column] < max_text_length[current_column]:
                max_column_width[current_column] = max_text_length[current_column]

    result_bytes = bytearray([32, 32])

    for current_column in range(start_column, end_column):  # Display Column Name
        current_columm_name = csv_info.column_name[current_column]
        current_length = len(current_columm_name)

        if is_column_real_number[current_column]:
            if max_column_width[current_column] > current_length:
                result_bytes.extend([32] * (max_column_width[current_column] - current_length))

            result_bytes.extend(current_columm_name.encode())

            result_bytes.extend([32, 32])
        else:
            result_bytes.extend(current_columm_name.encode())

            if max_column_width[current_column] > current_length:
                result_bytes.extend([32] * (max_column_width[current_column] - current_length))

            result_bytes.extend([32, 32])

    result_bytes.extend([13, 10])

    n = 0
    current_row = 0

    while n < cell:  # Display body
        result_bytes.extend([32, 32])
       
        for current_column in range(start_column, end_column):
            current_cell = (max_col + 1) * current_row + current_column
            start_byte = _cell_address[current_cell]
            end_byte = _cell_address[current_cell + 1] - 1
            start_byte, end_byte = skip_white_space(byte_array, start_byte, end_byte)

            if is_column_real_number[current_column]:
                if max_decimal_length[current_column] == 0:  # Integer only
                    current_length = int(end_byte - start_byte)

                    if max_column_width[current_column] > current_length:
                        result_bytes.extend([32] * (max_column_width[current_column] - current_length))

                    result_bytes.extend(byte_array[start_byte:end_byte])

                    result_bytes.extend([32, 32])
                else:  # Decimal only
                    current_length = int(end_byte - start_byte)
                    current_integer_length = 0

                    for current_byte in range(start_byte, end_byte):
                        if byte_array[current_byte] == ord('.'):
                            break
                        current_integer_length += 1

                    left_space = max_column_width[current_column] - max_integer_length[current_column] - max_decimal_length[current_column] - 1

                    result_bytes.extend([32] * left_space)

                    integer_space = 0

                    if max_integer_length[current_column] > current_integer_length:
                        result_bytes.extend([32] * (max_integer_length[current_column] - current_integer_length))
                        integer_space += max_integer_length[current_column] - current_integer_length

                    result_bytes.extend(byte_array[start_byte:end_byte])

                    right_space = max_column_width[current_column] - current_length - integer_space - left_space

                    if max_column_width[current_column] > current_length:
                        result_bytes.extend([32] * right_space)

                    result_bytes.extend([32, 32])
            else:  # Text only
                current_length = int(end_byte - start_byte)

                result_bytes.extend(byte_array[start_byte:end_byte])

                if max_column_width[current_column] > current_length:
                    result_bytes.extend([32] * (max_column_width[current_column] - current_length))

                result_bytes.extend([32, 32])

        result_bytes.extend([13, 10])

        n += max_col + 1
        current_row += 1
        if current_row >= total_row:
            break

    print(result_bytes.decode())

def max_column_width(byte_array: bytearray, csv_info):
    
    _cell_address = cell_address(byte_array, csv_info)

    is_zero_row = len(byte_array) == 0

    start_byte = 0
    end_byte = 0

    extra_line_br_char = 1 if csv_info.is_line_br_13_exist else 0

    max_col = csv_info.total_column + extra_line_br_char - 1
    cell = len(_cell_address) - (max_col + 1)
    total_column = csv_info.total_column
    n = 0
    current_row = 0
    temp_bytes = []
    read_csv_delimiter = csv_info.delimiter
    max_text_length = {}
    max_integer_length = {}
    max_decimal_length = {}
    max_column_width = {}
    is_column_real_number = {}

    for current_column in range(total_column):
        max_text_length[current_column] = 1
        max_integer_length[current_column] = 1
        max_decimal_length[current_column] = 1
        max_column_width[current_column] = len(csv_info.column_name[current_column])
        is_column_real_number[current_column] = True

    if not is_zero_row:
        while n < cell:
            for current_column in range(total_column):
                if current_column > 0:
                    temp_bytes.append(read_csv_delimiter)

                current_cell = (max_col + 1) * current_row + current_column
                start_byte = _cell_address[current_cell]
                end_byte = _cell_address[current_cell + 1] - 1
                start_byte, end_byte = skip_white_space(byte_array, start_byte, end_byte)

                is_real_number = True
                is_float = False
                is_negative_sign_exist = False
                is_open_bracket = False
                is_close_bracket = False
                integer_length = 0
                decimal_length = 0
                is_dot_exist = False
                text_length = 0

                for current_byte in range(start_byte, end_byte):
                    if byte_array[current_byte] == ord('.'):
                        if is_float:
                            is_float = False
                            is_real_number = False
                        is_float = True
                        if current_byte == start_byte or current_byte == end_byte - 1:
                            is_real_number = False
                    elif byte_array[current_byte] == ord('-'):
                        if is_negative_sign_exist:
                            is_negative_sign_exist = False
                            is_real_number = False
                        if current_byte == start_byte:
                            is_negative_sign_exist = True
                        else:
                            is_real_number = False
                    elif byte_array[current_byte] == ord('('):
                        if current_byte == start_byte:
                            is_open_bracket = True
                        else:
                            is_real_number = False
                    elif byte_array[current_byte] == ord(')'):
                        if current_byte == end_byte - 1:
                            is_close_bracket = True
                        else:
                            is_real_number = False
                    else:
                        if byte_array[current_byte] < 48 or byte_array[current_byte] > 57:
                            is_real_number = False

                if is_negative_sign_exist:
                    is_real_number = True

                if is_open_bracket and is_close_bracket:
                    is_real_number = True

                if is_real_number:
                    for current_byte in range(start_byte, end_byte):
                        if byte_array[current_byte] == ord('.'):
                            is_dot_exist = True
                        else:
                            if is_dot_exist:
                                decimal_length += 1
                            else:
                                integer_length += 1

                    if integer_length > max_integer_length[current_column]:
                        max_integer_length[current_column] = integer_length

                    if decimal_length > max_decimal_length[current_column]:
                        max_decimal_length[current_column] = decimal_length

                is_column_real_number[current_column] &= is_real_number

                text_length = int(end_byte - start_byte)

                if text_length > max_text_length[current_column]:
                    max_text_length[current_column] = text_length

                temp_bytes.extend(byte_array[start_byte:end_byte])

            n += max_col + 1
            current_row += 1
            if current_row > 10:
                break
            temp_bytes.clear()

    for current_column in range(total_column):
        current_length = max_integer_length[current_column] + max_decimal_length[current_column] + 1

        if is_column_real_number[current_column]:
            if max_column_width[current_column] < current_length:
                max_column_width[current_column] = current_length
        else:
            if max_column_width[current_column] < max_text_length[current_column]:
                max_column_width[current_column] = max_text_length[current_column]

    return is_zero_row, max_column_width

def view(byte_array, csv_info):

    is_zero_row, _max_column_width = max_column_width(byte_array, csv_info)

    if not is_zero_row:
        total_width = 0
        current_width = 0

        for i in range(len(_max_column_width)):
            total_width += _max_column_width[i]

        total_row = 20
        table_count = 1

        print()

        current_column = 0
        start_column = 0

        if total_width > 150:
            total_row = 8

            while current_column < len(_max_column_width):
                current_width += _max_column_width[current_column]
                if current_width > 100 * table_count:
                    current_view(byte_array, csv_info, start_column, current_column, total_row)
                    start_column = current_column
                    table_count += 1

                current_column += 1

            current_view(byte_array, csv_info, start_column, len(_max_column_width), total_row)

        elif total_width > 100:
            total_row = 8

            while current_column < len(_max_column_width):
                current_width += _max_column_width[current_column]
                if current_width > total_width / 2:
                    current_view(byte_array, csv_info, 0, current_column, total_row)
                    start_column = current_column
                    break
                current_column += 1

            current_view(byte_array, csv_info, start_column, len(_max_column_width), total_row)

        else:
            current_view(byte_array, csv_info, 0, len(_max_column_width), total_row)

def write_csv_sample_file(byte_array, csv_info):
    csv_string = csv_info.column_name[0]

    for i in range(1, len(csv_info.column_name)):
        csv_string += ","
        csv_string += csv_info.column_name[i]

    csv_string += "\r\n"

    column_name_byte_array = bytearray(csv_string, encoding='utf-8')

    try:    
        with open("%Sample.csv", "wb") as f:
            f.write(column_name_byte_array)
            f.write(byte_array)            
            print("A file named %Sample.csv is created from the rows that executed validation.")
            print()
    except Exception as err:
        print("*** Fail to write file ***")
        print(err)



if len(sys.argv) == 1:
    file_path = "Fact.csv"
elif len(sys.argv) == 2:
    file_path = sys.argv[1]

csv_info, validate_byte, err = get_csv_info(file_path, 100)

if err:
    print()
    print(err)
else:

    view(validate_byte, csv_info)
    write_csv_sample_file(validate_byte, csv_info)

    print("File Size: " + number_display_format(float(csv_info.file_size)) + " bytes", end =" ")
    print("  Total Column: ", number_display_format(float(csv_info.total_column)))
    print("Validated Row: ", number_display_format(float(csv_info.validate_row)), end =" ")
    print("  Estimated Row: ", number_display_format(float(csv_info.estimate_row)))

    print("Column Name: ", end =" ")

    for i in range(len(csv_info.column_name)):
        if i < len(csv_info.column_name) - 1:
            print(csv_info.column_name[i], end=",")
        else:
            print(csv_info.column_name[i])

    if csv_info.delimiter == 0:
        print("Delimiter: ")
    else:              
        print("Delimiter: " + number_display_format(int.from_bytes(csv_info.delimiter, byteorder='big', signed=False)) + " [" + str(csv_info.delimiter) + "]")

    print("Is Line Br 10/13 Exist: ", csv_info.is_line_br_10_exist, "/", csv_info.is_line_br_13_exist)


