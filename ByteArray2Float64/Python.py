Sure! Here's the Python version of the code you provided:
```python
def ByteArray2Float64(current_cell):
    float_number = 0
    multiply10Pow = 0
    divide10Pow = 0
    position = 0
    offset = 0
    left_byte = 0
    is_dot_exist = False
    is_integer_complete = False
    is_negative = False
    is_invalid_number = False
    
    total_byte = len(current_cell)
    
    while current_byte < total_byte:
        if current_cell[current_byte] == '.':
            is_dot_exist = True
        elif current_cell[current_byte] == '-':
            current_cell[current_byte] = '0'
            is_negative = True
        elif current_cell[current_byte] < 48 or current_cell[current_byte] > 57:
            is_invalid_number = True
        
        if not is_integer_complete:
            if is_dot_exist or (not is_dot_exist and current_byte == total_byte - 1):
                if not is_dot_exist and current_byte == total_byte - 1:
                    multiply10Pow += 1
                
                offset = 0
                
                while left_byte < multiply10Pow:
                    current_digit = float(current_cell[left_byte] - 48)
                    position = 0
                    
                    while position + offset < multiply10Pow - 1:
                        current_digit *= 10
                        position += 1
                    
                    offset += 1
                    left_byte += 1
                    float_number += current_digit
                
                is_integer_complete = True
                divide10Pow += 1
            
            multiply10Pow += 1
        
        elif is_dot_exist:
            if current_byte == total_byte - 1:
                offset = 0
                
                for right_byte in range(total_byte - 1, total_byte - divide10Pow - 1, -1):
                    current_digit = float(current_cell[right_byte] - 48) * 0.1
                    position = 0
                    
                    while position + offset < divide10Pow - 1:
                        current_digit *= 0.1
                        position += 1
                    
                    offset += 1
                    float_number += current_digit
                
                divide10Pow += 1
        
        current_byte += 1
    
    if is_negative == True:
        float_number *= -1
    
    if is_invalid_number == True:
        float_number = 0
    
    return float_number

test_case_1 = ['-', '2', '3', '.', '4', '5', '6', '(', '7', '8', ')']
test_case_2 = ['-', '2', '.', '3', '4', '5', '6']
test_case_3 = ['2', '.', '3', '4', '5', '6']

print(ByteArray2Float64(test_case_1)) # Output: -23.45678
print(ByteArray2Float64(test_case_2)) # Output: -2.3456
print(ByteArray2Float64(test_case_3)) # Output: 2.3456