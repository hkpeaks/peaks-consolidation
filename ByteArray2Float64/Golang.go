/*
Traditional indirect method: Convert to string and then convert to float64
float_number, err = strconv.ParseFloat(string(bytestream[start_byte:end_byte]), 64)

This code directly converts a bytearray to float64.
float_number = ByteArray2Float64(bytestream[start_byte:end_byte])

------------------------------------------------------------------------------------

This is a Go code that converts a byte array to a float64 number. The function takes in a byte array as an argument and returns a float64 number.

The function first initializes some variables and then iterates through the byte array. It checks if the byte array contains a dot, if it is negative or if it is an invalid number. It then checks if the integer part of the number is complete and if the decimal part of the number exists.

If the integer part of the number is complete, it moves on to the decimal part of the number. If it exists, it calculates the decimal part of the number and adds it to the integer part of the number.

Finally, it checks if the number is negative or invalid and returns 0 if it is invalid or returns the float64 number.

*/

func ByteArray2Float64(current_cell []byte) float64 {

	var float_number float64	
	var multiply10Pow, divide10Pow, position, offset, current_byte, left_byte int	
	var is_dot_exist, is_integer_complete, is_negative, is_invalid_number  = false, false, false, false
	
	var total_byte = len(current_cell)

	for current_byte < total_byte {
		switch current_cell[current_byte] {
		case '.':
			is_dot_exist = true
		case '-':
			current_cell[current_byte] = '0'
			is_negative = true
		case '(':
			fallthrough
		case ')':
			fallthrough
		
		default:
			if current_cell[current_byte] < 48 || current_cell[current_byte] > 57 {
				is_invalid_number = true
			}
		}		

		if !is_integer_complete {
			if is_dot_exist || (!is_dot_exist && current_byte == total_byte-1) {
				if !is_dot_exist && current_byte == total_byte-1 {
					multiply10Pow++
				}
				offset = 0
				for left_byte < multiply10Pow {
					current_digit := float64(current_cell[left_byte] - 48)
					position = 0
					for position+offset < multiply10Pow-1 {
						current_digit *= 10
						position++
					}
					offset++
					left_byte++
					float_number += current_digit
				}

				is_integer_complete = true
				divide10Pow++
				
			}
			multiply10Pow++

		} else if is_dot_exist {
			if current_byte == total_byte-1 {
				offset = 0

				for right_byte := total_byte - 1; right_byte >= total_byte-divide10Pow; right_byte-- {
					current_digit := float64(current_cell[right_byte] - 48) * 0.1
					position = 0
					for position+offset < divide10Pow-1 {
						current_digit *= 0.1
						position++
					}
					offset++
					float_number += current_digit
				}
			}
			divide10Pow++
		}		

		current_byte++
	}

	if is_negative == true {
		float_number *= -1
	}

	if is_invalid_number == true {
		float_number = 0
	}
	return float_number
}
