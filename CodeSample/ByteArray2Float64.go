// This code is use to convert bytearray to float64 directly

// Traditional indirect method: Convert to string and then convert to float64
// float_number, err = strconv.ParseFloat(string(bytestream[start_byte:end_byte]), 64)

// Usage
// float_number = ByteArray2Float64(bytestream[start_byte:end_byte])

func ByteArray2Float64(current_cell []byte) float64 {
	var multiply10Pow, divide10Pow, digit, offset, current_byte, left_byte int
	var total_amount float64
	var is_dot_exist, is_integer_complete, is_negative, is_invalid_number bool
	is_dot_exist, is_integer_complete, is_negative, is_invalid_number = false, false, false, false
	total_amount, multiply10Pow, divide10Pow = 0, 0, 0

	var start_byte, total_byte = 0, len(current_cell)

	for current_byte < total_byte {

		if current_cell[current_byte] == 40 || current_cell[current_byte] == 41 || current_cell[current_byte] == 45 {
			current_cell[current_byte] = 48
			is_negative = true
		} else if current_cell[current_byte] == 46 {
			is_dot_exist = true
		} else if current_cell[current_byte] < 48 || current_cell[current_byte] > 57 {
			is_invalid_number = true
		}

		if !is_integer_complete {
			if is_dot_exist || (!is_dot_exist && current_byte == total_byte-1) {
				if !is_dot_exist && current_byte == total_byte-1 {
					multiply10Pow++
				}
				offset = 0
				for left_byte < start_byte+multiply10Pow {
					current_digit := int((current_cell[left_byte] - 48))
					digit = 0
					for digit+offset < multiply10Pow-1 {
						current_digit *= 10
						digit++
					}
					offset++
					left_byte++
					total_amount += float64(current_digit)
				}

				is_integer_complete = true
			}
		} else if is_dot_exist {
			if current_byte == total_byte-1 {
				offset = 0

				for right_byte := total_byte - 1; right_byte >= total_byte-divide10Pow; right_byte-- {
					current_digit := float64((current_cell[right_byte] - 48)) * 0.1
					digit = 0
					for digit+offset < divide10Pow-1 {
						current_digit *= 0.1
						digit++
					}
					offset++
					total_amount += float64(current_digit)
				}
			}
		}
		if is_integer_complete {
			divide10Pow++
		} else {
			multiply10Pow++
		}

		current_byte++
	}

	if is_negative == true {
		total_amount *= -1
	}

	if is_invalid_number == true {
		total_amount = 0
	}

	return total_amount
}