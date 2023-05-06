fn byte_array_to_float64(current_cell: &[u8]) -> f64 {
    let mut float_number = 0.0;
    let mut multiply10_pow = 0;
    let mut divide10_pow = 0;
    let mut position = 0;
    let mut offset = 0;
    let mut current_byte = 0;
    let mut left_byte = 0;
    let mut is_dot_exist = false;
    let mut is_integer_complete = false;
    let mut is_negative = false;
    let mut is_invalid_number = false;

    let total_byte = current_cell.len();

    while current_byte < total_byte {
        match current_cell[current_byte] {
            b'.' => is_dot_exist = true,
            b'-' => {
                current_cell[current_byte] = b'0';
                is_negative = true;
            }
            b'(' | b')' => {}
            _ => {
                if current_cell[current_byte] < 48 || current_cell[current_byte] > 57 {
                    is_invalid_number = true;
                }
            }
        }

        if !is_integer_complete {
            if is_dot_exist || (!is_dot_exist && current_byte == total_byte - 1) {
                if !is_dot_exist && current_byte == total_byte - 1 {
                    multiply10_pow += 1;
                }
                offset = 0;
                while left_byte < multiply10_pow {
                    let current_digit =
                        f64::from(current_cell[left_byte] - 48);
                    position = 0;
                    while position + offset < multiply10_pow - 1 {
                        float_number *= 10.0;
                        position += 1;
                    }
                    offset += 1;
                    left_byte += 1;
                    float_number += current_digit;
                }

                is_integer_complete = true;
                divide10_pow += 1;
            }
            multiply10_pow += 1;
        } else if is_dot_exist {
            if current_byte == total_byte - 1 {
                offset = 0;

                for right_byte in (total_byte - divide10_pow..total_byte).rev() {
                    let current_digit =
                        f64::from(current_cell[right_byte] - 48) * 0.1;

                    position = 0;

                    while position + offset < divide10_pow - 1 {
                        float_number *= 0.1;
                        position += 1;
                    }

                    offset += 1;

                    float_number += current_digit;
                }
            }
            divide10_pow += 1;
        }

        current_byte += 1;
    }

    if is_negative == true {
        float_number *= -1.0
    }

    if is_invalid_number == true {
        float_number = 0.0
    }

    return float_number
}