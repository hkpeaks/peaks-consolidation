public static double byteArrayToFloat64(byte[] current_cell) {
    double float_number = 0.0;
    int multiply10Pow = 0;
    int divide10Pow = 0;
    int position = 0;
    int offset = 0;
    int left_byte = 0;
    boolean is_dot_exist = false;
    boolean is_integer_complete = false;
    boolean is_negative = false;
    boolean is_invalid_number = false;

    int total_byte = current_cell.length;

    for (int current_byte = 0; current_byte < total_byte; current_byte++) {
        switch (current_cell[current_byte]) {
            case '.':
                is_dot_exist = true;
                break;
            case '-':
                current_cell[current_byte] = '0';
                is_negative = true;
                break;
            case '(':
            case ')':
                break;
            default:
                if (current_cell[current_byte] < 48 || current_cell[current_byte] > 57) {
                    is_invalid_number = true;
                }
        }

        if (!is_integer_complete) {
            if (is_dot_exist || (!is_dot_exist && current_byte == total_byte - 1)) {
                if (!is_dot_exist && current_byte == total_byte - 1) {
                    multiply10Pow += 1;
                }

                offset = 0;

                while (left_byte < multiply10Pow) {
                    double current_digit =
                            (double) (current_cell[left_byte] - 48);
                    position = 0;

                    while (position + offset < multiply10Pow - 1) {
                        current_digit *= 10.0;
                        position += 1;
                    }

                    offset += 1;
                    left_byte += 1;
                    float_number += current_digit;
                }

                is_integer_complete = true;
                divide10Pow += 1;
            }

            multiply10Pow += 1;

        } else if (is_dot_exist) {
            if (current_byte == total_byte - 1) {
                offset = 0;

                for (int right_byte = total_byte - 1; right_byte >= total_byte - divide10Pow; right_byte--) {
                    double current_digit =
                            (double) (current_cell[right_byte] - 48) * 0.1;
                    position = 0;

                    while (position + offset < divide10Pow - 1) {
                        current_digit *= 0.1;
                        position += 1;
                    }

                    offset += 1;
                    float_number += current_digit;
                }
            }
            divide10Pow++;
        }
    }

    if (is_negative == true) {
        float_number *= -1.0;
    }

    if (is_invalid_number == true) {
        float_number = 0.0;
    }

    return float_number;
}
