- (float) byteArray2Float64:(NSData *)current_cell {
    float float_number = 0;
    int multiply10Pow = 0;
    int divide10Pow = 0;
    int position = 0;
    int offset = 0;
    int current_byte = 0;
    int left_byte = 0;
    bool is_dot_exist = false;
    bool is_integer_complete = false;
    bool is_negative = false;
    bool is_invalid_number = false;

    int total_byte = (int)[current_cell length];

    while (current_byte < total_byte) {
        char c;
        [current_cell getBytes:&c range:NSMakeRange(current_byte, 1)];
        switch (c) {
            case '.':
                is_dot_exist = true;
                break;
            case '-':
                c = '0';
                is_negative = true;
                break;
            case '(':
            case ')':
                break;

            default:
                if (c < 48 || c > 57) {
                    is_invalid_number = true;
                }
        }

        if (!is_integer_complete) {
            if (is_dot_exist || (!is_dot_exist && current_byte == total_byte - 1)) {
                if (!is_dot_exist && current_byte == total_byte - 1) {
                    multiply10Pow++;
                }
                offset = 0;
                while (left_byte < multiply10Pow) {
                    char current_digit_c;
                    [current_cell getBytes:&current_digit_c range:NSMakeRange(left_byte, 1)];
                    float current_digit = (float)(current_digit_c - 48);
                    position = 0;
                    while (position + offset < multiply10Pow - 1) {
                        current_digit *= 10.0f;
                        position++;
                    }
                    offset++;
                    left_byte++;
                    float_number += current_digit;
                }

                is_integer_complete = true;
                divide10Pow++;

            }
            multiply10Pow++;

        } else if (is_dot_exist) {
            if (current_byte == total_byte - 1) {
                offset = 0;

                for (int right_byte = total_byte - 1; right_byte >= total_byte - divide10Pow; right_byte--) {
                    char current_digit_c;
                    [current_cell getBytes:&current_digit_c range:NSMakeRange(right_byte, 1)];
                    float current_digit = ((float)(current_digit_c - 48)) * 0.1f;
                    position = 0;
                    while (position + offset < divide10Pow - 1) {
                        current_digit *= 0.1f;
                        position++;
                    }
                    offset++;
                    float_number += current_digit;
                }
            }
            divide10Pow++;
        }

        current_byte++;
    }

    if (is_negative == true) {
        float_number *= -1.0f;
    }

    if (is_invalid_number == true) {
        float_number = 0.0f;
    }
    return float_number;
}