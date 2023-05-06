public static double ByteArray2Float64(byte[] current_cell)
{
    double float_number = 0;
    int multiply10Pow = 0, divide10Pow = 0, position = 0, offset = 0, current_byte = 0, left_byte = 0;
    bool is_dot_exist = false, is_integer_complete = false, is_negative = false, is_invalid_number = false;

    int total_byte = current_cell.Length;

    while (current_byte < total_byte)
    {
        switch (current_cell[current_byte])
        {
            case (byte)'.': is_dot_exist = true; break;
            case (byte)'-': current_cell[current_byte] = (byte)'0'; is_negative = true; break;
            case (byte)'(': goto case (byte)')';
            case (byte)')': break;
            default:
                if (current_cell[current_byte] < 48 || current_cell[current_byte] > 57)
                {
                    is_invalid_number = true;
                }
                break;
        }

        if (!is_integer_complete)
        {
            if (is_dot_exist || (!is_dot_exist && current_byte == total_byte - 1))
            {
                if (!is_dot_exist && current_byte == total_byte - 1)
                {
                    multiply10Pow++;
                }
                offset = 0;
                while (left_byte < multiply10Pow)
                {
                    double current_digit = (double)(current_cell[left_byte] - 48);
                    position = 0;
                    while (position + offset < multiply10Pow - 1)
                    {
                        current_digit *= 10;
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

        }
        else if (is_dot_exist)
        {
            if (current_byte == total_byte - 1)
            {
                offset = 0;

                for (int right_byte = total_byte - 1; right_byte >= total_byte - divide10Pow; right_byte--)
                {
                    double current_digit = ((double)(current_cell[right_byte] - 48)) * 0.1;
                    position = 0;
                    while (position + offset < divide10Pow - 1)
                    {
                        current_digit *= 0.1;
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

    if (is_negative == true)
    {
        float_number *= -1;
    }

    if (is_invalid_number == true)
    {
        float_number = 0;
    }
    return float_number;
}