function ByteArray2Float64(current_cell::Vector{UInt8})
    float_number = 0.0
    multiply10Pow = 0
    divide10Pow = 0
    position = 0
    offset = 0
    current_byte = 1
    left_byte = 1
    right_byte = 1
    is_dot_exist = false
    is_integer_complete = false
    is_negative = false
    is_invalid_number = false

    total_byte = length(current_cell)

    while current_byte <= total_byte
        if current_cell[current_byte] == UInt8('.')
            is_dot_exist = true
        elseif current_cell[current_byte] == UInt8('-')
            current_cell[current_byte] = UInt8('0')
            is_negative = true
        elseif current_cell[current_byte] == UInt8('(') || current_cell[current_byte] == UInt8(')') || (current_cell[current_byte] < UInt8('0') || current_cell[current_byte] > UInt8('9'))
            is_invalid_number = true
        end

        if !is_integer_complete
            if is_dot_exist || (!is_dot_exist && current_byte == total_byte)
                if !is_dot_exist && current_byte == total_byte
                    multiply10Pow += 1
                end

                offset = 0

                while left_byte <= multiply10Pow
                    current_digit = float(current_cell[left_byte] - UInt8('0'))
                    position = 0

                    while position + offset < multiply10Pow - 1
                        current_digit *= 10.0
                        position += 1
                    end

                    offset += 1
                    left_byte += 1

                    float_number += current_digit

                end

                is_integer_complete = true

                divide10Pow += 1

            end

            multiply10Pow += 1

        elseif is_dot_exist

            if current_byte == total_byte

                offset = 0

                while right_byte >= total_byte - divide10Pow + 1

                    current_digit = float(current_cell[right_byte] - UInt8('0')) * 0.1

                    position = 0

                    while position + offset < divide10Pow - 1

                        current_digit *= 0.1

                        position += 1

                    end

                    offset += 1

                    right_byte -= 1

                    float_number += current_digit

                end
                
            end
            
            divide10Pow += 1
            
        end
        
        current_byte += 1
        
    end
    
    if is_negative == true
        
        float_number *= -1
        
    end
    
    if is_invalid_number == true
        
        float_number = 0
        
    end
    
    return float_number
    
end 