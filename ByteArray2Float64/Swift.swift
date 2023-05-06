func byteArray2Float64(current_cell: [UInt8]) -> Double {
    var float_number: Double = 0
    var multiply10Pow = 0, divide10Pow = 0, position = 0, offset = 0, left_byte = 0
    var is_dot_exist = false, is_integer_complete = false, is_negative = false, is_invalid_number = false
    let total_byte = current_cell.count

    var current_byte = 0
    while current_byte < total_byte {
        switch current_cell[current_byte] {
        case UInt8(ascii: "."):
            is_dot_exist = true
        case UInt8(ascii: "-"):
            current_cell[current_byte] = UInt8(ascii: "0")
            is_negative = true
        case UInt8(ascii: "("), UInt8(ascii: ")"):
            break
        default:
            if current_cell[current_byte] < 48 || current_cell[current_byte] > 57 {
                is_invalid_number = true
            }
        }

        if !is_integer_complete {
            if is_dot_exist || (!is_dot_exist && current_byte == total_byte - 1) {
                if !is_dot_exist && current_byte == total_byte - 1 {
                    multiply10Pow += 1
                }
                offset = 0
                while left_byte < multiply10Pow {
                    let current_digit = Double(current_cell[left_byte] - 48)
                    position = 0
                    while position + offset < multiply10Pow - 1 {
                        float_number += current_digit * pow(10.0, Double(position))
                        position += 1
                    }
                    offset += 1
                    left_byte += 1
                }

                is_integer_complete = true
                divide10Pow += 1

            }
            multiply10Pow += 1

        } else if is_dot_exist {
            if current_byte == total_byte - 1 {
                offset = 0

                for right_byte in stride(from: total_byte - 1, to: total_byte - divide10Pow - 1, by: -1) {
                    let current_digit = Double(current_cell[right_byte] - 48) * 0.1
                    position = 0
                    while position + offset < divide10Pow - 1 {
                        float_number += current_digit * pow(0.1, Double(position))
                        position += 1
                    }
                    offset += 1
                }
            }
            divide10Pow += 1
        }

        current_byte += 1
    }

    if is_negative == true {
        float_number *= -1
    }

    if is_invalid_number == true {
        float_number = 0
    }
    return float_number
}
