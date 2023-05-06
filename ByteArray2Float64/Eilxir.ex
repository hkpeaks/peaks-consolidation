defmodule ByteArray do
  def byte_array_to_float64(current_cell) do
    float_number = 0.0
    multiply10Pow = 0
    divide10Pow = 0
    position = 0
    offset = 0
    current_byte = 0
    left_byte = 0
    right_byte = 0
    is_dot_exist = false
    is_integer_complete = false
    is_negative = false
    is_invalid_number = false

    total_byte = length(current_cell)

    while current_byte < total_byte do
      case current_cell[current_byte] do
        ?\. ->
          is_dot_exist = true

        ?- ->
          current_cell[current_byte] = ?0
          is_negative = true

        ?( ->
          _fallthrough

        ?) ->
          _fallthrough

        _ ->
          if current_cell[current_byte] < ?0 || current_cell[current_byte] > ?9 do
            is_invalid_number = true
          end
      end

      if !is_integer_complete do
        if is_dot_exist || (!is_dot_exist && current_byte == total_byte - 1) do
          if !is_dot_exist && current_byte == total_byte - 1 do
            multiply10Pow = multiply10Pow + 1
          end

          offset = 0

          while left_byte < multiply10Pow do
            current_digit = :erlang.binary_to_float(<<current_cell[left_byte]>>)
            position = 0

            while position + offset < multiply10Pow - 1 do
              current_digit = current_digit * 10.0
              position = position + 1
            end

            offset = offset + 1
            left_byte = left_byte + 1

            float_number = float_number + current_digit
          end

          is_integer_complete = true

          divide10Pow = divide10Pow + 1
        end

        multiply10Pow = multiply10Pow + 1

      else 
        if is_dot_exist do 
          if current_byte == total_byte - 1 do 
            offset = 0 

            for right_byte <- :lists.reverse(1..divide10Pow) do 
              current_digit = :erlang.binary_to_float(<<current_cell[total_byte - right_byte]>>) * 0.1 
              position = 0 

              while position + offset < divide10Pow - 1 do 
                current_digit *= 0.1 
                position += 1 
              end 

              offset += 1 
              float_number += current_digit 
            end 
          end 

          divide10Pow += 1 
        end 
      end 

      current_byte += 1 
    end 

    if is_negative == true do 
      float_number *= -1 
    end 

    if is_invalid_number == true do 
      float_number = 0.0 
    end 

    return float_number 
  end 
end