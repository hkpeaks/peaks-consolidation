def byte_array_to_float64(current_cell)
  float_number = 0.0
  multiply10Pow = 0
  divide10Pow = 0
  position = 0
  offset = 0
  current_byte = 0
  left_byte = 0
  is_dot_exist = false
  is_integer_complete = false
  is_negative = false
  is_invalid_number = false

  total_byte = current_cell.length

  while current_byte < total_byte do
    case current_cell[current_byte]
    when "."
      is_dot_exist = true
    when "-"
      current_cell[current_byte] = "0"
      is_negative = true
    when "(", ")"
      # do nothing
    else
      if current_cell[current_byte] < "0" || current_cell[current_byte] > "9"
        is_invalid_number = true
      end
    end

    if !is_integer_complete then
      if is_dot_exist || (!is_dot_exist && current_byte == total_byte-1) then
        if !is_dot_exist && current_byte == total_byte-1 then
          multiply10Pow += 1
        end

        offset = 0

        while left_byte < multiply10Pow do
          current_digit = (current_cell[left_byte].ord - "0".ord).to_f

          position = 0

          while position + offset < multiply10Pow-1 do
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
      
    elsif is_dot_exist then
      
      if current_byte == total_byte-1 then
        
        offset = 0
        
        (total_byte-1).downto(total_byte-divide10Pow) do |right_byte|
          
          current_digit = (current_cell[right_byte].ord - "0".ord).to_f * 0.1

          position = 0

          while position + offset < divide10Pow-1 do
            
            current_digit *= 0.1
            
            position += 1
            
          end

          offset += 1

          float_number += current_digit
        
        end
      
      end
      
      divide10Pow += 1
      
    end
    
    current_byte += 1
    
  end
  
  
  if is_negative == true then
    
    float_number *= -1
    
  end
  
  
  if is_invalid_number == true then
    
    float_number = 0
    
  end
  
  
  return float_number
  
end