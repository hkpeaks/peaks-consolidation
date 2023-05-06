ByteArray2Float64 <- function(current_cell) {
  float_number <- 0
  multiply10Pow <- 0
  divide10Pow <- 0
  position <- 0
  offset <- 0
  current_byte <- 1
  left_byte <- 1
  right_byte <- 1
  
  is_dot_exist <- FALSE
  is_integer_complete <- FALSE
  is_negative <- FALSE
  is_invalid_number <- FALSE
  
  total_byte = length(current_cell)
  
  while (current_byte <= total_byte) {
    switch(current_cell[current_byte]) {
      case '.':
        is_dot_exist = TRUE
      case '-':
        current_cell[current_byte] = '0'
        is_negative = TRUE
      case '(':
        break;
      case ')':
        break;
      default:
        if (current_cell[current_byte] < '0' || current_cell[current_byte] > '9') {
          is_invalid_number = TRUE
        }
    }
    
    if (!is_integer_complete) {
      if (is_dot_exist || (!is_dot_exist && current_byte == total_byte)) {
        if (!is_dot_exist && current_byte == total_byte) {
          multiply10Pow = multiply10Pow + 1
        }
        
        offset = 0
        
        while (left_byte <= multiply10Pow) {
          current_digit = as.numeric(charToRaw(current_cell[left_byte])) - as.numeric(charToRaw('0'))
          position = 0
          
          while ((position + offset) < (multiply10Pow - 1)) {
            current_digit = current_digit * 10
            position = position + 1
          }
          
          offset = offset + 1
          left_byte = left_byte + 1
          float_number = float_number + current_digit
        }
        
        is_integer_complete = TRUE
        divide10Pow = divide10Pow + 1
        
      }
      
      multiply10Pow = multiply10Pow + 1
      
    } else if (is_dot_exist) {
      if (current_byte == total_byte) {
        offset = 0
        
        for (right_byte in total_byte:total_byte-divide10Pow+1) {
          current_digit = as.numeric(charToRaw(current_cell[right_byte])) - as.numeric(charToRaw('0'))
          current_digit = current_digit * .1
          position = 0

          while ((position + offset) < (divide10Pow - 1)) {
            current_digit = current_digit * .1
            position = position + 1
          }

          offset = offset + 1

          float_number = float_number + current_digit

        }
        
      }
      
      divide10Pow = divide10Pow + 1
      
    }    
    
    current_byte = current_byte + 1
    
    if (is_negative == TRUE) {
      float_number = float_number * -1
      
    }
    
    if (is_invalid_number == TRUE) {
      float_number = 0
      
    }
    
    return(float_number)
}