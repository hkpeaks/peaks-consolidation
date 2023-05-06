function byteArray2Float64($current_cell) {
  $float_number = 0;
  $multiply10Pow = 0;
  $divide10Pow = 0;
  $position = 0;
  $offset = 0;
  $left_byte = 0;
  $is_dot_exist = false;
  $is_integer_complete = false;
  $is_negative = false;
  $is_invalid_number = false;

  $total_byte = count($current_cell);

  for ($current_byte = 0; $current_byte < $total_byte; $current_byte++) {
    switch ($current_cell[$current_byte]) {
      case ".":
        $is_dot_exist = true;
        break;
      case "-":
        $current_cell[$current_byte] = "0";
        $is_negative = true;
        break;
      case "(":
      case ")":
      default:
        if ($current_cell[$current_byte] < "48" || $current_cell[$current_byte] > "57") {
          $is_invalid_number = true;
        }
        break;
    }

    if (!$is_integer_complete) {
      if ($is_dot_exist || (!$is_dot_exist && $current_byte == $total_byte - 1)) {
        if (!$is_dot_exist && $current_byte == $total_byte - 1) {
          ++$multiply10Pow;
        }
        $offset = 0;
        while ($left_byte < $multiply10Pow) {
          $current_digit = floatval($current_cell[$left_byte]) - floatval("48");
          $position = 0;
          while ($position + $offset < $multiply10Pow - 1) {
            $current_digit *= 10;
            ++$position;
          }
          ++$offset;
          ++$left_byte;
          $float_number += $current_digit;
        }

        $is_integer_complete = true;
        ++$divide10Pow;
      }
      ++$multiply10Pow;

    } else if ($is_dot_exist) {
      if ($current_byte == $total_byte - 1) {
        $offset = 0;

        for (
          $right_byte = $total_byte - 1; 
          $right_byte >= ($total_byte - $divide10Pow); 
          --$right_byte
        ) {
          $current_digit =
            (floatval($current_cell[$right_byte]) - floatval("48")) * floatval("0.1");
          $position = 0;
          while ($position + $offset < ($divide10Pow - 1)) {
            $current_digit *= floatval("0.1");
            ++$position;
          }
          ++$offset;
          $float_number += floatval($current_digit);
        }
      }
      ++$divide10Pow;

    }

    ++$current_byte;

  }

  if ($is_negative == true) {
    return -$float_number;

  } else if ($is_invalid_number == true) {
    return floatval("0");

  } else {
    return floatval($float_number);

  }
}