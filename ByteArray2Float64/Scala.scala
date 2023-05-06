def byteArray2Float64(currentCell: Array[Byte]): Double = {
  var floatNumber = 0.0
  var multiply10Pow, divide10Pow, position, offset, currentByte, leftByte = 0
  var isDotExist = false
  var isIntegerComplete = false
  var isNegative = false
  var isInvalidNumber = false

  val totalByte = currentCell.length

  while (currentByte < totalByte) {
    currentCell(currentByte) match {
      case '.' =>
        isDotExist = true
      case '-' =>
        currentCell(currentByte) = '0'
        isNegative = true
      case '(' =>
      case ')' =>
      case _ =>
        if (currentCell(currentByte) < 48 || currentCell(currentByte) > 57) {
          isInvalidNumber = true
        }
    }

    if (!isIntegerComplete) {
      if (isDotExist || (!isDotExist && currentByte == totalByte - 1)) {
        if (!isDotExist && currentByte == totalByte - 1) {
          multiply10Pow += 1
        }
        offset = 0
        while (leftByte < multiply10Pow) {
          val currentDigit = currentCell(leftByte).toDouble - 48.0
          position = 0
          while (position + offset < multiply10Pow - 1) {
            currentDigit *= 10.0
            position += 1
          }
          offset += 1
          leftByte += 1
          floatNumber += currentDigit
        }

        isIntegerComplete = true
        divide10Pow += 1

      }
      multiply10Pow += 1

    } else if (isDotExist) {
      if (currentByte == totalByte - 1) {
        offset = 0

        for (rightByte <- totalByte - 1 to totalByte - divide10Pow by -1) {
          val currentDigit = (currentCell(rightByte).toDouble - 48.0) * 0.1
          position = 0
          while (position + offset < divide10Pow - 1) {
            currentDigit *= 0.1
            position += 1
          }
          offset += 1
          floatNumber += currentDigit
        }
      }
      divide10Pow += 1
    }

    currentByte += 1
  }

  if (isNegative == true) {
    floatNumber *= -1.0
  }

  if (isInvalidNumber == true) {
    floatNumber = 0.0
  }
  
   floatNumber 
}