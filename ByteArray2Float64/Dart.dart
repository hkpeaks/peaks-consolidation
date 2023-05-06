double byteArray2Float64(List<int> currentCell) {
  double floatNumber = 0.0;
  int multiply10Pow = 0;
  int divide10Pow = 0;
  int position = 0;
  int offset = 0;
  int currentByte = 0;
  int leftByte = 0;
  bool isDotExist = false;
  bool isIntegerComplete = false;
  bool isNegative = false;
  bool isInvalidNumber = false;

  int totalByte = currentCell.length;

  while (currentByte < totalByte) {
    switch (String.fromCharCode(currentCell[currentByte])) {
      case ".":
        isDotExist = true;
        break;
      case "-":
        currentCell[currentByte] = "0".codeUnitAt(0);
        isNegative = true;
        break;
      case "(":
      case ")":
        break;
      default:
        if (currentCell[currentByte] < "0".codeUnitAt(0) ||
            currentCell[currentByte] > "9".codeUnitAt(0)) {
          isInvalidNumber = true;
        }
    }

    if (!isIntegerComplete) {
      if (isDotExist || (!isDotExist && currentByte == totalByte - 1)) {
        if (!isDotExist && currentByte == totalByte - 1) {
          multiply10Pow += 1;
        }

        offset = 0;

        while (leftByte < multiply10Pow) {
          double currentDigit =
              (currentCell[leftByte] - "0".codeUnitAt(0)).toDouble();

          position = 0;

          while (position + offset < multiply10Pow - 1) {
            currentDigit *= 10.0;

            position += 1;
          }

          offset += 1;

          leftByte += 1;

          floatNumber += currentDigit;
        }

        isIntegerComplete = true;

        divide10Pow += 1;
      }

      multiply10Pow += 1;
    } else if (isDotExist) {
      if (currentByte == totalByte - 1) {
        offset = 0;

        for (int rightByte = totalByte - 1;
            rightByte >= totalByte - divide10Pow;
            rightByte--) {
          double currentDigit =
              (currentCell[rightByte] - "0".codeUnitAt(0)).toDouble() * 0.1;

          position = 0;

          while (position + offset < divide10Pow - 1) {
            currentDigit *= 0.1;

            position += 1;
          }

          offset += 1;

          floatNumber += currentDigit;
        }
      }

      divide10Pow += 1;
    }

    currentByte += 1;
  }

  if (isNegative == true) {
    floatNumber *= -1.0;
  }

  if (isInvalidNumber == true) {
    floatNumber = 0.0;
  }

  
return floatNumber;  
}