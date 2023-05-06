fun byteArray2Float64(currentCell: ByteArray): Double {
    var floatNumber = 0.0
    var multiply10Pow = 0
    var divide10Pow = 0
    var position: Int
    var offset: Int
    var currentByte = 0
    var leftByte = 0
    var isDotExist = false
    var isIntegerComplete = false
    var isNegative = false
    var isInvalidNumber = false

    val totalByte = currentCell.size

    while (currentByte < totalByte) {
        when (currentCell[currentByte]) {
            '.'.toByte() -> isDotExist = true
            '-'.toByte() -> {
                currentCell[currentByte] = '0'.toByte()
                isNegative = true
            }
            '('.toByte(), ')'.toByte() -> {
            }
            else -> if (currentCell[currentByte] < 48 || currentCell[currentByte] > 57) {
                isInvalidNumber = true
            }
        }
        if (!isIntegerComplete) {
            if (isDotExist || !isDotExist && currentByte == totalByte - 1) {
                if (!isDotExist && currentByte == totalByte - 1) {
                    multiply10Pow++
                }
                offset = 0
                while (leftByte < multiply10Pow) {
                    val currentDigit =
                        (currentCell[leftByte].toInt() - 48).toDouble()
                    position = 0
                    while (position + offset < multiply10Pow - 1) {
                        currentDigit *= 10.0
                        position++
                    }
                    offset++
                    leftByte++
                    floatNumber += currentDigit
                }
                isIntegerComplete = true
                divide10Pow++
            }
            multiply10Pow++
        } else if (isDotExist) {
            if (currentByte == totalByte - 1) {
                offset = 0
                for (rightByte in totalByte - 1 downTo totalByte - divide10Pow) {
                    val currentDigit =
                        (currentCell[rightByte].toInt() - 48).toDouble() * 0.1
                    position = 0
                    while (position + offset < divide10Pow - 1) {
                        currentDigit *= 0.1
                        position++
                    }
                    offset++
                    floatNumber += currentDigit
                }
            }
            divide10Pow++
        }
        currentByte++
    }

    if (isNegative == true) {
        floatNumber *= -1.0
    }

    if (isInvalidNumber == true) {
        floatNumber = 0.0
    }

    return floatNumber

}