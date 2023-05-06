Function ByteArray2Float64(current_cell() As Byte) As Double

    Dim float_number As Double
    Dim multiply10Pow, divide10Pow, position, offset, current_byte, left_byte As Integer
    Dim is_dot_exist, is_integer_complete, is_negative, is_invalid_number As Boolean

    Dim total_byte As Integer = current_cell.Length

    While current_byte < total_byte
        Select Case Chr(current_cell(current_byte))
            Case "."
                is_dot_exist = True
            Case "-"
                current_cell(current_byte) = 48
                is_negative = True
            Case "("
            Case ")"
            Case Else
                If current_cell(current_byte) < 48 Or current_cell(current_byte) > 57 Then
                    is_invalid_number = True
                End If
        End Select

        If Not is_integer_complete Then
            If is_dot_exist Or (Not is_dot_exist And current_byte = total_byte - 1) Then
                If Not is_dot_exist And current_byte = total_byte - 1 Then
                    multiply10Pow += 1
                End If

                offset = 0

                While left_byte < multiply10Pow
                    Dim current_digit As Double = CDbl(current_cell(left_byte) - 48)
                    position = 0

                    While position + offset < multiply10Pow - 1
                        current_digit *= 10
                        position += 1
                    End While

                    offset += 1
                    left_byte += 1
                    float_number += current_digit
                End While

                is_integer_complete = True
                divide10Pow += 1

            End If

            multiply10Pow += 1

        ElseIf is_dot_exist Then

            If current_byte = total_byte - 1 Then

                offset = 0

                For right_byte As Integer = total_byte - 1 To total_byte - divide10Pow Step -1

                    Dim current_digit As Double = CDbl(current_cell(right_byte) - 48) * 0.1
                    position = 0

                    While position + offset < divide10Pow - 1
                        current_digit *= 0.1
                        position += 1
                    End While

                    offset += 1
                    float_number += current_digit

                Next right_byte

            End If

            divide10Pow += 1

        End If

        current_byte += 1

    End While


    If is_negative Then float_number *= -1


    If is_invalid_number Then float_number = 0


    Return float_number


End Function 