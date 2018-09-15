

def encode_decimal(decimal_num):
    decimal_tuple = decimal_num.normalize().as_tuple()
    encoded_decimal = {
        "sign": decimal_tuple.sign,
        "digits": list(decimal_tuple.digits),
        "exponent": decimal_tuple.exponent
    }
    return encoded_decimal

