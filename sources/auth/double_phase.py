import hmac


def verify_double_code(redisserver, login, submitted_code):
    key = "nyx_double_" + login
    stored_code = redisserver.get(key)
    if stored_code is None or not isinstance(submitted_code, str) or not hmac.compare_digest(
        stored_code.decode("ascii"), submitted_code
    ):
        redisserver.delete(key)
        return False

    redisserver.delete(key)
    return True
