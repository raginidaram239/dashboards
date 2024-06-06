import base64


def str_to_base64str(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf8")).decode("utf8")


def base64str_to_str(value_base64: str) -> str:
    return base64.urlsafe_b64decode(value_base64).decode("utf8")
