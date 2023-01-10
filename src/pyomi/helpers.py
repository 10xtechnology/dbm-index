from hashlib import sha256


def custom_hash(val: str):
    return sha256(val.encode()).hexdigest()