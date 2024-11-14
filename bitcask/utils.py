
TSTAMP_BYTES = 20
BYTE_ORDER = 'little'

def tstamp_to_bytes(tstamp: int) -> bytes:
    return tstamp.to_bytes(TSTAMP_BYTES, BYTE_ORDER)

def tstamp_from_bytes(tstamp: bytes) -> int:
    return int.from_bytes(tstamp, BYTE_ORDER)

def size_to_bytes(size: int) -> bytes:
    return size.to_bytes(2, BYTE_ORDER)

def size_from_bytes(size: bytes) -> int:
    return int.from_bytes(size, BYTE_ORDER)

def pos_to_bytes(pos: int) -> bytes:
    return pos.to_bytes(2, BYTE_ORDER)

def pos_from_bytes(pos: bytes) -> int:
    return int.from_bytes(pos, BYTE_ORDER)
