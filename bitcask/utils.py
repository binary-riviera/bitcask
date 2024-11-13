
TSTAMP_BYTES = 20 # FIXME: bad

def tstamp_to_bytes(tstamp: int) -> bytes:
    return tstamp.to_bytes(TSTAMP_BYTES, 'little')

def size_to_bytes(size: int) -> bytes:
    return size.to_bytes(2, 'little')

def pos_to_bytes(pos: int) -> bytes:
    return pos.to_bytes(2, 'little')