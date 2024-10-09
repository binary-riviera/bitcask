# bitcask
import time
from binascii import crc32

DEFAULT_ENCODING = "utf-8"
TSTAMP_BYTES = 20 # FIXME: bad

class BitcaskRow:
    """Bitcask to be written to the file.
    
    | crc | tstamp | ksz | value_sz | key | value

    """
    def __init__(self, key, value):
        self.tstamp = time.time_ns() 
        self.key = key
        self.value = value
        self.ksz = len(key)
        self.value_sz = len(value)

    @property
    def crc(self):
        x = bytearray(self.tstamp.to_bytes(TSTAMP_BYTES, 'little'))
        x += self.ksz.to_bytes(2, 'little')
        x += self.value_sz.to_bytes(2, 'little')
        x += self.key
        x += self.value
        return crc32(x)

    def __str__(self):
        return f'crc: {self.crc} tstamp: {self.tstamp} ksz: {self.ksz} value_sz: {self.value_sz} key: {self.key} value: {self.value}'


if __name__ == '__main__':
    foo = BitcaskRow(key=b'foo', value=b'bar')
    print(foo)

