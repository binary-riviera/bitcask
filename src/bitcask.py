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


class Bitcask:

    def __init__(directory: str):
        self.directory = directory
        self.keydir = dict()

    def update_keydir(self, row, file_id, value_pos):
        pass

    """
    Below are the actual defined operatiors in the specification
    """
    def open(directoryName, opts=None):
        pass

    def get(key):
        pass

    def put(key, value):
        pass

    def delete(key):
        pass

    def list_keys():
        pass

    def fold(f, acc0):
        pass

    def merge():
        pass

    def sync():
        pass

    def close():
        pass



if __name__ == '__main__':
    foo = BitcaskRow(key=b'foo', value=b'bar')
    print(foo)

