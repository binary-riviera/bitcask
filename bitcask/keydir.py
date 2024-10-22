from io import BufferedReader
from typing import TypedDict
from bitcask.bitcask_row import TSTAMP_BYTES
import os

KeyInfo = TypedDict('KeyInfo', {'file_id': str, 'value_sz': int, 'value_pos': int, 'tstamp': int})
KeyDir = dict[bytes, KeyInfo]

def read_row(f: BufferedReader) -> tuple[bytes, KeyInfo]:
    crc = f.read(8) # TODO: capture this then validate the row
    timestamp = int.from_bytes(f.read(TSTAMP_BYTES), 'little')
    ksz = int.from_bytes(f.read(2), 'little')
    value_sz = int.from_bytes(f.read(2), 'little')
    key = f.read(ksz)
    value = f.read(value_sz)
    return (key, KeyInfo(
        file_id=f.name,
        value_sz=value_sz,
        value_pos=f.tell(),
        tstamp=timestamp
    ))


def construct_keydir(directory: str) -> KeyDir:
    filepaths = [os.path.join(directory, f) for f in os.listdir(directory)]
    filepaths_mod_time = sorted(filepaths, key=os.path.getmtime, reverse=True)
    
    keydir: KeyDir = {}

    for filepath in filepaths_mod_time:
        with open(filepath, mode='rb') as f:
            pass
        print(filepath)

    return keydir
    