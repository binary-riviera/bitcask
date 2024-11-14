from typing import NamedTuple, Optional
from bitcask.utils import TSTAMP_BYTES, pos_from_bytes, size_from_bytes, size_to_bytes, tstamp_from_bytes, tstamp_to_bytes, pos_to_bytes
from pathlib import Path

Hint = NamedTuple('Hint', [
    ('timestamp', int),
    ('ksz', int),
    ('value_sz', int),
    ('value_pos', int),
    ('key', bytes),
])

def hint_to_bytes(hint: Hint) -> bytes:
    return tstamp_to_bytes(hint.timestamp) + size_to_bytes(hint.ksz) + size_to_bytes(hint.value_sz) + pos_to_bytes(hint.value_pos) + hint.key

def bytes_to_hint(b: bytes) -> Hint:
     timestamp = tstamp_from_bytes(b[0:TSTAMP_BYTES])
     ksz = size_from_bytes(b[TSTAMP_BYTES: TSTAMP_BYTES+2])
     value_sz = size_from_bytes(b[TSTAMP_BYTES+2: TSTAMP_BYTES+4])
     value_pos = pos_from_bytes(b[TSTAMP_BYTES+4: TSTAMP_BYTES+6])
     key = b[TSTAMP_BYTES+6:]
     return Hint(
         timestamp=timestamp,
         ksz=ksz,
         value_sz=value_sz,
         value_pos=value_pos,
         key=key
     )

def write_hint_file(store_file: str, hints: list[Hint]):
    print('Writing hint file')
    hint_file = Path(store_file).with_suffix('.hint')
    with open(hint_file, 'ab') as f:
        f.writelines(map(hint_to_bytes, hints))\

def read_hint_file_if_exists(store_file: str) -> Optional[list[Hint]]:
    hint_file = Path(store_file).with_suffix('.hint')
    if hint_file.exists():
        with open(hint_file, 'rb') as f:
            return list(map(bytes_to_hint, f.readlines()))
    return None