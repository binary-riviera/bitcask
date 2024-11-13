from typing import NamedTuple
from bitcask.utils import size_to_bytes, tstamp_to_bytes, pos_to_bytes
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

def write_hint_file(store_file: str, hints: list[Hint]):
    print('Writing hint file')
    hint_file = Path(store_file).with_suffix('.hint')
    with open(hint_file, 'ab') as f:
        f.writelines(map(hint_to_bytes, hints))
    