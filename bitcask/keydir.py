from typing import TypedDict
import os

KeyInfo = TypedDict('KeyInfo', {'file_id': str, 'value_sz': int, 'value_pos': int, 'tstamp': int})
KeyDir = dict[bytes, KeyInfo]

def construct_keydir(directory: str) -> KeyDir:
    filepaths = [os.path.join(directory, f) for f in os.listdir(directory)]
    filepaths_mod_time = sorted(filepaths, key=os.path.getmtime)
    pass