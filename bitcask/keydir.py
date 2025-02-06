from io import BufferedReader
import logging
from typing import TypedDict, Optional
from bitcask.utils import (
    TSTAMP_BYTES,
    pos_from_bytes,
    size_from_bytes,
    tstamp_from_bytes,
)
import os

logger = logging.getLogger(__name__)

KeyInfo = TypedDict(
    "KeyInfo", {"file_id": str, "value_sz": int, "value_pos": int, "tstamp": int}
)

KeyDir = dict[bytes, KeyInfo]


def read_row_store_file(f: BufferedReader) -> Optional[tuple[bytes, KeyInfo]]:
    crc = f.read(8)
    if len(crc) < 8:  # end of file reached
        return None
    tstamp = tstamp_from_bytes(f.read(TSTAMP_BYTES))
    ksz = size_from_bytes(f.read(2))
    value_sz = size_from_bytes(f.read(2))
    key = f.read(ksz)
    value_pos = f.tell()
    _value = f.read(value_sz)
    return (
        key,
        KeyInfo(file_id=f.name, value_sz=value_sz, value_pos=value_pos, tstamp=tstamp),
    )


def read_row_hint_file(f: BufferedReader) -> Optional[tuple[bytes, KeyInfo]]:
    # tstamp | ksz | value_sz | value_pos | key
    tstamp_bytes = f.read(TSTAMP_BYTES)
    if len(tstamp_bytes) < TSTAMP_BYTES:
        return None
    tstamp = tstamp_from_bytes(tstamp_bytes)
    ksz = size_from_bytes(f.read(2))
    value_sz = size_from_bytes(f.read(2))
    value_pos = pos_from_bytes(f.read(2))
    key = f.read(ksz)
    return (
        key,
        KeyInfo(
            file_id=f.name.replace(".hint", ".store"), # type: ignore[misc]
            value_sz=value_sz,
            value_pos=value_pos,
            tstamp=tstamp,
        ),
    )


def construct_keydir(directory: str) -> KeyDir:
    logger.info("constructing keydir...")
    filepaths = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.split(".")[1] == "store"
    ]
    filepaths_mod_time = sorted(filepaths, key=os.path.getmtime)
    keydir: KeyDir = {}
    for filepath in filepaths_mod_time:
        hint_filepath = filepath.replace(".store", ".hint")
        if os.path.isfile(hint_filepath):
            logger.debug(f"reading hint file {filepath}")
            with open(hint_filepath, mode="rb") as f:
                while (key_entry := read_row_hint_file(f)) is not None:
                    key, key_info = key_entry
                    keydir[key] = key_info
        else:
            logger.debug(f"reading store file {filepath}")
            with open(filepath, mode="rb") as f:
                while (key_entry := read_row_store_file(f)) is not None:
                    key, key_info = key_entry
                    keydir[key] = key_info
    return keydir
