from binascii import crc32
import time

from bitcask.utils import size_to_bytes, tstamp_to_bytes


class BitcaskRow:
    """Bitcask row to be written to the file.
    | crc | tstamp | ksz | value_sz | key | value
    """

    def __init__(self, key: bytes, value: bytes):
        self.tstamp = time.time_ns()
        self.key = key
        self.value = value
        self.ksz = len(key)
        self.value_sz = len(value)

    @property
    def bytes_no_crc(self) -> bytes:
        """Returns the binary representation of the row data"""
        return (
            tstamp_to_bytes(self.tstamp)
            + size_to_bytes(self.ksz)
            + size_to_bytes(self.value_sz)
            + self.key
            + self.value
        )

    @property
    def crc(self) -> bytes:
        """Returns the binary representation of the CRC"""
        return crc32(self.bytes_no_crc).to_bytes(8, "little")

    @property
    def bytes(self) -> bytes:
        """Returns the binary representation of the row"""
        return self.crc + self.bytes_no_crc

    @property
    def value_offset(self) -> int:
        """Returns the offset before the value data starts"""
        # TODO: refactor, you don't need to calculate the bytes manually
        return len(
            self.crc
            + tstamp_to_bytes(self.tstamp)
            + size_to_bytes(self.ksz)
            + size_to_bytes(self.value_sz)
            + self.key
        )

    def __str__(self) -> str:
        return f"crc: {self.crc!r} tstamp: {self.tstamp} ksz: {self.ksz} value_sz: {self.value_sz} key: {self.key!r} value: {self.value!r}"
