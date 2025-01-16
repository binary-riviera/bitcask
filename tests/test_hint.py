import unittest
from bitcask.hint import Hint, bytes_to_hint, hint_to_bytes, read_hint_file_if_exists
from tests.bitcask_testcase import DB_PATH, BitcaskTestCase


class TestHint(BitcaskTestCase):

    def test_decode_encode_hint(self):
        hint = Hint(timestamp=1, ksz=2, value_sz=3, value_pos=4, key=b"key")
        bytes = hint_to_bytes(hint)
        new_hint = bytes_to_hint(bytes)
        self.assertTupleEqual(hint, new_hint)

    
    def test_read_hint_file_if_exists(self):
        self.assertIsNone(read_hint_file_if_exists('incorrect_file'))
        with open(DB_PATH + '/correct_file.hint', mode="a"):
            pass
        self.assertIsNotNone(read_hint_file_if_exists(DB_PATH + '/correct_file'))