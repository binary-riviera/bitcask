import unittest
from bitcask.hint import Hint, bytes_to_hint, hint_to_bytes

class TestHint(unittest.TestCase):
    
    def test_decode_encode_hint(self):
        hint = Hint(timestamp=1, ksz=2, value_sz=3, value_pos=4, key=b'key')
        bytes = hint_to_bytes(hint)
        new_hint = bytes_to_hint(bytes)
        self.assertTupleEqual(hint, new_hint)