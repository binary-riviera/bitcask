import unittest

from bitcask.bitcask_row import BitcaskRow

class TestBitcaskRow(unittest.TestCase):

    def test_creation(self):
        key = b'key'
        value = b'value'
        row = BitcaskRow(key, value)
        self.assertEqual(row.key, key)
        self.assertEqual(row.value, value)
        self.assertEqual(row.ksz, 3)
        self.assertEqual(row.value_sz, 5)