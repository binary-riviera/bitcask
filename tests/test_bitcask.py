import unittest

from bitcask.bitcask import Bitcask
from tests.test_utils import *
from unittest.mock import patch

DB_PATH = './tests/db'

class TestBitcask(unittest.TestCase):
    def setUp(self):
        setup_db_folder(DB_PATH)

    def tearDown(self):
        teardown_db_folder(DB_PATH)
    
    @patch('uuid.uuid4')
    def test_single_insert(self, mock_uuid):
        # Create a Bitcask instance
        mock_uuid.return_value = 'abc123xyz789'
        bitcask = Bitcask()
        bitcask.open(DB_PATH)
        self.assertEqual(bitcask.current_file, DB_PATH + '/' + 'abc123xyz789.store')

        # Insert a key value pair
        key = b'key'
        value = b'value'
        bitcask.put(key, value)
        self.assertEqual(len(bitcask.keydir), 1)
        self.assertEqual(bitcask.keydir[key]['value_pos'], 35)
        self.assertEqual(bitcask.keydir[key]['value_sz'], 5)

        # Get the key value pair out again
        self.assertEqual(bitcask.get(key), value)

    @patch('uuid.uuid4')
    def test_multiple_insert(self, mock_uuid):
        mock_uuid.return_value = 'abc123xyz789'
        bitcask = Bitcask()
        bitcask.open(DB_PATH)

        key1 = b'key1'
        value1 = b'value1'
        key2 = b'key2'
        value2 = b'value2'

        bitcask.put(key1, value1)
        bitcask.put(key2, value2)
        self.assertEqual(len(bitcask.keydir), 2)
        self.assertEqual(bitcask.get(key1), value1)
        self.assertEqual(bitcask.get(key2), value2)
        
        

        

