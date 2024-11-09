import unittest
import os

from bitcask.bitcask import Bitcask, BitcaskException
from tests.bitcask_testcase import DB_PATH, BitcaskTestCase
from tests.test_utils import *
from unittest.mock import patch

class TestBitcask(BitcaskTestCase):
    
    @patch('uuid.uuid4')
    def test_single_insert(self, mock_uuid):
        # Create a Bitcask instance
        mock_uuid.return_value = 'abc123xyz789'
        bitcask = Bitcask()
        bitcask.open(DB_PATH)
        self.assertEqual(bitcask._current_file, DB_PATH + '/' + 'abc123xyz789.store')

        # Insert a key value pair
        key = b'key'
        value = b'value'
        bitcask.put(key, value)
        self.assertEqual(len(bitcask.keydir), 1)
        self.assertEqual(bitcask.keydir[key]['value_pos'], 35)
        self.assertEqual(bitcask.keydir[key]['value_sz'], 5)

        # Get the key value pair out again
        self.assertEqual(bitcask.get(key), value)

    def test_multiple_insert(self):
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

    def test_empty_key_value(self):
        bitcask = Bitcask()
        bitcask.open(DB_PATH)

        emptyKey = None
        emptyValue = b''

        self.assertRaises(BitcaskException, bitcask.put, emptyKey, emptyValue)

    def test_list_keys(self):
        bitcask = Bitcask()
        bitcask.open(DB_PATH)
        bitcask.put(b'key1', b'value1')
        bitcask.put(b'key2', b'value2')

        self.assertListEqual(list(bitcask.list_keys()), [b'key1', b'key2'])

    @patch('uuid.uuid4')
    def test_size_threshold_trigger(self, mock_uuid):
        # Create a store file almost at the threshold, then check to see if putting more creates a new store file
        mock_uuid.return_value = 'abc123abc123'
        bitcask = Bitcask()
        bitcask.open(DB_PATH)
        
        key1 = b'key1'
        big_byte_value = b'a' * 60
        bitcask.put(key1, big_byte_value)
        self.assertEqual(bitcask._current_file, DB_PATH + '/' + 'abc123abc123.store')

        mock_uuid.return_value = 'xyz789xyz789'
        bitcask.put(key1, big_byte_value)
        self.assertEqual(bitcask._current_file, DB_PATH + '/' + 'xyz789xyz789.store')

    @patch('uuid.uuid4')
    def test_merge(self, mock_uuid):
        mock_uuid.return_value = 'abc123abc123'
        
        """
        To test the merge function for a single file, we need to do the following:
        1. put multiple key value pairs for the same key
        
        """
        bitcask = Bitcask()
        bitcask.open(DB_PATH)
        key1 = b'key1'
        bitcask.put(key1, b'abc')
        bitcask.put(key1, b'def')
        bitcask.put(key1, b'xyz')
        
        key2 = b'key2'
        bitcask.put(key2, b'123')
        bitcask.put(key2, b'567')
        bitcask.put(key2, b'789')
        
        old_size = os.path.getsize(bitcask._current_file)
        
        bitcask.merge()

        new_size = os.path.getsize(bitcask._current_file)
        
        self.assertGreater(old_size, new_size)

        

