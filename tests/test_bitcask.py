import unittest
import os

from bitcask.bitcask import Bitcask, BitcaskException, Mode
from bitcask.bitcask_row import BitcaskRow
from tests.bitcask_testcase import DB_PATH, BitcaskTestCase
from tests.test_utils import *
from unittest.mock import patch

class TestBitcask(BitcaskTestCase):
    
    @patch('uuid.uuid4')
    def test_single_insert(self, mock_uuid):
        # Create a Bitcask instance
        mock_uuid.return_value = 'abc123xyz789'
        bitcask = Bitcask(mode=Mode.READ_WRITE)
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
        bitcask = Bitcask(mode=Mode.READ_WRITE)
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
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)

        emptyKey = None
        emptyValue = b''

        self.assertRaises(BitcaskException, bitcask.put, emptyKey, emptyValue)

    def test_list_keys(self):
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)
        bitcask.put(b'key1', b'value1')
        bitcask.put(b'key2', b'value2')

        self.assertListEqual(list(bitcask.list_keys()), [b'key1', b'key2'])

    @patch('uuid.uuid4')
    def test_size_threshold_trigger(self, mock_uuid):
        # Create a store file almost at the threshold, then check to see if putting more creates a new store file
        mock_uuid.return_value = 'abc123abc123'
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)
        
        key1 = b'key1'
        big_byte_value = b'a' * 60
        bitcask.put(key1, big_byte_value)
        self.assertEqual(bitcask._current_file, DB_PATH + '/' + 'abc123abc123.store')

        mock_uuid.return_value = 'xyz789xyz789'
        bitcask.put(key1, big_byte_value)
        self.assertEqual(bitcask._current_file, DB_PATH + '/' + 'xyz789xyz789.store')

    @patch('uuid.uuid4')
    def test_merge_simple_single_store(self, mock_uuid):
        mock_uuid.return_value = 'abc123abc123'
        bitcask = Bitcask(mode=Mode.READ_WRITE)
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
        # TODO: assert that the hint file hasn't been created
    
    def test_merge_hint_file(self):
        # create multiple version of several big values, and check the hint file has been created
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)
        # in file 1, keys 1 2 3
        bitcask.put(b'key1', b'value1')
        bitcask.put(b'key2', b'value2')
        bitcask.put(b'key3', b'value3')
        # in file 2, keys 4 5 6
        bitcask.put(b'key4', b'value4')
        bitcask.put(b'key5', b'value5')
        bitcask.put(b'key6', b'value6')
        # in file 3, keys 7 8 9
        bitcask.put(b'key7', b'value7')
        bitcask.put(b'key8', b'value8')
        bitcask.put(b'key9', b'value9')
        self.assertNumStoreFiles(4)

        # merge
        bitcask.merge()
        self.assertNumStoreFiles(4) # only unique values have been written, so no size improvement
        self.assertNumHintFiles(3)


        

