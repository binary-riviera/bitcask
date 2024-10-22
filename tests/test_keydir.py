from unittest.mock import patch
from bitcask.bitcask import Bitcask
from tests.bitcask_testcase import DB_PATH, BitcaskTestCase
from bitcask.keydir import construct_keydir, read_row

class TestKeydir(BitcaskTestCase):

    @patch('uuid.uuid4')
    def test_read_row(self, mock_uuid):
        # TODO: mock timestamp
        mock_uuid.return_value = 'abc123xyz789'
        bitcask = Bitcask()
        bitcask.open(DB_PATH)
        key = b'key'
        value = b'value'
        bitcask.put(key, value)
        loaded_key = None
        keydir = {}
        filepath = DB_PATH + '/' + 'abc123xyz789.store'
        with open(filepath, 'rb') as f:
            loaded_key, keydir = read_row(f)
        self.assertEqual(loaded_key, key)
        self.assertEqual(keydir['file_id'], filepath)
        self.assertEqual(keydir['value_sz'], len(value))
        self.assertEqual(keydir['value_pos'], 40)  

    def test_construct_keydir(self):
        # create a few big key value pairs so they're in seperate store files
        bitcask = Bitcask()
        bitcask.open(DB_PATH)
        bigValue = b'a'*100
        bitcask.put(b'key1', bigValue)
        bitcask.put(b'key2', bigValue)
        bitcask.put(b'key3', bigValue)

        construct_keydir(DB_PATH)