from unittest.mock import patch, ANY
from bitcask.bitcask import Bitcask, Mode
from tests.bitcask_testcase import DB_PATH, BitcaskTestCase
from bitcask.keydir import construct_keydir, read_row


class TestKeydir(BitcaskTestCase):

    @patch("uuid.uuid4")
    def test_read_row(self, mock_uuid):
        mock_uuid.return_value = "abc123xyz789"
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)
        key = b"key"
        value = b"value"
        bitcask.put(key, value)
        loaded_key = None
        keydir = {}
        filepath = DB_PATH + "/" + "abc123xyz789.store"
        with open(filepath, "rb") as f:
            loaded_key, keydir = read_row(f)
        self.assertEqual(loaded_key, key)
        self.assertEqual(keydir["file_id"], filepath)
        self.assertEqual(keydir["value_sz"], len(value))
        self.assertEqual(keydir["value_pos"], 40)

    def test_read_row_eof_reached(self):
        # create empty file
        with open(DB_PATH + "/abc123.store", "wb"):
            pass
        with open(DB_PATH + "/abc123.store", "rb") as f:
            self.assertIsNone(read_row(f))

    @patch("uuid.uuid4")
    def test_construct_keydir(self, mock_uuid):
        # create a few big key value pairs so they're in seperate store files
        mock_uuid.side_effect = ["abc123", "def456", "ghi789", "jkl123"]
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)
        bigValue = b"a" * 100

        bitcask.put(b"key1", bigValue)
        bitcask.put(b"key2", bigValue)
        bitcask.put(b"key3", bigValue)

        expected_keydir = {
            b"key1": {
                "file_id": DB_PATH + "/abc123.store",
                "value_sz": 100,
                "value_pos": 136,
                "tstamp": ANY,
            },
            b"key2": {
                "file_id": DB_PATH + "/def456.store",
                "value_sz": 100,
                "value_pos": 136,
                "tstamp": ANY,
            },
            b"key3": {
                "file_id": DB_PATH + "/ghi789.store",
                "value_sz": 100,
                "value_pos": 136,
                "tstamp": ANY,
            },
        }

        keydir = construct_keydir(DB_PATH)

        self.assertDictEqual(keydir, expected_keydir)
