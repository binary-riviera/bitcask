import socket
import os

from bitcask.bitcask import Bitcask, BitcaskException, Mode
from tests.bitcask_testcase import DB_PATH, BitcaskTestCase
from tests.test_utils import *
from unittest.mock import patch


class TestBitcask(BitcaskTestCase):

    @patch("uuid.uuid4")
    def test_single_insert(self, mock_uuid):
        # Create a Bitcask instance
        mock_uuid.return_value = "abc123xyz789"
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)
        self.assertEqual(bitcask._current_file, DB_PATH + "/" + "abc123xyz789.store")

        # Insert a key value pair
        key = b"key"
        value = b"value"
        bitcask.put(key, value)
        self.assertEqual(len(bitcask.keydir), 1)
        self.assertEqual(bitcask.keydir[key]["value_pos"], 35)
        self.assertEqual(bitcask.keydir[key]["value_sz"], 5)

        # Get the key value pair out again
        self.assertEqual(bitcask.get(key), value)

    def test_multiple_insert(self):
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)

        key1 = b"key1"
        value1 = b"value1"
        key2 = b"key2"
        value2 = b"value2"

        bitcask.put(key1, value1)
        bitcask.put(key2, value2)
        self.assertEqual(len(bitcask.keydir), 2)
        self.assertEqual(bitcask.get(key1), value1)
        self.assertEqual(bitcask.get(key2), value2)

    def test_empty_key_value(self):
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)

        emptyKey = None
        emptyValue = b""

        self.assertRaisesRegex(
            BitcaskException,
            "Key and Value can't be empty",
            bitcask.put,
            emptyKey,
            emptyValue,
        )
        bitcask.close()

    def test_list_keys(self):
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)
        bitcask.put(b"key1", b"value1")
        bitcask.put(b"key2", b"value2")

        self.assertListEqual(list(bitcask.list_keys()), [b"key1", b"key2"])

    @patch("uuid.uuid4")
    def test_size_threshold_trigger(self, mock_uuid):
        # Create a store file almost at the threshold, then check to see if putting more creates a new store file
        mock_uuid.return_value = "abc123abc123"
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)

        key1 = b"key1"
        big_byte_value = b"a" * 60
        bitcask.put(key1, big_byte_value)
        self.assertEqual(bitcask._current_file, DB_PATH + "/" + "abc123abc123.store")

        mock_uuid.return_value = "xyz789xyz789"
        bitcask.put(key1, big_byte_value)
        self.assertEqual(bitcask._current_file, DB_PATH + "/" + "xyz789xyz789.store")

    @patch("uuid.uuid4")
    def test_merge_simple_single_store(self, mock_uuid):
        mock_uuid.return_value = "abc123abc123"
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)
        key1 = b"key1"
        bitcask.put(key1, b"abc")
        bitcask.put(key1, b"def")
        bitcask.put(key1, b"xyz")

        key2 = b"key2"
        bitcask.put(key2, b"123")
        bitcask.put(key2, b"567")
        bitcask.put(key2, b"789")

        old_size = os.path.getsize(bitcask._current_file)
        bitcask.merge()
        new_size = os.path.getsize(bitcask._current_file)
        self.assertGreater(old_size, new_size)
        self.assertNumHintFiles(0)

    def test_merge_hint_file(self):
        # create multiple version of several big values, and check the hint file has been created
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)
        # in file 1, keys 1 2 3
        bitcask.put(b"key1", b"value1")
        bitcask.put(b"key2", b"value2")
        bitcask.put(b"key3", b"value3")
        # in file 2, keys 4 5 6
        bitcask.put(b"key4", b"value4")
        bitcask.put(b"key5", b"value5")
        bitcask.put(b"key6", b"value6")
        # in file 3, keys 7 8 9
        bitcask.put(b"key7", b"value7")
        bitcask.put(b"key8", b"value8")
        bitcask.put(b"key9", b"value9")
        self.assertNumStoreFiles(4)

        # merge
        bitcask.merge()
        self.assertNumStoreFiles(
            4
        )  # only unique values have been written, so no size improvement
        self.assertNumHintFiles(3)

    def test_read_mode_put_fails(self):
        bitcask = Bitcask(mode=Mode.READ)
        bitcask.open(DB_PATH)
        self.assertRaisesRegex(
            BitcaskException,
            "Mode must be READ_WRITE to PUT",
            bitcask.put,
            b"key",
            b"value",
        )

    def test_get_key_error(self):
        bitcask = Bitcask(mode=Mode.READ)
        bitcask.open(DB_PATH)
        self.assertRaisesRegex(
            BitcaskException, "Couldn't find key b'key' in keydir", bitcask.get, b"key"
        )

    def test_get_generic_error(self):
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)
        bitcask.keydir[b"key"] = None
        with self.assertRaisesRegex(
            BitcaskException,
            "Couldn't GET value, error: 'NoneType' object is not subscriptable",
        ):
            bitcask.get(b"key")

    def test_wrong_type_key_value(self):
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)
        self.assertRaisesRegex(
            BitcaskException, "Key and value must be bytes type", bitcask.put, "key", 3
        )

    def test_delete(self):
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        bitcask.open(DB_PATH)
        bitcask.delete(b"key")
        self.assertEqual(bitcask.get(b"key"), b"DELETED")

    @patch("socket.socket")
    def test_open_server(self, mock_socket):
        bitcask = Bitcask(mode=Mode.READ_WRITE)
        mock_socket.assert_called_once_with(socket.AF_INET, socket.SOCK_STREAM)
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.bind.assert_called_once_with(("localhost", 12345))
        mock_socket_instance.listen.assert_called_once_with(5)
        self.assertIsNotNone(bitcask._server_socket)

    @patch("socket.socket")
    def test_open_server_error(self, mock_socket):
        mock_socket_instance = mock_socket.return_value
        mock_socket_instance.bind.side_effect = socket.error("socket error")
        with self.assertRaisesRegex(
            BitcaskException, "Couldn't start server socket, error: socket error"
        ):
            bitcask = Bitcask(mode=Mode.READ_WRITE)

    def test_multiple_readwrite_error(self):
        bitcask1 = Bitcask(mode=Mode.READ_WRITE)
        with self.assertRaisesRegex(BitcaskException, r"Couldn't start server socket, error: \[Errno 48\] Address already in use"):
            bitcask2 = Bitcask(mode=Mode.READ_WRITE)
            bitcask2.close()
        bitcask1.close()
