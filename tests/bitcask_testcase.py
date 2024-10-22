
import unittest

from tests.test_utils import setup_db_folder, teardown_db_folder

DB_PATH = './tests/db'

class BitcaskTestCase(unittest.TestCase):
    def setUp(self):
        setup_db_folder(DB_PATH)

    def tearDown(self):
        teardown_db_folder(DB_PATH)