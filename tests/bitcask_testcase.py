import unittest

from tests.test_utils import setup_db_folder, teardown_db_folder
from pathlib import Path

DB_PATH = "./tests/db"


class BitcaskTestCase(unittest.TestCase):
    def setUp(self):
        setup_db_folder(DB_PATH)

    def tearDown(self):
        teardown_db_folder(DB_PATH)

    def assertNumStoreFiles(self, n: int):
        self.assertEqual(len(list(Path(DB_PATH).glob("**/*.store"))), n)

    def assertNumHintFiles(self, n: int):
        self.assertEqual(len(list(Path(DB_PATH).glob("**/*.hint"))), n)
