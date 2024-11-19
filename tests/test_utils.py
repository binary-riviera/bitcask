import os
import shutil


def setup_db_folder(db_loc: str):
    if not os.path.exists(db_loc):
        os.makedirs(db_loc)


def teardown_db_folder(db_loc: str):
    shutil.rmtree(db_loc)
