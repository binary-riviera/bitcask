# bitcask
import os
import uuid

from bitcask.bitcask_exception import BitcaskException
from bitcask.bitcask_row import BitcaskRow
from bitcask.keydir import KeyDir, KeyInfo, construct_keydir

DEFAULT_ENCODING = "utf-8"
SIZE_THRESHOLD_BYTES = 100

class Bitcask:

    def create_new_store(self):
        """Create a new .store file"""
        print('Creating new store file')
        filename = str(uuid.uuid4()) + '.store'
        filepath = os.path.join(self.directory, filename)
        with open(filepath, mode='a'): pass
        self.current_file = filepath

    """
    Below are the actual defined operations in the specification
    """
    def open(self, directory: str):
        """Open a new or existing Bitcask instance"""
        self.directory = directory
        if len(os.listdir(directory)) == 0:
            self.create_new_store()
        else:
            files = os.listdir(directory)
            filepaths = [os.path.join(directory, f) for f in files]
            self.current_file = max(filepaths, key=os.path.getmtime)
        print(f'Loaded db file {self.current_file}')

        # TODO: When sharing between processes is enabled, we need to share the keydir instead of constructing it here
        self.keydir: KeyDir = construct_keydir(directory)



    def get(self, key: bytes) -> bytes:
        """Get a key value pair from the datastore"""
        try:
            k = self.keydir[key]
            with open(k['file_id'], 'rb') as f:
                f.seek(k['value_pos'])
                value = f.read(k['value_sz'])
                return value
        except KeyError:
            raise BitcaskException(f"Couldn't find key {k} in keydir")
        except Exception as e:
            raise BitcaskException(f"Couldn't GET value, error: {e}")

    def put(self, key: bytes, value: bytes):
        """Store a key value pair in the datastore"""

        if ((key is None or key == b'') or (value is None or value == b'')):
            raise BitcaskException("Key and Value can't be empty")
        
        row = BitcaskRow(key, value)
        with open(self.current_file, 'ab') as f:
            pre_loc = f.tell()
            f.write(row.bytes)
            self.keydir[key] = KeyInfo(
                file_id = self.current_file,
                value_sz = row.value_sz,
                value_pos = pre_loc + row.value_offset,
                tstamp = row.tstamp
            )
        print(f'wrote {len(row.bytes)} byte row to {self.current_file}')

        if os.path.getsize(self.current_file) > SIZE_THRESHOLD_BYTES:
            self.create_new_store()
            


    def delete(self, key: bytes):
        """Delete a value key from the database"""

    def list_keys(self):
        return self.keydir.keys()

    def fold(f, acc0):
        pass

    def merge(self):
        pass

    def sync(self):
        pass

    def close(self):
        pass

