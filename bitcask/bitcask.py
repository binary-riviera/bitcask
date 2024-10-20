# bitcask
import os
import uuid
from typing import TypedDict

from bitcask_row import BitcaskRow

DEFAULT_ENCODING = "utf-8"

class BitcaskException(Exception):
    pass

KeyInfo = TypedDict('KeyInfo', {'file_id': str, 'value_sz': int, 'value_pos': int, 'tstamp': int})

class Bitcask:
    """
    Below are the actual defined operations in the specification
    """
    def open(self, directory: str):
        """Open a new or existing Bitcask instance"""
        self.directory = directory
        if len(os.listdir(directory)) == 0:
            print('Directory empty, creating new file...')
            filename = str(uuid.uuid4()) + '.store'
            filepath = os.path.join(directory, filename)
            with open(filepath, mode='a'): pass
            self.current_file = filepath
        else:
            files = os.listdir(directory)
            filepaths = [os.path.join(directory, f) for f in files]
            self.current_file = max(filepaths, key=os.path.getmtime)
        self.keydir: dict[bytes, KeyInfo] = {}
        print(f'Loaded db file {self.current_file}')

        # TODO: we need to construct the keydir here?
        

    def get(self, key: bytes) -> bytes:
        """Get a key value pair from the datastore"""
        try:
            k = self.keydir[key]
            print(k)
            with open(k['file_id'], 'rb') as f:
                f.seek(k['value_pos'])
                value = f.read(k['value_sz'])
                return value
        except KeyError:
            raise BitcaskException(f"Couldn't find key {k} in keydir")

    def put(self, key: bytes, value: bytes):
        """Store a key value pair in the datastore"""
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
        print(self.keydir)


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



if __name__ == '__main__':
    bitcask_handle = Bitcask()
    bitcask_handle.open('./db')
    bitcask_handle.put(b'foo', b'bar')
