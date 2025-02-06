from enum import Enum
import os
import uuid
import logging

from bitcask.bitcask_exception import BitcaskException
from bitcask.bitcask_row import BitcaskRow
from bitcask.keydir import KeyDir, KeyInfo, construct_keydir
from pathlib import Path
from bitcask.hint import Hint, write_hint_file

SIZE_THRESHOLD_BYTES = 100
TOMBSTONE = b"DELETED"

logging.basicConfig(
    level="INFO",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class Mode(Enum):
    READ = "read"
    READ_WRITE = "read_write"


class Bitcask:

    def create_new_store(self) -> None:
        """Create a new .store file"""
        filename = str(uuid.uuid4()) + ".store"
        filepath = os.path.join(self.directory, filename)
        with open(filepath, mode="a"):
            pass
        logger.info(f"created new store file {filename}")
        self._current_file = filepath

    """
    Below are the actual defined operations in the specification
    """

    def __init__(self, mode: Mode = Mode.READ, sync_on_put: bool = False) -> None:
        logger.info(f"Opened bitcask instance in {mode}")
        self._mode = mode
        self._sync_on_put = sync_on_put

    def open(self, directory: str) -> None:
        """Open a new or existing Bitcask instance"""
        self.directory = directory
        if len(os.listdir(directory)) == 0:
            self.create_new_store()
        else:
            files = os.listdir(directory)
            filepaths = [os.path.join(directory, f) for f in files]
            self._current_file = max(filepaths, key=os.path.getmtime)
        logger.info(f"loaded db file {self._current_file}")
        self.keydir: KeyDir = construct_keydir(directory)

    def get(self, key: bytes) -> bytes:
        """Get a key value pair from the datastore"""
        try:
            k = self.keydir[key]
            with open(k["file_id"], "rb") as f:
                f.seek(k["value_pos"])
                value = f.read(k["value_sz"])
                return value
        except KeyError:
            raise BitcaskException(f"Couldn't find key {key} in keydir")
        except Exception as e:
            raise BitcaskException(f"Couldn't GET value, error: {e}")

    def put(self, key: bytes, value: bytes) -> None:
        """Store a key value pair in the datastore"""
        if self._mode == Mode.READ:
            raise BitcaskException("Mode must be READ_WRITE to PUT")

        if (key is None or key == b"") or (value is None or value == b""):
            raise BitcaskException("Key and Value can't be empty")

        if type(key) != bytes or type(value) != bytes:
            raise BitcaskException("Key and value must be bytes type")

        if value == TOMBSTONE:
            logger.info(f"Deleting key {key}")

        row = BitcaskRow(key, value)
        with open(self._current_file, "ab") as f:
            pre_loc = f.tell()
            f.write(row.bytes)
            logger.info(pre_loc + row.value_offset)
            self.keydir[key] = KeyInfo(
                file_id=self._current_file,
                value_sz=row.value_sz,
                value_pos=pre_loc + row.value_offset,
                tstamp=row.tstamp,
            )
        logger.info(f"wrote {len(row.bytes)} byte row to {self._current_file}")

        if os.path.getsize(self._current_file) > SIZE_THRESHOLD_BYTES:
            self.create_new_store()

    def delete(self, key: bytes) -> None:
        """Delete a value key from the database"""
        self.put(key, TOMBSTONE)

    def list_keys(self) -> list[bytes]:
        return list(self.keydir.keys())

    def merge(self) -> None:
        """Merge the store files, keeping only the current value for each key and removing deleted keys"""
        # easiest way to do this is use the keydir, since it should always be up to date
        key_values: list[tuple[bytes, bytes]] = []
        for key in self.keydir:
            key_values.append(
                (key, self.get(key))
            )  # TODO: is there anyway around having to get all of these?
        logger.info(f"Read {len(key_values)} values from store files")

        # now we have all the values, we can delete all the files
        for path in Path(self.directory).glob("**/*"):
            path.unlink()

        assert len(os.listdir(self.directory)) == 0

        logger.debug(f"Rewriting stores and constructing hint files...")
        # then we rewrite the new values to new files, and construct the hint files simultaneously
        self.keydir = {}
        self.create_new_store()
        current_hint_file = self._current_file
        hints: list[Hint] = []
        for key, value in key_values:
            if value == TOMBSTONE:
                continue  # we can just ignore deleted values
            self.put(key, value)
            hints.append(
                Hint(
                    timestamp=self.keydir[key]["tstamp"],
                    ksz=len(key),
                    value_sz=len(value),
                    value_pos=self.keydir[key]["value_pos"],
                    key=key,
                )
            )
            if self._current_file != current_hint_file:
                write_hint_file(current_hint_file, hints)
                logger.debug(
                    f"Wrote {len(hints)} hints to hint file {current_hint_file}"
                )
                current_hint_file = self._current_file
                hints = []

    def sync(self) -> None:
        pass

    def close(self) -> None:
        pass
