"""
disk_store module implements DiskStorage class which implements the KV store on the
disk

DiskStorage provides two simple operations to get and set key value pairs. Both key and
value needs to be of string type. All the data is persisted to disk. During startup,
DiskStorage loads all the existing KV pair metadata.  It will throw an error if the
file is invalid or corrupt.

Do note that if the database file is large, then the initialisation will take time
accordingly. The initialisation is also a blocking operation, till it is completed
the DB cannot be used.

Typical usage example:

    disk: DiskStorage = DiskStore(file_name="books.db")
    disk.set(key="othello", value="shakespeare")
    author: str = disk.get("othello")
    # it also supports dictionary style API too:
    disk["hamlet"] = "shakespeare"
"""

import os.path
import time

from constants import HEADER_SIZE
from format import encode_kv, decode_kv, decode_header


# DiskStorage is a Log-Structured Hash Table as described in the BitCask paper. We
# keep appending the data to a file, like a log. DiskStorage maintains an in-memory
# hash table called KeyDir, which keeps the row's location on the disk.
#
# The idea is simple yet brilliant:
#   - Write the record to the disk
#   - Update the internal hash table to point to that byte offset
#   - Whenever we get a read request, check the internal hash table for the address,
#       fetch that and return
#
# KeyDir does not store values, only their locations.
#
# The above approach solves a lot of problems:
#   - Writes are insanely fast since you are just appending to the file
#   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
#       storage, there could be 2-3 disk seeks
#
# However, there are drawbacks too:
#   - We need to maintain an in-memory hash table KeyDir. A database with a large
#       number of keys would require more RAM
#   - Since we need to build the KeyDir at initialisation, it will affect the startup
#       time too
#   - Deleted keys need to be purged from the file to reduce the file size
#
# Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf


class DiskStorage:
    """
    Implements the KV store on the disk

    Args:
        file_name (str): name of the file where all the data will be written. Just
            passing the file name will save the data in the current directory. You may
            pass the full file location too.
    """

    def __init__(self, file_name: str = "data.db"):
        open(file_name, "a").close()

        self.file_store = open(file_name, "rb+")
        self.key_dir: dict[str, int] = {}
        self.__init_db()

    def __init_db(self):
        self.file_store.seek(0, os.SEEK_END)
        total_file_size = self.file_store.tell()
        if total_file_size < 10:
            return

        current_file_pointer = 0
        self.file_store.seek(current_file_pointer)
        while current_file_pointer < total_file_size:
            header_b = self.file_store.read(HEADER_SIZE)
            timestamp, k_sz, v_sz = decode_header(header_b)
            kv_data_b = self.file_store.read(k_sz + v_sz)
            t, k, v = decode_kv(header_b + kv_data_b)
            self.key_dir[k] = current_file_pointer
            current_file_pointer = self.file_store.seek(
                current_file_pointer + HEADER_SIZE + k_sz + v_sz
            )

    def set(self, key: str, value: str) -> None:
        self.file_store.seek(0, os.SEEK_END)
        total_file_size_before_key_set = self.file_store.tell()
        encoded_data = encode_kv(int(time.time()), key, value)
        self.file_store.write(encoded_data[1])
        # set the key dir with the key & the offset
        self.key_dir[key] = total_file_size_before_key_set

    def get(self, key: str) -> str:
        offset_to_jump_to = self.key_dir.get(key)
        if offset_to_jump_to is None:
            return ""

        self.file_store.seek(offset_to_jump_to, 0)
        header_b = self.file_store.read(HEADER_SIZE)
        timestamp, key_size, value_size = decode_header(header_b)
        self.file_store.seek(offset_to_jump_to + HEADER_SIZE, 0)
        kv_data_b = self.file_store.read(key_size + value_size)
        timestamp, k, v = decode_kv(header_b + kv_data_b)
        return v

    def close(self) -> None:
        self.file_store.close()

    def __setitem__(self, key: str, value: str) -> None:
        return self.set(key, value)

    def __getitem__(self, item: str) -> str:
        return self.get(item)
