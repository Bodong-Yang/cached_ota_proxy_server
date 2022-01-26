import sqlite3
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import List, Tuple

@dataclass
class CacheMeta:
    url: str
    hash: str
    size: int
    content_type: str
    content_encoding: str

class OTACacheDB:
    TABLE_NAME: str='ota_cache'
    COLUMNS: dict={
        "url": 0,
        "hash": 1,
        "content_type": 2,
        "content_encoding": 3,
    }

    def __init__(self, db_file: str, init: bool=False):
        self._db_file = db_file
        self._wlock = Lock()
        self._closed = False

        self._connect_db(init)

    def __del__(self):
        self.close()

    def _init_table(self):
        cur = self._con.cursor()

        # create the table
        cur.execute(
        f'''CREATE TABLE {self.TABLE_NAME}(
                    url text UNIQUE PRIMARY KEY, 
                    hash text NOT NULL, 
                    content_type text, 
                    content_encoding text)''')

        self._con.commit()
        cur.close()

    def _connect_db(self, init: bool):
        if init:
            Path(self._db_file).unlink(missing_ok=True)
            self._con = sqlite3.connect(self._db_file)
            self._init_table()
        else:
            self._con = sqlite3.connect(self._db_file)

        # check if the table exists
        cur = self._con.cursor()
        cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (self.TABLE_NAME, ))
        if cur.fetchone() is None:
            self._init_table()

    def remove_url_by_hash(self, hash: str):
        # TODO: !!!
        return

    def remove_urls(self, *urls):
        """
        remove specific URL records from db
        """
        if self._closed:
            raise sqlite3.OperationalError("connect is closed")

        with self._wlock:
            for url in urls:
                t = (url, )
                cur = self._con.cursor()
                cur.execute(f'DELETE FROM {self.TABLE_NAME} WHERE URL=?', t)
            
            self._con.commit()
            cur.close()
    
    def insert_urls(self, *rows: List[Tuple]):
        if self._closed:
            raise sqlite3.OperationalError("connect is closed")

        with self._wlock:
            cur = self._con.cursor()
            cur.executemany(f'INSERT INTO {self.TABLE_NAME} VALUES (?,?,?,?)', rows)

            self._con.commit()
            cur.close()

    def lookup_url(self, url: str) -> CacheMeta:
        if self._closed:
            raise sqlite3.OperationalError("connect is closed")

        cur = self._con.cursor()
        cur.execute('SELECT * FROM ota_cache WHERE URL=?', (url, ))
        row = cur.fetchone()
        if not row:
            return

        return row[self.COLUMNS["hash"]]
        

    def close(self):
        if not self._closed:
            self._con.close()
            self._closed = True
        else:
            raise sqlite3.OperationalError("connect is already closed")

