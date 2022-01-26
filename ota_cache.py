import io
from async_timeout import asyncio
import requests
import subprocess
import shlex
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from hashlib import sha256
from threading import Lock
from pathlib import Path

from . import db

def _subprocess_check_output(cmd: str, *, raise_exception=False) -> str:
    try:
        return subprocess.check_output(shlex.split(cmd), stderr=subprocess.DEVNULL).decode().strip()
    except subprocess.CalledProcessError:
        if raise_exception:
            raise
        return ""

class OTAFile:
    CHUNK_SIZE = 4194_304 # in bytes, 4MB

    def __init__(
        self, url: str, base_path: str,
        session: requests.Session,
        meta: db.CacheMeta=None,
        store_cache: bool=False):

        self.base_path = base_path
        self._session = session
        self._cached = False
        self._store_cache = store_cache

        # cache entry meta
        self.url = url
        self.hash = None
        self.content_type = None
        self.content_encoding = None
        self.size = 0

        # data fp that we used in iterator
        self._fp = None
        self._dst_fp = None # cache entry dst if store_cache

        # life cycle
        self._finished = False
        self.cached_success = False

        if meta: # cache entry presented
            self.hash: str = meta.hash
            self.fpath = Path(self.base_path)/self.hash

            if self.fpath.is_file():
                # check if it is a dangling cache
                # NOTE: we are currently not dealing with dangling entry yet
                self.content_type = meta.content_type
                self.content_encoding = meta.content_encoding
                self.size = meta.size
                self._cached = True
                self._fp = open(self.fpath, 'rb')
        
        if not self._cached:
            # open a queue
            self._queue = Queue()

            # open the remote connection
            self._response = self._session.get(self.url, stream=True)
            self.content_type = self._response.headers.get("Content-Type", 'application/octet-stream')
            self.content_encoding = self._response.headers.get("Content-Encoding", '')
            self._fp = self._response.raw
    
    def background_write_cache(self, callback):
        if not self._store_cache:
            return
        
        try:
            self._hash_f = sha256()
            tmp_fpath = Path(self.base_path)/str(time.time()).replace('.','')
            self.temp_fpath = tmp_fpath
            self._dst_fp = open(tmp_fpath, 'wb')

            while not self._finished or not self._queue.empty():
                data = self._queue.get()
                self._hash_f.update(data)
                self.size += self._dst_fp.write(data)
            
            self._dst_fp.close()
            # rename the file to the hash value
            self.temp_fpath.rename(Path(self.base_path)/self.hash)
            self.hash = self._hash_f.hexdigest()
            self.cached_success = True
        finally:
            # execute the callback function
            callback(self)

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        if self._finished:
            raise ValueError("file is closed")

        chunk = self._fp.read(self.CHUNK_SIZE)
        if len(chunk) == 0: # stream finished
            self._finished = True
            # finish the background cache writing
            if self._store_cache:
                self._queue.put(b'')

            # cleanup
            self._fp.close()
            if self._response is not None:
                self._response.close()

            raise StopAsyncIteration
        else:
            # stream the contents to background caching task non-blockingly
            if self._store_cache:
                self._queue.put_nowait(chunk)

            return chunk

class OTACache:
    CACHE_DIR = "/ota-cache"
    DB_FILE = f"{CACHE_DIR}/cache_db"
    DISK_USE_LIMIT_P = 80 # in p%
    # 0, 10KiB, 100KiB, 500KiB, 1MiB, 5MiB, 10MiB, 100MiB, 1GiB
    BUCKET_FILE_SIZE_LIST = (
        0, 10_240, 102_400, 512_000, 1_048_576, 5_242_880, 10_485_760, 104_857_600, 1_048_576_000) # Bytes

    def __init__(self, cache_enabled: bool, init: bool):
        self._cache_enabled = cache_enabled

        if cache_enabled:
            self._cache_enabled = True
            self._executor = ThreadPoolExecutor()

            # prepare cache dire
            if init:
                shutil.rmtree(self.CACHE_DIR, ignore_errors=True)
            Path(self.CACHE_DIR).mkdir(exist_ok=True)

            self._db = db.OTACacheDB(self.DB_FILE)
            # NOTE: requests doesn't decompress the contents,
            # we cache the contents as its original form, and send
            # to the client
            self._session = requests.Session()

            self._init_buckets()
        else:
            self._cache_enabled = False

    def __del__(self):
        if self._cache_enabled:
            self._executor.shutdown(wait=True)

    def _check_free_space(self) -> bool:
        try:
            cmd = f'findmnt -o SOURCE -n {self.CACHE_DIR}'
            dev = _subprocess_check_output(cmd, raise_exception=True)
            cmd = f'df --output=pcent {dev}'
            current_used_p = _subprocess_check_output(cmd, raise_exception=True)

            # expected output:
            # 0: Use%
            # 1: 33%
            current_used_p = int(current_used_p.splitlines()[-1].strip(' %'))
            return current_used_p < self.DISK_USE_LIMIT_P
        except Exception:
            return False

    def _init_buckets(self):
        """
        only one buckets currently
        """
        self._buckets = dict() # dict[file_size_target]list[hash]
        self._bucket_locks = dict() # dict[file_size]Lock

        # file size larger than 0KiB
        for s in self.BUCKET_FILE_SIZE_LIST:
            self._buckets[s] = list()
            self._bucket_locks[s] = Lock()

    def _register_cache_callback(self, f: OTAFile):
        """
        the callback of register new cached file
        the caller will call this method if the new cached file is valid

        NOTE: the caller should not forget calling this method or 
        retrieve_file_cleanup_file_callback method,
        otherwise there will be dangling cached files
        """
        bucket = self._find_target_bucket_size(f.size)

        if f.cached_success:
            with self._bucket_locks[bucket]:
                # regist the file, append to the bucket
                self._buckets[bucket].append(f.hash)
                # row: (url, hash, content_type, content_encoding)
                row = (f.url, f.hash, f.content_type, f.content_encoding)
                self._db.insert_urls(row)
        else:
            # try to cleanup the dangling cache file
            Path(f.temp_fpath).unlink(missing_ok=True)

    def _find_target_bucket_size(self, file_size: int) -> int:
        s, e = 0, len(self.BUCKET_FILE_SIZE_LIST)-1
        target_size = None

        if file_size >= self.BUCKET_FILE_SIZE_LIST[-1]:
            target_size = self.BUCKET_FILE_SIZE_LIST[-1]
        else:
            while True:
                if abs(e-s) <= 1:
                    target_size = s
                    break

                if file_size <= self.BUCKET_FILE_SIZE_LIST[(s+e)//2]:
                    e = (s+e)//2
                elif file_size > self.BUCKET_FILE_SIZE_LIST[(s+e)//2]:
                    s = (s+e)//2

        return target_size

    def _ensure_free_space(self, size: int) -> bool:
        if not self._check_free_space(): # no enough free space
            bucket_size = self._find_target_bucket_size(size)
            # first check the current bucket
            # if it is the last bucket, only check the current bucket
            bucket = self._buckets[bucket_size]
            files_size, index = 0, 0
            for i, hash in enumerate(bucket):
                if files_size >= size:
                    index = i
                    break
                else:
                    f: Path = Path(self.CACHE_DIR)/hash
                    if f.is_file():
                        # NOTE: deal with dangling cache?
                        files_size += f.stat().st_size

            if files_size >= size:
                # if current bucket is enough for space reservation
                # remove cache entries from the current bucket to reserve space
                for _ in range(index):
                    # get one entry from the bucket
                    hash = bucket[0]
                    bucket = bucket[1:]

                    f: Path = Path(self.CACHE_DIR)/hash
                    f.unlink(missing_ok=True)
                    self._db.remove_url_by_hash(hash)

                # sync the new bucket
                self._buckets[bucket_size] = bucket
                return True
            else:
                # if current bucket is not enough, check higher bucket
                next_bucket, next_bucket_size = None, 0
                for bs in self.BUCKET_FILE_SIZE_LIST[self.BUCKET_FILE_SIZE_LIST.index(bucket_size)+1:]:
                    if len(self._buckets[bs]) > 0:
                        next_bucket = self._buckets[bs]
                        next_bucket_size = bs
                        break
                
                if next_bucket:
                    # get one entry from the target bucket
                    # and then delete it
                    hash = next_bucket[0]
                    self._buckets[next_bucket_size] = next_bucket[1:]

                    f: Path = Path(self.CACHE_DIR)/hash
                    f.unlink(missing_ok=True)
                    self._db.remove_url_by_hash(hash)

                    return True
        else: # there is already enough space
            return True

        return False
    
    def _promote_cache_entry(self, size: int, hash: str):
        bsize = self._find_target_bucket_size(size)
        bucket = self._buckets[bsize]

        try:
            # warm up the cache entry
            entry_index = bucket.index(hash)
            bucket = bucket[:entry_index] + bucket[entry_index+1:]
            bucket.append(hash)
        except Exception:
            # NOTE: not dealing with dangling cache entry now
            pass
    
    # exposed API

    async def retrieve_file(self, url: str, size: int) -> OTAFile:
        res = None
        if not self._cache_enabled:
            # case 1: not using cache, directly download file
            res = OTAFile(
                url=url, base_path=None, session=self._session, store_cache=False)
        else:
            new_cache = False

            cache_meta = self._db.lookup_url(url)
            if cache_meta: # cache hit
                hash = cache_meta.hash
                path = Path(self.CACHE_DIR)/hash
                if not path.is_file():
                    # invalid cache entry found in the db, cleanup it
                    self._db.remove_urls(url)
                    new_cache = True
            
            # check whether we should use cache, not use cache, 
            # or download and cache the new file
            if new_cache:
                # case 2: download and cache new file
                # try to ensure space for new cache
                store_cache = self._ensure_free_space(size)
                res = OTAFile(
                    url=url, base_path=self.CACHE_DIR, session=self._session,
                    store_cache=store_cache)

                if store_cache:
                    # dispatch the background cache writing to executor
                    loop = asyncio.get_running_loop()
                    loop.run_in_executor(
                        self._executor, 
                        res.background_write_cache,
                        self._register_cache_callback
                    )
            else:
                # case 3: use cache
                # warm up the cache
                self._promote_cache_entry(cache_meta.size, cache_meta.hash)
                # use cache
                res = OTAFile(
                    url=url, base_path=self.CACHE_DIR, session=self._session,
                    hash=cache_meta.hash,
                    content_type=cache_meta.content_type,
                    content_encoding=cache_meta.content_encoding)

        return res