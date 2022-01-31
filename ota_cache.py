import asyncio
import requests
import subprocess
import shlex
import shutil
import time
import typing
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from hashlib import sha256
from http import HTTPStatus
from threading import Lock, Event
from typing import Dict
from pathlib import Path

from . import db
from .config import config as cfg

import logging

logger = logging.getLogger(__name__)


def _subprocess_check_output(cmd: str, *, raise_exception=False) -> str:
    try:
        return (
            subprocess.check_output(shlex.split(cmd), stderr=subprocess.DEVNULL)
            .decode()
            .strip()
        )
    except subprocess.CalledProcessError:
        if raise_exception:
            raise
        return ""


class _Bucket(OrderedDict):
    def __init__(self, size: int, cache_dir: str):
        super().__init__()
        self._lock = Lock()
        self.size = size

    def add_entry(self, key):
        """
        newly added item will be added to the last(right)
        this method can also be used to warm up an entry
        """
        self[key] = None

    def warm_up_entry(self, key):
        try:
            self.move_to_end(key)
        except KeyError:
            return

    def popleft(self) -> str:
        try:
            res, _ = self.popitem(last=False)
            return res
        except KeyError:
            return

    def reserve_space(self, size: int) -> list:
        enough_space = False
        with self._lock:
            hash_list, files_list = [], []
            space_available = None
            for h in self:
                if space_available >= size:
                    break
                else:
                    f: Path = Path(cfg.BASE_DIR) / h
                    if f.is_file():
                        # TODO: deal with dangling cache?
                        space_available += f.stat().st_size
                        hash_list.append(h)
                        files_list.append(f)

            if space_available >= size:
                enough_space = True
                # we can reserve enough space from current bucket
                for h in hash_list:
                    del self[h]

        if enough_space:
            for f in files_list:
                f.unlink(missing_ok=True)
            return hash_list

        return


class OTAFile:
    def __init__(
        self,
        url: str,
        fp: typing.BinaryIO,
        meta: db.CacheMeta,
        store_cache: bool = False,
        free_space_event: Event = None,
    ):
        logger.debug(f"new OTAFile request: {url}")
        self._store_cache = store_cache
        self._free_space_event = free_space_event
        # NOTE: for new cache entry meta, the hash and size are not set yet
        self.meta = meta

        # data stream
        self._fp = fp
        self._queue = None

        # life cycle
        self.finished = False
        self.cached_success = False

        # prepare for data streaming
        if store_cache:
            self._hash_f = sha256()
            self._queue = Queue()

    def background_write_cache(self, callback):
        if not self._store_cache or not self._free_space_event:
            return

        try:
            logger.debug(f"start to cache for {self.meta.url}...")
            tmp_fpath = Path(cfg.BASE_DIR) / str(time.time()).replace(".", "")
            self.temp_fpath = tmp_fpath
            self._dst_fp = open(tmp_fpath, "wb")

            while not self.finished or not self._queue.empty():
                if not self._free_space_event.is_set():
                    # if the free space is not enough duing caching
                    # abort the caching
                    logger.error(
                        f"not enough free space during caching url={self.meta.url}, abort"
                    )
                    break

                try:
                    data = self._queue.get(timeout=360)
                except Exception:
                    logger.error(f"timeout caching for {self.meta.url}, abort")
                    break

                self._hash_f.update(data)
                self.meta.size += self._dst_fp.write(data)

            self._dst_fp.close()
            if self.finished and self.meta.size > 0:  # not caching 0 size file
                # rename the file to the hash value
                hash = self._hash_f.hexdigest()[:16]

                self.meta.hash = hash
                self.temp_fpath.rename(Path(cfg.BASE_DIR) / hash)
                self.cached_success = True
                logger.debug(f"successfully cache {self.meta.url}")
            # NOTE: if queue is empty but self._finished is not set
            # an unfinished caching might happen
        finally:
            # commit the cache via callback
            self._dst_fp.close()
            callback(self)

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        if self.finished:
            raise ValueError("file is closed")

        chunk = self._fp.read(cfg.CHUNK_SIZE)
        if len(chunk) == 0:  # stream finished
            self.finished = True
            # finish the background cache writing
            if self._store_cache:
                self._queue.put(b"")

            # cleanup
            self._fp.close()

            raise StopAsyncIteration
        else:
            # stream the contents to background caching task non-blockingly
            if self._store_cache:
                self._queue.put_nowait(chunk)

            return chunk


class OTACache:
    CACHE_DIR = "/ota-cache"
    DB_FILE = f"{CACHE_DIR}/cache_db"
    DISK_USE_LIMIT_P = 70  # in p%
    DISK_USE_PULL_INTERVAL = 2  # in seconds
    BUCKET_FILE_SIZE_LIST = (
        0,
        10_240,  # 10KiB
        102_400,  # 100KiB
        512_000,  # 500KiB
        1_048_576,  # 1MiB
        5_242_880,  # 5MiB
        10_485_760,  # 10MiB
        104_857_600,  # 100MiB
        1_048_576_000,  # 1GiB
    )  # Bytes

    def __init__(
        self,
        cache_enabled: bool,
        init: bool,
        upper_proxy: str = None,
        enable_https: bool = False,
    ):
        logger.debug(f"init ota cache({cache_enabled=}, {init=}, {upper_proxy=})")

        self._closed = False
        self._cache_enabled = cache_enabled
        self._enable_https = enable_https
        self._executor = ThreadPoolExecutor()

        self._enough_free_space_event = Event()

        if cache_enabled:
            self._cache_enabled = True

            # prepare cache dire
            if init:
                shutil.rmtree(self.CACHE_DIR, ignore_errors=True)
            Path(self.CACHE_DIR).mkdir(exist_ok=True, parents=True)

            # dispatch a background task to pulling the disk usage info
            self._executor.submit(self._background_check_free_space)

            self._db = db.OTACacheDB(self.DB_FILE)
            # NOTE: requests doesn't decompress the contents,
            # we cache the contents as its original form, and send
            # to the client
            self._session = requests.Session()
            if upper_proxy:
                # override the proxy setting if we configure one
                proxies = {"http": upper_proxy, "https": ""}
                self._session.proxies.update(proxies)
                # if upper proxy presented, we must disable https
                self._enable_https = False

            self._init_buckets()
        else:
            self._cache_enabled = False

    def close(self, cleanup: bool = False):
        logger.debug(f"shutdown ota-cache({cleanup=})...")
        if self._cache_enabled and not self._closed:
            self._closed = True
            self._executor.shutdown(wait=True)
            self._db.close()

            if cleanup:
                shutil.rmtree(self.CACHE_DIR, ignore_errors=True)
        logger.info("shutdown ota-cache completed")

    def _background_check_free_space(self) -> bool:
        while not self._closed:
            try:
                cmd = f"df --output=pcent {self.CACHE_DIR}"
                current_used_p = _subprocess_check_output(cmd, raise_exception=True)

                # expected output:
                # 0: Use%
                # 1: 33%
                current_used_p = int(current_used_p.splitlines()[-1].strip(" %"))
                if current_used_p < self.DISK_USE_LIMIT_P:
                    self._enough_free_space_event.set()
                else:
                    self._enough_free_space_event.clear()
            except Exception:
                self._enough_free_space_event.clear()

            time.sleep(self.DISK_USE_PULL_INTERVAL)

    def _init_buckets(self):
        self._buckets: Dict[_Bucket] = dict()  # dict[file_size_target]_Bucket

        for s in self.BUCKET_FILE_SIZE_LIST:
            self._buckets[s] = _Bucket(size=s, cache_dir=self.CACHE_DIR)

    def _register_cache_callback(self, f: OTAFile):
        """
        the callback for finishing up caching

        NOTE:
        1. if the free space is used up duing caching,
        the caching will terminate immediately.
        2. if the free space is used up after cache writing finished,
        we will first try reserving free space, if fails,
        then we delete the already cached file.
        """
        meta = f.meta
        if f.cached_success and self._ensure_free_space(meta.size):
            logger.debug(f"commit cache for {meta.url}...")
            bs = self._find_target_bucket_size(meta.size)
            self._buckets[bs].add_entry(meta.hash)

            # register to the database
            self._db.insert_urls(meta)
        else:
            # try to cleanup the dangling cache file
            Path(f.temp_fpath).unlink(missing_ok=True)

    def _find_target_bucket_size(self, file_size: int) -> int:
        if file_size < 0:
            raise ValueError(f"invalid file size {file_size}")

        s, e = 0, len(self.BUCKET_FILE_SIZE_LIST) - 1
        target_size = None

        if file_size >= self.BUCKET_FILE_SIZE_LIST[-1]:
            target_size = self.BUCKET_FILE_SIZE_LIST[-1]
        else:
            idx = None
            while True:
                if abs(e - s) <= 1:
                    idx = s
                    break

                if file_size <= self.BUCKET_FILE_SIZE_LIST[(s + e) // 2]:
                    e = (s + e) // 2
                elif file_size > self.BUCKET_FILE_SIZE_LIST[(s + e) // 2]:
                    s = (s + e) // 2
            target_size = self.BUCKET_FILE_SIZE_LIST[idx]

        return target_size

    def _ensure_free_space(self, size: int) -> bool:
        if not self._enough_free_space_event.is_set():  # no enough free space
            bs = self._find_target_bucket_size(size)
            bucket: _Bucket = self._buckets[bs]

            # first check the current bucket
            hash_list = bucket.reserve_space(size)
            if hash_list:
                self._db.remove_url_by_hash(*hash_list)
                return True

            else:  # if current bucket is not enough, check higher bucket
                entry_to_clear = None
                for bs in self.BUCKET_FILE_SIZE_LIST[
                    self.BUCKET_FILE_SIZE_LIST.index(bs) + 1 :
                ]:
                    bucket = self._buckets[bs]
                    entry_to_clear = bucket.popleft()

                if entry_to_clear:
                    # get one entry from the target bucket
                    # and then delete it

                    f: Path = Path(self.CACHE_DIR) / entry_to_clear
                    f.unlink(missing_ok=True)
                    self._db.remove_url_by_hash(entry_to_clear)

                    return True

        else:  # there is already enough space
            return True

        return False

    def _promote_cache_entry(self, cache_meta: db.CacheMeta):
        bs = self._find_target_bucket_size(cache_meta.size)
        bucket: _Bucket = self._buckets[bs]
        bucket.warm_up_entry(cache_meta.hash)

    def _open_fp_by_cache(self, meta: db.CacheMeta) -> typing.BinaryIO:
        hash: str = meta.hash
        fpath = Path(self.CACHE_DIR) / hash

        if fpath.is_file():
            return open(fpath, "rb")

    def _open_fp_by_requests(
        self, raw_url: str
    ) -> typing.Tuple[typing.BinaryIO, db.CacheMeta]:
        url = raw_url
        if self._enable_https:
            url = raw_url.replace("http", "https")

        response = self._session.get(url, stream=True)
        if response.status_code != HTTPStatus.OK:
            return

        # assembling output cachemeta
        # NOTE: output cachemeta doesn't have hash and size set yet
        meta = db.CacheMeta(
            url=url, hash=None, content_encoding=None, content_type=None, size=0
        )

        meta.content_type = response.headers.get(
            "Content-Type", "application/octet-stream"
        )
        meta.content_encoding = response.headers.get("Content-Encoding", "")

        # return the raw urllib3.response.HTTPResponse and a new CacheMeta instance
        return response.raw, meta

    # exposed API
    async def retrieve_file(self, url: str) -> OTAFile:
        logger.debug(f"try to retrieve file from {url=}")
        if self._closed:
            raise ValueError("ota cache pool is closed")

        res = None
        if not self._cache_enabled:
            # case 1: not using cache, directly download file
            fp, meta = self._open_fp_by_requests(url)

            res = OTAFile(url=url, fp=fp, meta=meta)
        else:
            no_cache_available = True

            cache_meta = self._db.lookup_url(url)
            if cache_meta:  # cache hit
                logger.debug(f"cache hit for {url=}\n, {cache_meta=}")
                path = Path(self.CACHE_DIR) / cache_meta.hash
                if not path.is_file():
                    # invalid cache entry found in the db, cleanup it
                    logger.error(f"dangling cache entry found: \n{cache_meta=}")
                    self._db.remove_urls(url)
                else:
                    no_cache_available = False

            # check whether we should use cache, not use cache,
            # or download and cache the new file
            if no_cache_available:
                # case 2: download and cache new file
                logger.debug(f"try to download and cache {url=}")
                fp, meta = self._open_fp_by_requests(url)
                res = OTAFile(
                    url=url,
                    fp=fp,
                    meta=meta,
                    store_cache=True,
                    free_space_event=self._enough_free_space_event,
                )

                # dispatch the background cache writing to executor
                loop = asyncio.get_running_loop()
                loop.run_in_executor(
                    self._executor,
                    res.background_write_cache,
                    self._register_cache_callback,
                )
            else:
                # case 3: use cache
                logger.debug(f"use cache for {url=}")
                self._promote_cache_entry(cache_meta)

                fp = self._open_fp_by_cache(cache_meta)
                # use cache
                res = OTAFile(url=url, fp=fp, meta=cache_meta)

        return res
