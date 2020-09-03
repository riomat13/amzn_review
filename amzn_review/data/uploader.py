#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sending data periodically."""

import os

import argparse
import asyncio
from asyncio import Queue
import collections
from dataclasses import dataclass
from datetime import datetime
import glob
import gzip
import json
import random
import shutil
import time
import threading
from typing import Callable, Coroutine, List

from botocore.exceptions import ClientError

from settings import Config, get_logger
from amzn_review.aws import AWS
from amzn_review.data.gen import MetadataGenerator

logger = get_logger('amzn_review.data.uploader')

random.seed(1234)


@dataclass(frozen=True)
class FileItem:
    category: str
    filepath: str


class MetadataUploader(object):
    
    def __init__(self,
                 categories: List[str],
                 bucket: str):
        """Server to send review data.

        Args:
            categories: list of str
                target categories for "ASINs" to parse
            bucket: str
                target bucket name to send files
        """
        self.categories = categories
        self._gen = MetadataGenerator(categories)
        self.client = AWS.get_client('s3')

    def run(self):
        count = 0
        while (metadata := self._gen()) is not None:
            metadata = gzip.compress(metadata.encode())

            try:
                # use the same file name as an object name
                resp = self.client.put_object(
                    Body=metadata,
                    Bucket=self.bucket,
                    ContentEncoding='gzip',
                    ContentType='application/json',
                    Key='uploaded/metadata/{:08x}.json.gz'.format(count),
                )
                logger.info(f'Uploaded: {fname}')
            except ClientError as e:
                logger.error(f'{e}: {fname}')
            else:
                count += 1


def load_json_content(filepath):
    """Update json object to upload to S3.
    Data will be compressed with gzip.

    The original input is:
    Original data format is:
        {
          "reviewerID":     ID of the reviewer, e.g. A2SUAM1J3GNN3B
          "asin":           ID of the product, e.g. 0000013714
          "reviewerName":   name of the reviewer
          "vote":           helpful votes of the review
          "style":          a disctionary of the product metadata,
                            e.g., "Format" is "Hardcover"
          "reviewText":     text of the review
          "overall":        rating of the product
          "summary":        summary of the review
          "unixReviewTime": time of the review (unix time)
          "reviewTime":     time of the review (raw)
          "image":          images that users post after received the product
        }
    Further detail is at following link:
        https://nijianmo.github.io/amazon/index.html#sample-review

    Output format will be:
        {
          "reviewerID"
          "asin"
          "reviewerName"
          "vote"
          "reviewText"
          "overall"
          "summary"
          "unixReviewTime" (use current time)
        }
    """

    with gzip.open(filepath, 'rb') as f:
        obj = json.load(f)

    out = {
        'overall': obj.get('overall'),
        'unixReviewTime': int(datetime.now().timestamp()),
        'reviewerID': obj.get('reviewerID'),
        'asin': obj.get('asin'),
        'reviewerName': obj.get('reviewerName'),
        'reviewText': obj.get('reviewText'),
        'summary': obj.get('summary'),
        'vote': obj.get('vote', '0'),
    }

    return gzip.compress(json.dumps(out).encode())


class FileExtractor(object):
    """Extract file and generate asynchronously."""
    def __init__(self, category: str, *, max_interval: float = 1.0):
        self.category = category
        self.max_interval = max_interval

    async def generate(self):
        """Yield review file.
        There is an interval between each yield.
        """
        path = os.path.join(Config.DATA_DIR, self.category, '*.json.gz')
        files = glob.iglob(path)

        for f in files:
            yield FileItem(self.category, f)

            await asyncio.sleep(random.random() * self.max_interval)


class MultipleCategoryFileExtractor(object):
    """FileExtractor for multiple categories."""
    def __init__(self, categories: List[str], max_interval: float = 1.0):
        self.workers = (FileExtractor(cat, max_interval=max_interval) for cat in categories)
        self._lock = threading.RLock()
        self._queue = collections.deque()
        self.count = len(categories)

    async def _generate_by_worker(self, worker):
        async for item in worker.generate():
            with self._lock:
                self._queue.append(item)
        
        with self._lock:
            self._queue.append(None)

    async def generate(self):
        tasks = [
            asyncio.create_task(self._generate_by_worker(worker))
            for worker in self.workers
        ]

        while self.count:
            if self._queue:
                with self._lock:
                    item = self._queue.popleft()
                if item is None:
                    self.count -= 1
                    continue

                yield item
            else:
                await asyncio.sleep(0.1)

        for task in tasks:
            await task

        logger.info('Generated all items')


class ReviewUploader(object):
    """Upload review data to storage."""
    def __init__(self,
                 group: int,
                 handler: Callable,
                 max_interval: float = 1.0):
        """Server to send review data.

        Args:
            group: int
                number of item groups
            handler: function
                data handler function
                this function will take FileItem object as input argument
            max_interval: float (default: 1.0)
                max interval between each upload operation in seconds

        Raises:
            ValueError: raises when group is not positive
            ValueError: raises when interval is negative
        """
        if group < 1:
            raise ValueError('`group` must be positive')
        if max_interval < 0:
            raise ValueError('`max_interval` must be non-negative')

        # only run in single process so synchronous Queue is enough
        self.rest = group
        self.queue = Queue(maxsize=100)
        self._lock = threading.RLock()
        self.interval = max_interval

        self.handler = handler

    async def put(self, item):
        """Put item which will be uploaded to Queue."""
        with self._lock:
            await self.queue.put(item)

    async def run(self) -> None:
        """Run server."""
        # TODO: add stop feature when group is larger than
        #       actual data groups (timeout?)
        while True:
            if self.queue.empty():
                await asyncio.sleep(0.1)
                continue

            for _ in range(self.queue.qsize()):
                with self._lock:
                    item = await self.queue.get()

                if item is None:
                    self.rest -= 1
                    if not self.rest:
                        logger.info('Finished uploaded')
                        return
                    continue

                self.handler(item)

            await asyncio.sleep(random.random() * self.interval)


async def _run(uploader: ReviewUploader,
               categories: List[str],
               worker: int = 2,
               max_interval: float = 1.0) -> None:  # pragma: no cover

    random.shuffle(categories)

    # grouping categories to assign each worker
    group_size = (len(categories) + worker - 1) // worker

    # if # of worker is greater than # of category, put all categories into one worker
    if not group_size:
        worker = 1
        group_size = len(categories)

    # asynchronously put items from each file extractor into internal Queue
    async def put_item(worker):
        nonlocal uploader

        async for item in worker.generate():
            logger.debug(f'putting item: {item.filepath}')
            await uploader.put(item)

        await uploader.put(None)

    idx = 0
    tasks = []

    for i in range(worker):
        # this grouping does not take into acount that each category has different data size,
        # which may cause skewness
        worker = MultipleCategoryFileExtractor(categories[idx:idx+group_size], max_interval=max_interval)
        coro = put_item(worker)
        tasks.append(asyncio.create_task(coro))
        idx += group_size

    tasks.append(asyncio.create_task(uploader.run()))

    logger.info('start uploading')

    for task in tasks:
        await task


def _create_upload_coro(mode: str, worker: int = 2, max_interval: float = 1.0) -> Coroutine:
    """Create upload couroutine.

    Args:
        mode: str
            upload mode, either `local`, `aws` or `db`
            if set to `local`, upload data to local storage
            if set to `aws`, upload data to S3 bucket
            if set to `db`, upload data to database (not implemented)
        worker: int (default: 2)
        max_interval: float (default: 1.0)
    """
    base_key = 'uploaded/reviews/{category}/{fname}'

    if mode == 'aws':
        target = Config.AWS['S3_BUCKET']
        client = AWS.get_client('s3')

        def upload_file(item: FileItem) -> None:
            """Upload file object to S3 bucket."""
            nonlocal base_key
            nonlocal client

            fname = os.path.basename(item.filepath)

            try:
                # use the same file name as an object name
                resp = client.put_object(
                    Body=load_json_content(item.filepath),
                    Bucket=target,
                    ContentEncoding='gzip',
                    ContentType='application/json',
                    Key=base_key.format(category=item.category,
                                        fname=fname),
                )
                logger.info(f'Uploaded: {fname}')
            except ClientError as e:
                logger.error(f'{e}: {fname}')

    elif mode == 'local':
        target = Config.DATA_DIR

        # check if directory exists to upload data
        for category in Config.DATA_CATEGORIES:
            if not os.path.isdir(path := os.path.join(Config.DATA_DIR, 'uploaded', 'reviews', category)):
                os.makedirs(path)

        dirpath = os.path.join(Config.DATA_DIR, base_key)

        def upload_file(item: FileItem) -> None:
            """Upload file to local storage."""
            nonlocal dirpath

            fname = os.path.basename(item.filepath)

            with open(dirpath.format(category=item.category, fname=fname), 'wb') as f:
                logger.info(f'writing file: {item.filepath}')
                f.write(load_json_content(item.filepath))

    elif mode == 'db':
        def upload_file(item: FileItem) -> None:
            """Upload file data to database."""
            pass

        raise NotImplementedError()
    else:
        raise ValueError('`mode` must be chosen from ("local", "aws", "db")')

    async def run() -> None:  # pragma: no cover
        """Execute data uploading operation."""
        nonlocal target
        nonlocal mode

        uploader = ReviewUploader(group=worker,
                                  handler=upload_file,
                                  max_interval=max_interval)

        await _run(uploader, Config.DATA_CATEGORIES, worker)

    return run()


if __name__ == '__main__':
    # TODO: add DB mode
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', type=str, default='local',
                        help='upload type ["local", "aws"] (default: "local")')
    parser.add_argument('--max-interval', type=float, default=1.0,
                        help='interval between upload in seconds(default: 1.0)')

    args = parser.parse_args()

    logger.info(f'Running "{args.mode}" mode')
    executor = _create_upload_coro(mode=args.mode,
                                   worker=Config.DATA_UPLOADER_WORKER,
                                   max_interval=args.max_interval)

    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(executor)
    except KeyboardInterrupt:
        print('Terminating tasks...')
        tasks = asyncio.all_tasks(loop=loop)

        for task in tasks:
            task.cancel()

        group = asyncio.gather(*tasks, return_exceptions=True)
        loop.run_until_complete(group)
    except Exception as e:
        logger.error(f'{type(e).__name__}: {e}')
        raise
    finally:
        logger.info('Closing uploader...')
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
