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
from typing import List

from botocore.exceptions import ClientError

from settings import Config, get_logger
from amzn_review.aws import AWS
from amzn_review.data.gen import MetadataGenerator

logger = get_logger('amzn_review.data.uploader')

random.seed(1234)


@dataclass(frozen=True)
class _FileItem:
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
                resp = self.s3_client.put_object(
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
            yield _FileItem(self.category, f)

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
                 target: str,
                 group: int,
                 mode: str,
                 interval: float = 1.0,
                 max_send: int = 0):
        """Server to send review data.

        Args:
            target: str
                target path or bucket name to upload file objects
            group: int
                number of groups to put items to this
            mode: str
                upload mode, either `local` or `aws`
                if set to `aws`, upload data to S3 bucket
            interval: float (default: 1.0)
                interval to send in seconds
                If this is set to 0, upload file to S3
                immediately once received file.
                This is an interval from the last time sending files.
            max_send: int (default: 0)
                if set as positive, send at most `max_send` items at once
                no limit when it is set as less than and equal to 0

        Raises:
            ValueError: raises when group is not positive
            ValueError: raises when interval is negative
            ValueError: raises when mode is neither `local` nor `aws`
        """
        if group < 1:
            raise ValueError('`group` must be positive')
        if interval < 0:
            raise ValueError('`interval` must be non-negative')
        if mode not in ('local', 'aws'):
            raise ValueError('`mode` must be chosen from ("local", "aws")')

        # only run in single process so synchronous Queue is enough
        self.rest = group
        self.queue = Queue(maxsize=max_send)
        self._lock = threading.RLock()
        self.interval = interval
        self.max_send = max_send

        if mode == 'local':
            self.path = target
        if mode == 'aws':
            self.bucket = target
            self.s3_client = AWS.get_client('s3')
        self._setup(mode)

    async def put(self, item):
        """Put item to Queue."""
        with self._lock:
            await self.queue.put(item)

    def _setup(self, mode):
        if hasattr(self, 'upload'):
            logger.warning('mode already set')
            return

        base_key = 'uploaded/reviews/{category}/{fname}'

        if mode == 'aws':
            def upload(item):
                nonlocal base_key

                fname = os.path.basename(item.filepath)

                try:
                    # use the same file name as an object name
                    resp = self.s3_client.put_object(
                        Body=load_json_content(item.filepath),
                        Bucket=self.bucket,
                        ContentEncoding='gzip',
                        ContentType='application/json',
                        Key=base_key.format(category=item.category,
                                            fname=fname),
                    )
                    logger.info(f'Uploaded: {fname}')
                except ClientError as e:
                    logger.error(f'{e}: {fname}')

        elif mode == 'local':
            dirpath = os.path.join(Config.DATA_DIR, base_key)

            def upload(item):
                nonlocal dirpath

                fname = os.path.basename(item.filepath)

                with open(dirpath.format(category=item.category, fname=fname), 'wb') as f:
                    logger.info(f'writing file: {item.filepath}')
                    f.write(load_json_content(item.filepath))

        self.upload = upload

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

                self.upload(item)

            await asyncio.sleep(self.interval)


async def _run(uploader: ReviewUploader,
               categories: List[str],
               worker: int = 2,
               max_interval: float = 1.0) -> None:  # pragma: no cover
    group_size = (len(categories) + worker - 1) // worker

    if not group_size:
        worker = 1
        group_size = len(categories)

    # asynchronously put items from each file extractor
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


async def run(categories: List[str], worker: int = 2, max_interval: float = 1.0) -> None:  # pragma: no cover
    """Execute file object uploader for AWS S3 bucket."""
    uploader = ReviewUploader(target=Config.AWS['S3_BUCKET'],
                              group=worker,
                              mode='aws',
                              interval=max_interval,
                              max_send=100)

    await _run(uploader, categories, worker)


async def run_local(categories: List[str], worker: int = 2, max_interval: float = 1.0) -> None:  # pragma: no cover
    """Execute file object uploader to local file system."""
    uploader = ReviewUploader(target=Config.DATA_DIR,
                              group=worker,
                              mode='local',
                              interval=max_interval,
                              max_send=100)

    for category in categories:
        if not os.path.isdir(path := os.path.join(Config.DATA_DIR, 'uploaded', 'reviews', category)):
            os.makedirs(path)

    await _run(uploader, categories, worker)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--local', action='store_true',
                        help='upload data to local storage')
    parser.add_argument('--max-interval', type=float, default=1.0,
                        help='interval between upload in seconds(default: 1.0)')

    args = parser.parse_args()

    loop = asyncio.get_event_loop()

    if args.local:
        logger.info('Running "Local" mode')
        run_fn = run_local
    else:
        run_fn = run

    try:
        loop.run_until_complete(
            run_fn(Config.DATA_CATEGORIES,
                   worker=Config.DATA_UPLOADER_WORKER,
                   max_interval=args.max_interval)
        )
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
