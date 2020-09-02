#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

from contextlib import contextmanager
from datetime import datetime
import json
import gzip
from typing import List, Tuple

import redis

from settings import Config, get_logger
from workflow.exceptions import BaseOperatorError

logger = get_logger('workflow.helper')


def makedir(path):
    if not os.path.isdir(path):
        os.makedirs(path)


@contextmanager
def redis_session():
    # TODO: store connections to pool and reuse them
    r = redis.Redis(host=Config.REDIS['HOST'], port=Config.REDIS['PORT'], db=Config.REDIS['DB'])
    try:
        yield r
    except BaseOperatorError as e:
        logger.error(f'{type(e).__name__}: {e}')
        raise
    except Exception as e:
        # TODO: handle exception
        logger.error(f'{type(e).__name__}: {e}')
        raise
    finally:
        r.close()


def load_review_files(files) -> List[Tuple[str]]:
    outputs = []
    for f in files:
        with gzip.open(f, 'rb') as g:
            data = json.loads(g.read())

        try:
            outputs.append((
                data['reviewerID'],
                data['asin'],
                data.get('reviewerName', 'anonymous'),
                int(data.get('vote', '0').replace(',', '')),
                int(data['overall']),
                datetime.fromtimestamp(int(data['unixReviewTime'])),
                data.get('reviewText'),
                data.get('summary')
            ))
        except KeyError as e:
            logger.error(f'Key not found: {e} - data={data}')
            raise
    return outputs


def load_metadata_files(files) -> List[Tuple[str]]:
    outputs = []
    for f in files:
        with gzip.open(f, 'rb') as g:
            data = json.loads(g.read())

        outputs.append((
            data['asin'],
            data['title'],
            json.dumps(data['feature']),
            data['description'],
            data['price'],
            json.dumps(data['related']),
            json.dumps(data['also_bought']),
            json.dumps(data['also_viewed']),
            json.dumps(data['bought_together']),
            json.dumps(data['buy_after_viewing']),
            json.dumps(data['salesRank']),
            data['brand'],
            json.dumps(data['categories'])
        ))
    return outputs


def cache_filepaths(key, filepaths) -> None:
    """Temporary cache filepath for later use."""
    with redis_session() as r:
        lock = r.lock('cache_filepaths')
        try:
            lock.acquire(blocking=True)
            r.set(f'{key}_base', os.path.dirname(filepaths[0]))
            r.rpush(key, *map(os.path.basename, filepaths))

            # total file counts of all batches
            r.incrby(f'{key}_count', len(filepaths))
        except Exception as e:
            logger.error(f'Failed to cache: {type(e).__name__}: {e}')
            raise
        finally:
            logger.info(f'Cached {len(filepaths)} files to "{key}"')
            lock.release()


def cache_nofile_found(key) -> None:
    with redis_session() as r:
        lock = r.lock('cache_filepaths')
        try:
            lock.acquire(blocking=True)
            r.set(f'{key}_count', 0)
        except Exception as e:
            logger.error(f'Failed to cache: {type(e).__name__}: {e}')
            raise
        finally:
            logger.info(f'Set "0" to "{key}"')
            lock.release()


class fetch_filepaths_from_cache:
    """Generate file paths from cache and consume them."""
    def __init__(self, key):
        self.key = key
        self.count = 0

    def __iter__(self):
        return self

    def __next__(self):
        with redis_session() as r:
            lock = r.lock(f'fetch_filepaths_{self.key}')
            try:
                lock.acquire(blocking=True)
                path = r.get(f'{self.key}_base')
                fname = r.lpop(self.key)
            except Exception as e:
                logger.error(f'Failed to cache: {type(e).__name__}: {e}')
                raise
            finally:
                lock.release()
        if fname is not None:
            self.count += 1
            return os.path.join(path, fname).decode('utf-8')

        logger.info(f'Generated {self.count} file paths')

        raise StopIteration()
