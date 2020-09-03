#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

import unittest
from unittest.mock import patch

import asyncio
import collections
import gzip
import json

from settings import Config
Config.test_config()

from amzn_review.data.uploader import (
    FileItem,
    load_json_content,
    MetadataUploader,
    FileExtractor,
    MultipleCategoryFileExtractor,
    ReviewUploader,
    _run,
    _create_upload_coro
)


class _CollectData(object):
    """Counter items to be sent."""

    def __init__(self, key=None, index=0):
        """Target item to store.
        If `key` is given, store by kwargs by the `key`,
        otherwise, store item from args by `index`."""
        self.key = key
        self.index = 0
        self.items = []

    def __iter__(self):
        return iter(self.items)

    def collect(self, *args, **kwargs):
        if self.key is None:
            self.items.append(args[self.index])
        elif (item := kwargs.get(self.key)) is not None:
            self.items.append(item)

    def count(self):
        return len(self.items)

    def is_match(self, targets):
        return all(item==target for item, target in zip(self.items, targets))


class LoadJSONContentTest(unittest.TestCase):

    @patch('builtins.open')
    @patch('amzn_review.data.uploader.json.load')
    def test_parse_and_compress(self, json_load, mock_open):
        """Check if data is returned with proper types."""
        test_data = {
            'overall': 5.0,
            'reviewTime': '9 23 2015',
            'reviewerID': 'A0000000001',
            'asin': '0123456789',
            'reviewerName': 'testuser',
            'reviewText': 'this is a test text',
            'summary': 'test text',
        }

        json_load.return_value = test_data

        res = load_json_content('test')
        mock_open.assert_called_once_with('test', 'rb')

        self.assertTrue(isinstance(res, bytes))

        res = gzip.decompress(res).decode('utf8')
        res = json.loads(res)

        self.assertEqual(res.get('overall'), test_data['overall'])
        self.assertEqual(res.get('reviewerID'), test_data['reviewerID'])
        self.assertEqual(res.get('asin'), test_data['asin'])
        self.assertEqual(res.get('reviewerName'), test_data['reviewerName'])
        self.assertEqual(res.get('reviewText'), test_data['reviewText'])
        self.assertEqual(res.get('summary'), test_data['summary'])
        self.assertTrue(isinstance(res.get('unixReviewTime'), int))


class FileExtractorTest(unittest.IsolatedAsyncioTestCase):

    @patch('amzn_review.data.uploader.asyncio.sleep')
    @patch('amzn_review.data.uploader.glob.iglob')
    async def test_generate_files(self, iglob, *unused):
        targets = ['test1', 'test2', 'test3']
        iglob.return_value = iter(targets)
        client = FileExtractor('test')

        res = [item async for item in client.generate()]

        self.assertEqual(len(res), len(targets))
        for item in res:
            self.assertTrue(hasattr(item, 'category'))
            self.assertTrue(hasattr(item, 'filepath'))


class MultipleCategoryFileExtractorTest(unittest.IsolatedAsyncioTestCase):

    @patch('amzn_review.data.uploader.random.random', return_value=1e-4)
    async def test_spawn_files_from_clients(self, *unused):

        async def mock_func(*args, **kwargs):
            for i in range(10):
                yield i

        with patch.object(FileExtractor, 'generate', side_effect=mock_func):
            extractor = MultipleCategoryFileExtractor(['test1', 'test2', 'test3'])
            res = [item async for item in extractor.generate()]

        self.assertEqual(len(res), 30)


class ReviewUploaderTest(unittest.IsolatedAsyncioTestCase):

    # test with 2 groups
    targets = (
        FileItem('test1', 'data/test1/1.json'),
        FileItem('test1', 'data/test1/2.json'),
        FileItem('test1', 'data/test1/3.json'),
        None,
        FileItem('test2', 'data/test2/4.json'),
        FileItem('test2', 'data/test2/5.json'),
        None
    )

    @patch('amzn_review.data.uploader.AWS.get_client')
    def test_basic_type(self, *_):
        fn = lambda: None
        # check group
        with self.assertRaises(ValueError):
            ReviewUploader(0, fn, 1)

        # check interval
        with self.assertRaises(ValueError):
            ReviewUploader(2, fn, -1)

    async def test_uploader_is_called(self):
        counter = _CollectData()

        upload_fn = lambda f: counter.collect(f.filepath)

        uploader = ReviewUploader(2, upload_fn, 0.0)

        for i, f in enumerate(self.targets):
            await uploader.put(f)

        await uploader.run()

        # check if bodies are taken from appropriate files
        target_items = tuple(t.filepath for t in self.targets if t is not None)

        self.assertEqual(counter.count(), len(target_items))
        self.assertTrue(
            counter.is_match(target_items)
        )


class RunUploadCoroutineTest(unittest.IsolatedAsyncioTestCase):

    @patch('amzn_review.data.uploader.MultipleCategoryFileExtractor')
    async def test_run_coroutine(self, Extractor):

        class MockWorker:
            def __init__(self, *args, **kwargs):
                pass

            async def generate(self):
                for i in range(10):
                    yield FileItem(i, i)

        class MockUploader(_CollectData):

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

                self.called = 0
                self.n_workers = 0

            async def put(self, item):
                if item is None:
                    self.n_workers += 1
                else:
                    self.collect(item.filepath)
                await asyncio.sleep(0.0)

            async def run(self):
                self.called += 1
                await asyncio.sleep(0.0)

        Extractor.side_effect = MockWorker

        uploader = MockUploader()

        n_workers = 4
        await _run(uploader, list(range(n_workers)), worker=n_workers)

        self.assertEqual(uploader.called, 1)
        self.assertEqual(uploader.n_workers, n_workers)
        self.assertEqual(uploader.count(), 10 * n_workers)

        for i, (item, cnt) in enumerate(collections.Counter(uploader.items).items()):
            self.assertEqual(item, i)
            self.assertEqual(cnt, n_workers)


@patch('amzn_review.data.uploader.ReviewUploader')
@patch('amzn_review.data.uploader._run')
class RunUploadCoroutineTest(unittest.IsolatedAsyncioTestCase):

    data = {
        'test': 'test_content'
    }

    def _get_handler(self, uploader):
        return uploader.call_args[1]['handler']

    @patch('builtins.open')
    async def test_uploade_local_storage(self, mock_open, mock_run, mock_uploader):
        coro = _create_upload_coro('local', 2, max_interval=0)
        await coro

        mock_run.assert_called_once()

        handler = self._get_handler(mock_uploader)

        item = FileItem('fileitem', 'testpath')

        # check if handler open file and write data in local storage
        with patch('amzn_review.data.uploader.load_json_content', return_value=self.data):
            handler(item)

        mock_open.assert_called_once()

        # check if data is properly written to file
        write = mock_open.return_value.__enter__.return_value.write
        write.assert_called_once_with(self.data)

    @patch('amzn_review.data.uploader.AWS.get_client')
    async def test_upload_to_s3(self, boto3client, mock_run, mock_uploader):
        """check data is sent via boto3 client."""
        coro = _create_upload_coro('aws', 2, max_interval=0)
        await coro

        mock_run.assert_called_once()

        handler = self._get_handler(mock_uploader)

        item = FileItem('fileitem', 'testpath')

        # check if handler upload data to S3 via boto3 client
        with patch('amzn_review.data.uploader.load_json_content', return_value=self.data):
            handler(item)

        boto3client.assert_called_once()
        put_obj = boto3client.return_value.put_object
        put_obj.assert_called_once()

        # check if target data is used to upload
        self.assertEqual(put_obj.call_args[1]['Body'], self.data)


if __name__ == '__main__':
    unittest.main()
