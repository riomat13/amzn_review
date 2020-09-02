#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

import unittest
from unittest.mock import patch

import asyncio
import gzip
import json

from settings import Config
Config.test_config()

from amzn_review.data.uploader import (
    _FileItem,
    load_json_content,
    MetadataUploader,
    FileExtractor,
    MultipleCategoryFileExtractor,
    ReviewUploader
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


@patch('amzn_review.data.uploader.load_json_content', side_effect=lambda x: x)
class ReviewUploaderTest(unittest.IsolatedAsyncioTestCase):

    # test with 2 groups
    targets = (
        _FileItem('test1', 'data/test1/1.json'),
        _FileItem('test1', 'data/test1/2.json'),
        _FileItem('test1', 'data/test1/3.json'),
        None,
        _FileItem('test2', 'data/test2/4.json'),
        _FileItem('test2', 'data/test2/5.json'),
        None
    )

    @patch('amzn_review.data.uploader.AWS.get_client')
    def test_basic_type(self, *_):
        # check group
        with self.assertRaises(ValueError):
            ReviewUploader('test', 0, 'local', 1, 1)

        # check interval
        with self.assertRaises(ValueError):
            ReviewUploader('test', 2, 'aws', -1, 1)

        # check invalid mode
        with self.assertRaises(ValueError):
            ReviewUploader('test', 2, 'wrong', 1, 1)

    @patch('builtins.open')
    async def test_sending_data_to_local_storage(self, mock_open, load_json):
        uploader = ReviewUploader('test', 2, mode='local', interval=0, max_send=0)

        for i, f in enumerate(self.targets):
            await uploader.put(f)

        counter = _CollectData()
        mock_open.return_value.__enter__.return_value.write.side_effect = counter.collect

        await uploader.run()

        # check if bodies are taken from appropriate files
        target_items = tuple(t.filepath for t in self.targets if t is not None)

        self.assertEqual(counter.count(), len(target_items))
        self.assertTrue(
            counter.is_match(target_items)
        )

    @patch('amzn_review.data.uploader.AWS.get_client')
    async def test_sending_data_to_s3(self, boto3client, load_json):
        """check data is sent via boto3 client."""
        uploader = ReviewUploader('test', 2, mode='aws', interval=0, max_send=0)

        for i, f in enumerate(self.targets):
            await uploader.put(f)

        counter = _CollectData(key='Body')
        boto3client.return_value.put_object.side_effect = counter.collect

        await uploader.run()

        # check if bodies are taken from appropriate files
        target_items = tuple(t.filepath for t in self.targets if t is not None)

        self.assertEqual(counter.count(), len(target_items))
        self.assertTrue(
            counter.is_match(target_items)
        )


if __name__ == '__main__':
    unittest.main()
