#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
from unittest.mock import patch, mock_open

import json

from amzn_review.data.gen import MetadataGenerator, write_metadata_files


class MetadataGeneratorTest(unittest.TestCase):

    @patch('amzn_review.data.gen.os.path.join')
    @patch('builtins.open', mock_open(read_data='1\n2'))
    def test_data_generator(self, *_):

        target_columns = {
          "asin", "title", "feature",
          "description", "price", "image",
          "related", "salesRank",
          "brand", "categories"
        }

        generator = MetadataGenerator(['test1', 'test2'])

        while (data := generator()) is not None:
            data = json.loads(data)

            self.assertTrue(all(col in data for col in target_columns))


class MetadataWriterTest(unittest.TestCase):

    @patch('amzn_review.data.gen.gzip.open')
    @patch('amzn_review.data.gen.MetadataGenerator._item_gen')
    def test_write_metadata_to_file(self, mock_gen, mock_open):
        def item_gen():
            for _ in range(10):
                data = {
                    'asin': 'test',
                    'title': 'test title',
                    'feature': ['test feature'],
                    'description': 'test description',
                    'price': 10.0,
                    'image': '',
                    'related': [],
                    'also_bought': [],
                    'also_viewed': [],
                    'bought_together': [],
                    'buy_after_viewing': [],
                    'salesRank': {'test': 1000},
                    'brand': 'test brand',
                    'categories': ['test'],
                }
                yield json.dumps(data)

        mock_gen.side_effect = item_gen
        mock_writer = mock_open.return_value.__enter__.return_value.write

        write_metadata_files('', [])

        self.assertEqual(mock_writer.call_count, 10)

        for call_args  in mock_writer.call_args_list:
            self.assertTrue(isinstance(call_args.args[0], bytes))


if __name__ == '__main__':
    unittest.main()
