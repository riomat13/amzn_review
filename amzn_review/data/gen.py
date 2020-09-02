#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gzip
import json
import random
from typing import List

from faker import Faker

from settings import Config


class MetadataGenerator(object):
    """Generate random fake metadata.

    This data will contains following keys:
        {
          "asin": str,
          "title": str,
          "feature": List[str],
          "description": str,
          "price": float,
          "image": str,
          "related: List[str],
          "also_bought": List[str],
          "also_viewed": List[str],
          "bought_together": List[str],
          "buy_after_viewing": List[str],
          "salesRank": dict (key: str(category), value: int(rank)),
          "brand": str,
          "categories": List[str]
        }

    (note that `related` column leaves empty to simplify)
    """
    def __init__(self, categories: List[str]):
        self.__gen = Faker()
        self.categories = iter(categories)
        self.item_gen = self._item_gen()

    def _item_gen(self):
        for cat in self.categories:
            filepath = os.path.join(Config.DATA_DIR, 'asin', cat, '0.txt')
            with open(filepath, 'r') as f:
                for asin in f:
                    data = {
                        'asin': asin.strip(),
                        'title': self.__gen.sentence(nb_words=6),
                        'feature': self.__gen.sentences(random.randint(1, 3)),
                        'description': self.__gen.text(max_nb_chars=150),
                        'price': round(random.random() * 30 + 5, 2),
                        'image': self.__gen.file_path(extension='jpg'),
                        'related': [],
                        'also_bought': [],        # leave it empty
                        'also_viewed': [],        # leave it empty
                        'bought_together': [],    # leave it empty
                        'buy_after_viewing': [],  # leave it empty
                        'salesRank': {cat: random.randint(1, 100000)},
                        'brand': self.__gen.word(),
                        'categories': [cat],
                    }

                    yield json.dumps(data)

    def __call__(self):
        try:
            return next(self.item_gen)
        except StopIteration:
            return None


def write_metadata_files(dirpath: str, categories: List[str]):
    """Save generated metadata as gzipped json file."""
    count = 0

    gen = MetadataGenerator(categories)

    while (data := gen()) is not None:
        filename = os.path.join(dirpath, 'metadata', f'{count:08x}.json.gz')
        with gzip.open(filename, 'wb') as f:
            f.write(data.encode())
        count += 1

    print(f'Total files generated: {count}')
