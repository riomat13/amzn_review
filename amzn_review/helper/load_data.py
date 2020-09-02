#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Preprocess original dataset."""

import os
import glob
import gzip
import json

import tqdm

from settings import Config, get_logger

logger = get_logger('uploader.load_data', level='DEBUG')


def load_asin(category):
    files = glob.glob(f'{Config.DATA_DIR}/{category}/*.json.gz')

    if not files:
        logger.warn('File not found')
        return

    asins = set()

    for f in tqdm.tqdm(files, ascii=True, desc='Loading files'):
        with gzip.open(f, 'rb') as g:
            data = json.loads(g.read())
        asins.add(data['asin'])

    if not os.path.isdir(dirpath := f'{Config.DATA_DIR}/asin/{category}'):
        os.makedirs(dirpath)

    with open(f'{Config.DATA_DIR}/asin/{category}/0.txt', 'a') as f:
        for line in tqdm.tqdm(asins, ascii=True, desc='Writing ASINs'):
            f.write(f'{line}\n')

    print('Done')


def load_from_compressed_file(filepath, category):
    if not os.path.isdir((dirpath := os.path.join(Config.DATA_DIR, category))):
        os.makedirs(dirpath)

    with gzip.open(filepath, 'rb') as g:
        for i, line in tqdm.tqdm(enumerate(g)):
            path = os.path.join(dirpath, f'{i:08x}.json.gz')
            with gzip.open(path, 'wb') as f:
                f.write(line)

    print('Done')
