#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

from datetime import datetime

from settings import Config, get_logger
from rating_model.data import ReviewDataset
from rating_model.train import Trainer
from workflow.helper import makedir, redis_session


logger = get_logger('workflow.dags.run_model')


def main():
    dttm = datetime.now().strftime('%Y%m%d_%H00')

    train = os.scandir(os.path.join(Config.DATA_DIR, 'tfrecord'))
    test = os.scandir(os.path.join(Config.DATA_DIR, 'test', 'tfrecord'))

    train = [f.path for f in train if f.is_file() and f.name.endswith('.tfrecord')]
    test = [f.path for f in test if f.is_file() and f.name.endswith('.tfrecord')]

    logger.info(f'Train files: {len(train)} Test files: {len(test)}')

    with redis_session() as r:
        data_size = int(r.get('loaded_review_files_count'))

    # batch_size must be smaller than data_size otherwise Dataset will skip to generate data
    batch_size = min(64, data_size)

    train_ds = ReviewDataset(train, cls='train', batch_size=batch_size, shuffle=True, repeat=True)
    test_ds = ReviewDataset(test, cls='test')

    trainer = Trainer(batch_size=batch_size)

    filepath = os.path.join(Config.LOG_DIR, 'models', f'run_model_{dttm}.log')
    makedir(os.path.dirname(filepath))

    iter_per_epoch = data_size // batch_size
    verbose_step = max(iter_per_epoch // 10, 5)

    # print output to log file
    stdout = sys.stdout

    with open(filepath, 'w') as f:
        sys.stdout = f

        trainer.train(train_ds,
                      epochs=5,
                      iter_per_epoch=iter_per_epoch,
                      val_data=test_ds,
                      verbose_step=verbose_step)

    sys.stdout = stdout
