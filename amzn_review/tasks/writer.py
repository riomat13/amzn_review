#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
from typing import List, Tuple

import numpy as np
import tensorflow as tf

from settings import get_logger


logger = get_logger('amzn_review.tasks.writer')


def _bytes_feature(value):  # pragma: no cover
    """Convert a value to byte type which is compatible with tf.Example."""
    if isinstance(value, type(tf.constant(0))):
        value = value.numpy()
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))


def _float_feature(value):  # pragma: no cover
    """Convert a value to float type which is compatible with tf.Example."""
    return tf.train.Feature(float_list=tf.train.FloatList(value=[value]))


class TFRecordReviewDataWriter(object):
    """Write TFRecord file from review dataset.

    Example usage:
        >>> writer = TFRecordReviewDataWriter(dirpath='/path/to/dir')
        >>> writer([(4.0, 'first text'), (3.0, 'second text')])
    """

    def __init__(self, dirpath: str, batch_size: int = 128):
        """
        Args:
            dirpath: str
                path to directory which stores tfrecord files
            batch_size: int (default: 1024)
                number of data to contain each file
        """
        if not os.path.isdir(dirpath):
            logger.info(f'Directory "{dirpath}" not found. Create the directory')
            os.makedirs(dirpath)

        self.dirpath = dirpath
        self.batch_size = batch_size
        self.count = 0
        self.filename = os.path.join(self.dirpath, '{:08x}.tfrecord')

    def _encode_token_array(self, arr):
        if isinstance(arr, bytes):
            return arr

        if isinstance(arr, type(tf.constant(0))):
            arr = arr.numpy()

        arr = np.array(arr, dtype=np.int32).tobytes()

        return arr

    def _create_serialized_message(self, token: dict, rating: float):  # pragma: no cover
        """Serialize message with tokenized sequence and rating."""
        token = {k: self._encode_token_array(v) for k, v in token.items()}

        feature = {
            'input_ids': _bytes_feature(token['input_ids']),
            'token_type_ids': _bytes_feature(token['token_type_ids']),
            'attention_mask': _bytes_feature(token['attention_mask']),
            'rating': _float_feature(rating)
        }

        example_proto = tf.train.Example(features=tf.train.Features(feature=feature))
        return example_proto.SerializeToString()

    def _decode_serialized_message(self, serialized_message):  # pragma: no cover
        return tf.train.Example.FromString(serialized_message)

    def _serialize_example(self, token, rating):  # pragma: no cover
        tf_string = tf.py_function(
            self._create_seralized_message,
            (token, rating),
            tf.string)

        # the result becomes scaler of string
        return tf.reshape(tf_string, ())

    def __call__(self, iterable: List[Tuple[dict, float]]):
        iterable = iter(iterable)

        try:
            token, rating = next(iterable)
        except StopIteration:
            raise ValueError('Provided empty item')

        while True:
            filename = self.filename.format(self.count)

            try:
                with tf.io.TFRecordWriter(filename) as writer:
                    for _ in range(self.batch_size):
                        try:
                            serialized = self._create_serialized_message(token, rating)
                            writer.write(serialized)
                        except Exception as e:
                            logger.error(f'Failed to write: (token={token}, rating={rating})')
                            logger.error(f'Error: {e}')

                        token, rating  = next(iterable)
            except StopIteration:
                break
            else:
                self.count += 1
