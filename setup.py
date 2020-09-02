#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(
    name='amzn_review',
    version='0.0.1',
    description='Amazon review data process pipeline',
    packages=find_packages(exclude=['tests']),
    install_requires=[],
    setup_requires=['setuptools', 'wheel', 'pipenv'],
    entry_points={
        'console_scripts': [
            'amzn-review = amzn_review.__main__:main',
        ]
    },
    scripts=[
        'scripts/workflow-init',
        'scripts/workflow-init-local',
        'scripts/workflow-run',
        'scripts/workflow-stop'
    ],
    python_requires='>=3.8.5',
)
