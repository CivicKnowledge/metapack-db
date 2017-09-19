#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

from setuptools import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as f:
    readme = f.read()

classifiers = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3.4',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

setup(
    name='metapack_db',
    version='0.0.1',
    description='CLI plugin for storing Metatab files in databases',
    long_description=readme,
    packages=['metapack_db', 'test'],
    package_data={'metapack_db.sql': ['*.sql']},
    zip_safe=False,
    install_requires=[
        'metapack',
        'sqlalchemy',
    ],

    entry_points={
        'console_scripts': [
        ],

        'appurl.urls' : [
        ],
        'rowgenerators': [
        ],
        'mt.subcommands': [
            'db=metapack_db.cli:metapackdb',
        ]

    },

    include_package_data=True,
    author='Eric Busboom',
    author_email='eric@civicknowledge.com',
    url='https://github.com/CivicKnowledge/metapack-db.git',
    license='MIT',
    classifiers=classifiers,
    extras_require={

    },

)
