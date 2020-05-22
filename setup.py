#!/usr/bin/env python

import os
from setuptools import setup

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='planchet',
    version='0.3.2',
    py_modules=['planchet.client', 'planchet.util'],
    zip_safe=True,
    include_package_data=False,
    description='Large Data Processing Assistant',
    author='Sasho Savkov',
    license='MIT',
    long_description=(
        open('README.md').read()
    ),
    long_description_content_type='text/markdown',
    install_requires=['requests==2.23.0'],
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Topic :: Internet :: WWW/HTTP'
    ]
)
