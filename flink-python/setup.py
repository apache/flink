################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from __future__ import print_function

import io
import os
import sys
from setuptools import setup

if sys.version_info < (2, 7):
    print("Python versions prior to 2.7 are not supported for PyFlink.",
          file=sys.stderr)
    sys.exit(-1)

this_directory = os.path.abspath(os.path.dirname(__file__))
version_file = os.path.join(this_directory, 'pyflink/version.py')

try:
    exec(open(version_file).read())
except IOError:
    print("Failed to load PyFlink version file for packaging. " +
          "'%s' not found!" % version_file,
          file=sys.stderr)
    sys.exit(-1)
VERSION = __version__  # noqa

with io.open(os.path.join(this_directory, 'README.md'), 'r', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='pyflink',
    version=VERSION,
    packages=['pyflink',
              'pyflink.table',
              'pyflink.util'],
    url='http://flink.apache.org',
    license='http://www.apache.org/licenses/LICENSE-2.0',
    author='Flink Developers',
    author_email='dev@flink.apache.org',
    install_requires=['py4j==0.10.8.1'],
    tests_require=['pytest==4.4.1'],
    description='Apache Flink Python API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 1 - Planning',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7']
)
