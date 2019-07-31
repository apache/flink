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
from shutil import copytree, copy, rmtree

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

TEMP_PATH = "deps"

LIB_TEMP_PATH = os.path.join(TEMP_PATH, "lib")
OPT_TEMP_PATH = os.path.join(TEMP_PATH, "opt")
CONF_TEMP_PATH = os.path.join(TEMP_PATH, "conf")
LOG_TEMP_PATH = os.path.join(TEMP_PATH, "log")
EXAMPLES_TEMP_PATH = os.path.join(TEMP_PATH, "examples")
LICENSES_TEMP_PATH = os.path.join(TEMP_PATH, "licenses")
PLUGINS_TEMP_PATH = os.path.join(TEMP_PATH, "plugins")
SCRIPTS_TEMP_PATH = os.path.join(TEMP_PATH, "bin")

LICENSE_FILE_TEMP_PATH = os.path.join("pyflink", "LICENSE")
NOTICE_FILE_TEMP_PATH = os.path.join("pyflink", "NOTICE")
README_FILE_TEMP_PATH = os.path.join("pyflink", "README.txt")

in_flink_source = os.path.isfile("../flink-java/src/main/java/org/apache/flink/api/java/"
                                 "ExecutionEnvironment.java")

try:
    if in_flink_source:

        try:
            os.mkdir(TEMP_PATH)
        except:
            print("Temp path for symlink to parent already exists {0}".format(TEMP_PATH),
                  file=sys.stderr)
            sys.exit(-1)

        FLINK_HOME = os.path.abspath("../build-target")

        incorrect_invocation_message = """
If you are installing pyflink from flink source, you must first build Flink and
run sdist.

    To build Flink with maven you can run:
      mvn -DskipTests clean package
    Building the source dist is done in the flink-python directory:
      cd flink-python
      python setup.py sdist
      pip install dist/*.tar.gz"""

        LIB_PATH = os.path.join(FLINK_HOME, "lib")
        OPT_PATH = os.path.join(FLINK_HOME, "opt")
        CONF_PATH = os.path.join(FLINK_HOME, "conf")
        EXAMPLES_PATH = os.path.join(FLINK_HOME, "examples")
        LICENSES_PATH = os.path.join(FLINK_HOME, "licenses")
        PLUGINS_PATH = os.path.join(FLINK_HOME, "plugins")
        SCRIPTS_PATH = os.path.join(FLINK_HOME, "bin")

        LICENSE_FILE_PATH = os.path.join(FLINK_HOME, "LICENSE")
        NOTICE_FILE_PATH = os.path.join(FLINK_HOME, "NOTICE")
        README_FILE_PATH = os.path.join(FLINK_HOME, "README.txt")

        if not os.path.isdir(LIB_PATH):
            print(incorrect_invocation_message, file=sys.stderr)
            sys.exit(-1)

        if getattr(os, "symlink", None) is not None:
            os.symlink(LIB_PATH, LIB_TEMP_PATH)
            os.symlink(OPT_PATH, OPT_TEMP_PATH)
            os.symlink(CONF_PATH, CONF_TEMP_PATH)
            os.symlink(EXAMPLES_PATH, EXAMPLES_TEMP_PATH)
            os.symlink(LICENSES_PATH, LICENSES_TEMP_PATH)
            os.symlink(PLUGINS_PATH, PLUGINS_TEMP_PATH)
            os.symlink(SCRIPTS_PATH, SCRIPTS_TEMP_PATH)
            os.symlink(LICENSE_FILE_PATH, LICENSE_FILE_TEMP_PATH)
            os.symlink(NOTICE_FILE_PATH, NOTICE_FILE_TEMP_PATH)
            os.symlink(README_FILE_PATH, README_FILE_TEMP_PATH)
        else:
            copytree(LIB_PATH, LIB_TEMP_PATH)
            copytree(OPT_PATH, OPT_TEMP_PATH)
            copytree(CONF_PATH, CONF_TEMP_PATH)
            copytree(EXAMPLES_PATH, EXAMPLES_TEMP_PATH)
            copytree(LICENSES_PATH, LICENSES_TEMP_PATH)
            copytree(PLUGINS_PATH, PLUGINS_TEMP_PATH)
            copytree(SCRIPTS_PATH, SCRIPTS_TEMP_PATH)
            copy(LICENSE_FILE_PATH, LICENSE_FILE_TEMP_PATH)
            copy(NOTICE_FILE_PATH, NOTICE_FILE_TEMP_PATH)
            copy(README_FILE_PATH, README_FILE_TEMP_PATH)
        os.mkdir(LOG_TEMP_PATH)
        with open(os.path.join(LOG_TEMP_PATH, "empty.txt"), 'w') as f:
            f.write("This file is used to force setuptools to include the log directory. "
                    "You can delete it at any time after installation.")
    else:
        if not os.path.isdir(LIB_TEMP_PATH) or not os.path.isdir(OPT_TEMP_PATH) \
                or not os.path.isdir(SCRIPTS_TEMP_PATH):
            print("The flink core files are not found. Please make sure your installation package "
                  "is complete, or do this in the flink-python directory of the flink source "
                  "directory.")
            sys.exit(-1)

    script_names = ["pyflink-shell.sh", "find-flink-home.sh"]
    scripts = [os.path.join(SCRIPTS_TEMP_PATH, script) for script in script_names]
    scripts.append("pyflink/find_flink_home.py")

    setup(
        name='apache-flink',
        version=VERSION,
        packages=['pyflink',
                  'pyflink.table',
                  'pyflink.util',
                  'pyflink.datastream',
                  'pyflink.dataset',
                  'pyflink.common',
                  'pyflink.lib',
                  'pyflink.opt',
                  'pyflink.conf',
                  'pyflink.log',
                  'pyflink.examples',
                  'pyflink.licenses',
                  'pyflink.plugins',
                  'pyflink.bin'],
        include_package_data=True,
        package_dir={
            'pyflink.lib': TEMP_PATH + '/lib',
            'pyflink.opt': TEMP_PATH + '/opt',
            'pyflink.conf': TEMP_PATH + '/conf',
            'pyflink.log': TEMP_PATH + '/log',
            'pyflink.examples': TEMP_PATH + '/examples',
            'pyflink.licenses': TEMP_PATH + '/licenses',
            'pyflink.plugins': TEMP_PATH + '/plugins',
            'pyflink.bin': TEMP_PATH + '/bin'
        },
        package_data={
            'pyflink': ['LICENSE', 'NOTICE', 'README.txt'],
            'pyflink.lib': ['*.jar'],
            'pyflink.opt': ['*.*', '*/*'],
            'pyflink.conf': ['*'],
            'pyflink.log': ['*'],
            'pyflink.examples': ['*.py', '*/*.py'],
            'pyflink.licenses': ['*'],
            'pyflink.plugins': ['*', '*/*'],
            'pyflink.bin': ['*']
        },
        scripts=scripts,
        url='https://flink.apache.org',
        license='https://www.apache.org/licenses/LICENSE-2.0',
        author='Flink Developers',
        author_email='dev@flink.apache.org',
        install_requires=['py4j==0.10.8.1', 'python-dateutil'],
        tests_require=['pytest==4.4.1'],
        description='Apache Flink Python API',
        long_description=long_description,
        long_description_content_type='text/markdown',
        classifiers=[
            'Development Status :: 1 - Planning',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7']
    )
finally:
    if in_flink_source:
        if getattr(os, "symlink", None) is not None:
            os.remove(LIB_TEMP_PATH)
            os.remove(OPT_TEMP_PATH)
            os.remove(CONF_TEMP_PATH)
            os.remove(EXAMPLES_TEMP_PATH)
            os.remove(LICENSES_TEMP_PATH)
            os.remove(PLUGINS_TEMP_PATH)
            os.remove(SCRIPTS_TEMP_PATH)
            os.remove(LICENSE_FILE_TEMP_PATH)
            os.remove(NOTICE_FILE_TEMP_PATH)
            os.remove(README_FILE_TEMP_PATH)
        else:
            rmtree(LIB_TEMP_PATH)
            rmtree(OPT_TEMP_PATH)
            rmtree(CONF_TEMP_PATH)
            rmtree(EXAMPLES_TEMP_PATH)
            rmtree(LICENSES_TEMP_PATH)
            rmtree(PLUGINS_TEMP_PATH)
            rmtree(SCRIPTS_TEMP_PATH)
            os.remove(LICENSE_FILE_TEMP_PATH)
            os.remove(NOTICE_FILE_TEMP_PATH)
            os.remove(README_FILE_TEMP_PATH)
        rmtree(LOG_TEMP_PATH)
        os.rmdir(TEMP_PATH)
