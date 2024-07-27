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

import glob
import io
import os
import platform
import subprocess
import sys
from shutil import copytree, copy, rmtree

from setuptools import setup
from xml.etree import ElementTree as ET


def remove_if_exists(file_path):
    if os.path.exists(file_path):
        if os.path.islink(file_path) or os.path.isfile(file_path):
            os.remove(file_path)
        else:
            assert os.path.isdir(file_path)
            rmtree(file_path)


def find_file_path(pattern):
    files = glob.glob(pattern)
    if len(files) < 1:
        print("Failed to find the file %s." % pattern)
        exit(-1)
    if len(files) > 1:
        print("The file pattern %s is ambiguous: %s" % (pattern, files))
        exit(-1)
    return files[0]


in_flink_source = os.path.isfile("../../flink-java/src/main/java/org/apache/flink/api/java/"
                                 "ExecutionEnvironment.java")
this_directory = os.path.abspath(os.path.dirname(__file__))
pyflink_directory = os.path.join(this_directory, "pyflink")
if in_flink_source:
    remove_if_exists(pyflink_directory)
    os.mkdir(pyflink_directory)
    version_file = os.path.join(this_directory, '../pyflink/version.py')
else:
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
LICENSES_TEMP_PATH = os.path.join(TEMP_PATH, "licenses")
PLUGINS_TEMP_PATH = os.path.join(TEMP_PATH, "plugins")
SCRIPTS_TEMP_PATH = os.path.join(TEMP_PATH, "bin")

LICENSE_FILE_TEMP_PATH = os.path.join(this_directory, "LICENSE")
NOTICE_FILE_TEMP_PATH = os.path.join(this_directory, "NOTICE")
README_FILE_TEMP_PATH = os.path.join("pyflink", "README.txt")
VERSION_FILE_TEMP_PATH = os.path.join("pyflink", "version.py")

# Due to changes in FLINK-14008, the licenses directory and NOTICE file may not exist in
# build-target folder. Just ignore them in this case.
exist_licenses = None
try:
    if in_flink_source:

        try:
            os.mkdir(TEMP_PATH)
        except:
            print("Temp path for symlink to parent already exists {0}".format(TEMP_PATH),
                  file=sys.stderr)
            sys.exit(-1)
        flink_version = ET.parse("../../pom.xml").getroot().find(
            'POM:version',
            namespaces={
                'POM': 'http://maven.apache.org/POM/4.0.0'
            }).text
        if not flink_version:
            print("Not able to get flink version", file=sys.stderr)
            sys.exit(-1)
        print("Detected flink version: {0}".format(flink_version))
        FLINK_HOME = os.path.abspath(
            "../../flink-dist/target/flink-%s-bin/flink-%s" % (flink_version, flink_version))

        incorrect_invocation_message = """
If you are installing pyflink from flink source, you must first build Flink and
run sdist.

    To build Flink with maven you can run:
      mvn -DskipTests clean package
    Building the source dist is done in the flink-python directory:
      cd flink-python
      cd apache-flink-libraries
      python setup.py sdist
      pip install dist/*.tar.gz"""

        LIB_PATH = os.path.join(FLINK_HOME, "lib")
        OPT_PATH = os.path.join(FLINK_HOME, "opt")
        OPT_PYTHON_JAR_NAME = os.path.basename(
            find_file_path(os.path.join(OPT_PATH, "flink-python*.jar")))
        OPT_SQL_CLIENT_JAR_NAME = os.path.basename(
            find_file_path(os.path.join(OPT_PATH, "flink-sql-client*.jar")))
        LICENSES_PATH = os.path.join(FLINK_HOME, "licenses")
        PLUGINS_PATH = os.path.join(FLINK_HOME, "plugins")
        SCRIPTS_PATH = os.path.join(FLINK_HOME, "bin")

        LICENSE_FILE_PATH = os.path.join(FLINK_HOME, "LICENSE")
        README_FILE_PATH = os.path.join(FLINK_HOME, "README.txt")
        VERSION_FILE_PATH = os.path.join(this_directory, "../pyflink/version.py")
        exist_licenses = os.path.exists(LICENSES_PATH)

        if not os.path.isdir(LIB_PATH):
            print(incorrect_invocation_message, file=sys.stderr)
            sys.exit(-1)

        try:
            os.symlink(LIB_PATH, LIB_TEMP_PATH)
            support_symlinks = True
        except BaseException:  # pylint: disable=broad-except
            support_symlinks = False

        os.mkdir(OPT_TEMP_PATH)
        if support_symlinks:
            os.symlink(os.path.join(OPT_PATH, OPT_PYTHON_JAR_NAME),
                       os.path.join(OPT_TEMP_PATH, OPT_PYTHON_JAR_NAME))
            os.symlink(os.path.join(OPT_PATH, OPT_SQL_CLIENT_JAR_NAME),
                       os.path.join(OPT_TEMP_PATH, OPT_SQL_CLIENT_JAR_NAME))
            os.symlink(PLUGINS_PATH, PLUGINS_TEMP_PATH)
            os.symlink(LICENSE_FILE_PATH, LICENSE_FILE_TEMP_PATH)
            os.symlink(README_FILE_PATH, README_FILE_TEMP_PATH)
            os.symlink(VERSION_FILE_PATH, VERSION_FILE_TEMP_PATH)
        else:
            copytree(LIB_PATH, LIB_TEMP_PATH)
            copy(os.path.join(OPT_PATH, OPT_PYTHON_JAR_NAME),
                 os.path.join(OPT_TEMP_PATH, OPT_PYTHON_JAR_NAME))
            copy(os.path.join(OPT_PATH, OPT_SQL_CLIENT_JAR_NAME),
                 os.path.join(OPT_TEMP_PATH, OPT_SQL_CLIENT_JAR_NAME))
            copytree(PLUGINS_PATH, PLUGINS_TEMP_PATH)
            copy(LICENSE_FILE_PATH, LICENSE_FILE_TEMP_PATH)
            copy(README_FILE_PATH, README_FILE_TEMP_PATH)
            copy(VERSION_FILE_PATH, VERSION_FILE_TEMP_PATH)

        os.mkdir(SCRIPTS_TEMP_PATH)
        bin_jars = glob.glob(os.path.join(SCRIPTS_PATH, "*.jar"))
        for bin_jar in bin_jars:
            copy(bin_jar, os.path.join(SCRIPTS_TEMP_PATH, os.path.basename(bin_jar)))

        if exist_licenses and platform.system() != "Windows":
            # regenerate the licenses directory and NOTICE file as we only copy part of the
            # flink binary distribution.
            collect_licenses_file_sh = os.path.abspath(os.path.join(
                this_directory, "..", "..", "tools", "releasing", "collect_license_files.sh"))
            subprocess.check_output([collect_licenses_file_sh, TEMP_PATH, TEMP_PATH])
            # move the NOTICE file to the root of the package
            GENERATED_NOTICE_FILE_PATH = os.path.join(TEMP_PATH, "NOTICE")
            os.rename(GENERATED_NOTICE_FILE_PATH, NOTICE_FILE_TEMP_PATH)
    else:
        if not os.path.isdir(LIB_TEMP_PATH) or not os.path.isdir(OPT_TEMP_PATH):
            print("The flink core files are not found. Please make sure your installation package "
                  "is complete, or do this in the flink-python directory of the flink source "
                  "directory.")
            sys.exit(-1)
        exist_licenses = os.path.exists(LICENSES_TEMP_PATH)

    PACKAGES = ['pyflink',
                'pyflink.bin',
                'pyflink.lib',
                'pyflink.opt',
                'pyflink.plugins']

    PACKAGE_DIR = {
        'pyflink.bin': TEMP_PATH + '/bin',
        'pyflink.lib': TEMP_PATH + '/lib',
        'pyflink.opt': TEMP_PATH + '/opt',
        'pyflink.plugins': TEMP_PATH + '/plugins'}

    PACKAGE_DATA = {
        'pyflink': ['README.txt', 'version.py'],
        'pyflink.bin': ['*.jar'],
        'pyflink.lib': ['*.jar'],
        'pyflink.opt': ['*.*', '*/*'],
        'pyflink.plugins': ['*', '*/*']}

    if exist_licenses and platform.system() != "Windows":
        PACKAGES.append('pyflink.licenses')
        PACKAGE_DIR['pyflink.licenses'] = TEMP_PATH + '/licenses'
        PACKAGE_DATA['pyflink.licenses'] = ['*']

    setup(
        name='apache-flink-libraries',
        version=VERSION,
        packages=PACKAGES,
        include_package_data=True,
        package_dir=PACKAGE_DIR,
        package_data=PACKAGE_DATA,
        url='https://flink.apache.org',
        license='https://www.apache.org/licenses/LICENSE-2.0',
        author='Apache Software Foundation',
        author_email='dev@flink.apache.org',
        python_requires='>=3.8',
        description='Apache Flink Libraries',
        long_description=long_description,
        long_description_content_type='text/markdown',
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
            'Programming Language :: Python :: 3.10',
            'Programming Language :: Python :: 3.11'],
    )
finally:
    if in_flink_source:
        remove_if_exists(TEMP_PATH)
        remove_if_exists(LICENSE_FILE_TEMP_PATH)
        remove_if_exists(NOTICE_FILE_TEMP_PATH)
        remove_if_exists(README_FILE_TEMP_PATH)
        remove_if_exists(VERSION_FILE_TEMP_PATH)
        remove_if_exists(pyflink_directory)
