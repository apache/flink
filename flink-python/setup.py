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
from distutils.command.build_ext import build_ext
from shutil import copytree, copy, rmtree

from setuptools import setup, Extension

if sys.version_info < (3, 5):
    print("Python versions prior to 3.5 are not supported for PyFlink.",
          file=sys.stderr)
    sys.exit(-1)


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


# Currently Cython optimizing doesn't support Windows.
if platform.system() == 'Windows':
    extensions = ([])
else:
    try:
        from Cython.Build import cythonize
        extensions = cythonize([
            Extension(
                name="pyflink.fn_execution.coder_impl_fast",
                sources=["pyflink/fn_execution/coder_impl_fast.pyx"],
                include_dirs=["pyflink/fn_execution/"]),
            Extension(
                name="pyflink.fn_execution.stream",
                sources=["pyflink/fn_execution/stream.pyx"],
                include_dirs=["pyflink/fn_execution/"]),
            Extension(
                name="pyflink.fn_execution.beam.beam_stream",
                sources=["pyflink/fn_execution/beam/beam_stream.pyx"],
                include_dirs=["pyflink/fn_execution/beam"]),
            Extension(
                name="pyflink.fn_execution.beam.beam_coder_impl_fast",
                sources=["pyflink/fn_execution/beam/beam_coder_impl_fast.pyx"],
                include_dirs=["pyflink/fn_execution/beam"]),
            Extension(
                name="pyflink.fn_execution.beam.beam_operations_fast",
                sources=["pyflink/fn_execution/beam/beam_operations_fast.pyx"],
                include_dirs=["pyflink/fn_execution/beam"]),
        ])
    except ImportError:
        if os.path.exists("pyflink/fn_execution/coder_impl_fast.c"):
            extensions = ([
                Extension(
                    name="pyflink.fn_execution.coder_impl_fast",
                    sources=["pyflink/fn_execution/coder_impl_fast.c"],
                    include_dirs=["pyflink/fn_execution/"]),
                Extension(
                    name="pyflink.fn_execution.stream",
                    sources=["pyflink/fn_execution/stream.c"],
                    include_dirs=["pyflink/fn_execution/"]),
                Extension(
                    name="pyflink.fn_execution.beam.beam_stream",
                    sources=["pyflink/fn_execution/beam/beam_stream.c"],
                    include_dirs=["pyflink/fn_execution/beam"]),
                Extension(
                    name="pyflink.fn_execution.beam.beam_coder_impl_fast",
                    sources=["pyflink/fn_execution/beam/beam_coder_impl_fast.c"],
                    include_dirs=["pyflink/fn_execution/beam"]),
                Extension(
                    name="pyflink.fn_execution.beam.beam_operations_fast",
                    sources=["pyflink/fn_execution/beam/beam_operations_fast.c"],
                    include_dirs=["pyflink/fn_execution/beam"]),
            ])
        else:
            extensions = ([])


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

LICENSE_FILE_TEMP_PATH = os.path.join(this_directory, "LICENSE")
NOTICE_FILE_TEMP_PATH = os.path.join(this_directory, "NOTICE")
README_FILE_TEMP_PATH = os.path.join("pyflink", "README.txt")
PYFLINK_UDF_RUNNER_SH = "pyflink-udf-runner.sh"
PYFLINK_UDF_RUNNER_BAT = "pyflink-udf-runner.bat"

in_flink_source = os.path.isfile("../flink-java/src/main/java/org/apache/flink/api/java/"
                                 "ExecutionEnvironment.java")

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
        flink_version = VERSION.replace(".dev0", "-SNAPSHOT")
        FLINK_HOME = os.path.abspath(
            "../flink-dist/target/flink-%s-bin/flink-%s" % (flink_version, flink_version))

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
        OPT_PYTHON_JAR_NAME = os.path.basename(
            find_file_path(os.path.join(OPT_PATH, "flink-python_*.jar")))
        OPT_SQL_CLIENT_JAR_NAME = os.path.basename(
            find_file_path(os.path.join(OPT_PATH, "flink-sql-client_*.jar")))
        CONF_PATH = os.path.join(FLINK_HOME, "conf")
        EXAMPLES_PATH = os.path.join(FLINK_HOME, "examples")
        LICENSES_PATH = os.path.join(FLINK_HOME, "licenses")
        PLUGINS_PATH = os.path.join(FLINK_HOME, "plugins")
        SCRIPTS_PATH = os.path.join(FLINK_HOME, "bin")

        LICENSE_FILE_PATH = os.path.join(FLINK_HOME, "LICENSE")
        README_FILE_PATH = os.path.join(FLINK_HOME, "README.txt")

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
            os.symlink(CONF_PATH, CONF_TEMP_PATH)
            os.symlink(EXAMPLES_PATH, EXAMPLES_TEMP_PATH)
            os.symlink(PLUGINS_PATH, PLUGINS_TEMP_PATH)
            os.symlink(LICENSE_FILE_PATH, LICENSE_FILE_TEMP_PATH)
            os.symlink(README_FILE_PATH, README_FILE_TEMP_PATH)
        else:
            copytree(LIB_PATH, LIB_TEMP_PATH)
            copy(os.path.join(OPT_PATH, OPT_PYTHON_JAR_NAME),
                 os.path.join(OPT_TEMP_PATH, OPT_PYTHON_JAR_NAME))
            copy(os.path.join(OPT_PATH, OPT_SQL_CLIENT_JAR_NAME),
                 os.path.join(OPT_TEMP_PATH, OPT_SQL_CLIENT_JAR_NAME))
            copytree(CONF_PATH, CONF_TEMP_PATH)
            copytree(EXAMPLES_PATH, EXAMPLES_TEMP_PATH)
            copytree(PLUGINS_PATH, PLUGINS_TEMP_PATH)
            copy(LICENSE_FILE_PATH, LICENSE_FILE_TEMP_PATH)
            copy(README_FILE_PATH, README_FILE_TEMP_PATH)
        os.mkdir(LOG_TEMP_PATH)
        with open(os.path.join(LOG_TEMP_PATH, "empty.txt"), 'w') as f:
            f.write("This file is used to force setuptools to include the log directory. "
                    "You can delete it at any time after installation.")

        # copy the udf runner scripts
        copytree(SCRIPTS_PATH, SCRIPTS_TEMP_PATH)
        copy(os.path.join(this_directory, "bin", PYFLINK_UDF_RUNNER_SH),
             os.path.join(SCRIPTS_TEMP_PATH, PYFLINK_UDF_RUNNER_SH))
        copy(os.path.join(this_directory, "bin", PYFLINK_UDF_RUNNER_BAT),
             os.path.join(SCRIPTS_TEMP_PATH, PYFLINK_UDF_RUNNER_BAT))

        if exist_licenses and platform.system() != "Windows":
            # regenerate the licenses directory and NOTICE file as we only copy part of the
            # flink binary distribution.
            collect_licenses_file_sh = os.path.abspath(os.path.join(
                this_directory, "..", "tools", "releasing", "collect_license_files.sh"))
            subprocess.check_output([collect_licenses_file_sh, TEMP_PATH, TEMP_PATH])
            # move the NOTICE file to the root of the package
            GENERATED_NOTICE_FILE_PATH = os.path.join(TEMP_PATH, "NOTICE")
            os.rename(GENERATED_NOTICE_FILE_PATH, NOTICE_FILE_TEMP_PATH)
    else:
        if not os.path.isdir(LIB_TEMP_PATH) or not os.path.isdir(OPT_TEMP_PATH) \
                or not os.path.isdir(SCRIPTS_TEMP_PATH):
            print("The flink core files are not found. Please make sure your installation package "
                  "is complete, or do this in the flink-python directory of the flink source "
                  "directory.")
            sys.exit(-1)
        exist_licenses = os.path.exists(LICENSES_TEMP_PATH)

    script_names = ["pyflink-shell.sh", "find-flink-home.sh"]
    scripts = [os.path.join(SCRIPTS_TEMP_PATH, script) for script in script_names]
    scripts.append("pyflink/find_flink_home.py")

    PACKAGES = ['pyflink',
                'pyflink.table',
                'pyflink.util',
                'pyflink.datastream',
                'pyflink.dataset',
                'pyflink.common',
                'pyflink.fn_execution',
                'pyflink.fn_execution.beam',
                'pyflink.metrics',
                'pyflink.ml',
                'pyflink.ml.api',
                'pyflink.ml.api.param',
                'pyflink.ml.lib',
                'pyflink.ml.lib.param',
                'pyflink.lib',
                'pyflink.opt',
                'pyflink.conf',
                'pyflink.log',
                'pyflink.examples',
                'pyflink.plugins',
                'pyflink.bin']

    PACKAGE_DIR = {
        'pyflink.lib': TEMP_PATH + '/lib',
        'pyflink.opt': TEMP_PATH + '/opt',
        'pyflink.conf': TEMP_PATH + '/conf',
        'pyflink.log': TEMP_PATH + '/log',
        'pyflink.examples': TEMP_PATH + '/examples',
        'pyflink.plugins': TEMP_PATH + '/plugins',
        'pyflink.bin': TEMP_PATH + '/bin'}

    PACKAGE_DATA = {
        'pyflink': ['README.txt'],
        'pyflink.lib': ['*.jar'],
        'pyflink.opt': ['*.*', '*/*'],
        'pyflink.conf': ['*'],
        'pyflink.log': ['*'],
        'pyflink.examples': ['*.py', '*/*.py'],
        'pyflink.plugins': ['*', '*/*'],
        'pyflink.bin': ['*']}

    if exist_licenses and platform.system() != "Windows":
        PACKAGES.append('pyflink.licenses')
        PACKAGE_DIR['pyflink.licenses'] = TEMP_PATH + '/licenses'
        PACKAGE_DATA['pyflink.licenses'] = ['*']

    setup(
        name='apache-flink',
        version=VERSION,
        packages=PACKAGES,
        include_package_data=True,
        package_dir=PACKAGE_DIR,
        package_data=PACKAGE_DATA,
        scripts=scripts,
        url='https://flink.apache.org',
        license='https://www.apache.org/licenses/LICENSE-2.0',
        author='Apache Software Foundation',
        author_email='dev@flink.apache.org',
        python_requires='>=3.5',
        install_requires=['py4j==0.10.8.1', 'python-dateutil==2.8.0', 'apache-beam==2.19.0',
                          'cloudpickle==1.2.2', 'avro-python3>=1.8.1,<=1.9.1', 'jsonpickle==1.2',
                          'pandas>=0.23.4,<=0.25.3', 'pyarrow>=0.15.1,<0.16.0', 'pytz>=2018.3'],
        cmdclass={'build_ext': build_ext},
        tests_require=['pytest==4.4.1'],
        description='Apache Flink Python API',
        long_description=long_description,
        long_description_content_type='text/markdown',
        zip_safe=False,
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7'],
        ext_modules=extensions
    )
finally:
    if in_flink_source:
        remove_if_exists(TEMP_PATH)
        remove_if_exists(LICENSE_FILE_TEMP_PATH)
        remove_if_exists(NOTICE_FILE_TEMP_PATH)
        remove_if_exists(README_FILE_TEMP_PATH)
