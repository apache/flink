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
import platform
import re
import sys
from distutils.command.build_ext import build_ext
from shutil import copytree, copy, rmtree

from setuptools import setup, Extension
from xml.etree import ElementTree as ET

if sys.version_info < (3, 8):
    print("Python versions prior to 3.8 are not supported for PyFlink.",
          file=sys.stderr)
    sys.exit(-1)


def remove_if_exists(file_path):
    if os.path.exists(file_path):
        if os.path.islink(file_path) or os.path.isfile(file_path):
            os.remove(file_path)
        else:
            assert os.path.isdir(file_path)
            rmtree(file_path)


def copy_files(src_paths, output_directory):
    for src_path, file_mode in src_paths:
        if os.path.isdir(src_path):
            child_files = os.listdir(src_path)
            for child_file in child_files:
                dst_path = copy(os.path.join(src_path, child_file), output_directory)
                os.chmod(dst_path, file_mode)
        else:
            dst_path = copy(src_path, os.path.join(output_directory, os.path.basename(src_path)))
            os.chmod(dst_path, file_mode)


def has_unsupported_tag(file_element):
    unsupported_tags = ['includes', 'exclueds']
    for unsupported_tag in unsupported_tags:
        if file_element.getElementsByTagName(unsupported_tag):
            print('Unsupported <{0}></{1}> tag'.format(unsupported_tag, unsupported_tag))
            return True
    return False


def extracted_output_files(base_dir, file_path, output_directory):
    extracted_file_paths = []
    from xml.dom.minidom import parse
    dom = parse(file_path)
    root_data = dom.documentElement
    file_elements = (root_data.getElementsByTagName("files")[0]).getElementsByTagName("file")
    # extracted <files><file></file></files>
    for file_element in file_elements:
        source = ((file_element.getElementsByTagName('source')[0]).childNodes[0]).data
        file_mode = int(((file_element.getElementsByTagName('fileMode')[0]).childNodes[0]).data, 8)
        try:
            dst = ((file_element.getElementsByTagName('outputDirectory')[0]).childNodes[0]).data
            if dst == output_directory:
                if has_unsupported_tag(file_element):
                    sys.exit(-1)
                extracted_file_paths.append((os.path.join(base_dir, source), file_mode))
        except IndexError:
            pass
    # extracted <fileSets><fileSet></fileSet></fileSets>
    file_elements = (root_data.getElementsByTagName("fileSets")[0]).getElementsByTagName("fileSet")
    for file_element in file_elements:
        source = ((file_element.getElementsByTagName('directory')[0]).childNodes[0]).data
        file_mode = int(((file_element.getElementsByTagName('fileMode')[0]).childNodes[0]).data, 8)
        try:
            dst = ((file_element.getElementsByTagName('outputDirectory')[0]).childNodes[0]).data
            if dst == output_directory:
                if has_unsupported_tag(file_element):
                    sys.exit(-1)
                extracted_file_paths.append((os.path.join(base_dir, source), file_mode))
        except IndexError:
            pass
    return extracted_file_paths


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
                name="pyflink.fn_execution.table.aggregate_fast",
                sources=["pyflink/fn_execution/table/aggregate_fast.pyx"],
                include_dirs=["pyflink/fn_execution/table/"]),
            Extension(
                name="pyflink.fn_execution.table.window_aggregate_fast",
                sources=["pyflink/fn_execution/table/window_aggregate_fast.pyx"],
                include_dirs=["pyflink/fn_execution/table/"]),
            Extension(
                name="pyflink.fn_execution.stream_fast",
                sources=["pyflink/fn_execution/stream_fast.pyx"],
                include_dirs=["pyflink/fn_execution/"]),
            Extension(
                name="pyflink.fn_execution.beam.beam_stream_fast",
                sources=["pyflink/fn_execution/beam/beam_stream_fast.pyx"],
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
                    name="pyflink.fn_execution.table.aggregate_fast",
                    sources=["pyflink/fn_execution/table/aggregate_fast.c"],
                    include_dirs=["pyflink/fn_execution/table/"]),
                Extension(
                    name="pyflink.fn_execution.table.window_aggregate_fast",
                    sources=["pyflink/fn_execution/table/window_aggregate_fast.c"],
                    include_dirs=["pyflink/fn_execution/table/"]),
                Extension(
                    name="pyflink.fn_execution.stream_fast",
                    sources=["pyflink/fn_execution/stream_fast.c"],
                    include_dirs=["pyflink/fn_execution/"]),
                Extension(
                    name="pyflink.fn_execution.beam.beam_stream_fast",
                    sources=["pyflink/fn_execution/beam/beam_stream_fast.c"],
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

CONF_TEMP_PATH = os.path.join(TEMP_PATH, "conf")
LOG_TEMP_PATH = os.path.join(TEMP_PATH, "log")
EXAMPLES_TEMP_PATH = os.path.join(TEMP_PATH, "examples")
SCRIPTS_TEMP_PATH = os.path.join(TEMP_PATH, "bin")

LICENSE_FILE_TEMP_PATH = os.path.join(this_directory, "LICENSE")
README_FILE_TEMP_PATH = os.path.join("pyflink", "README.txt")
PYFLINK_UDF_RUNNER_SH = "pyflink-udf-runner.sh"
PYFLINK_UDF_RUNNER_BAT = "pyflink-udf-runner.bat"

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
        namespace = "http://maven.apache.org/POM/4.0.0"
        flink_version = ET.parse("../pom.xml").getroot().find(
            'POM:version',
            namespaces={
                'POM': 'http://maven.apache.org/POM/4.0.0'
            }).text
        if not flink_version:
            print("Not able to get flink version", file=sys.stderr)
            sys.exit(-1)
        print("Detected flink version: {0}".format(flink_version))
        FLINK_HOME = os.path.abspath(
            "../flink-dist/target/flink-%s-bin/flink-%s" % (flink_version, flink_version))
        FLINK_ROOT = os.path.abspath("..")
        FLINK_DIST = os.path.join(FLINK_ROOT, "flink-dist")
        FLINK_BIN = os.path.join(FLINK_DIST, "src/main/flink-bin")

        EXAMPLES_PATH = os.path.join(this_directory, "pyflink/examples")

        LICENSE_FILE_PATH = os.path.join(FLINK_ROOT, "LICENSE")
        README_FILE_PATH = os.path.join(FLINK_BIN, "README.txt")

        FLINK_BIN_XML_FILE = os.path.join(FLINK_BIN, '../assemblies/bin.xml')
        # copy conf files
        os.mkdir(CONF_TEMP_PATH)
        conf_paths = extracted_output_files(FLINK_DIST, FLINK_BIN_XML_FILE, 'conf')
        copy_files(conf_paths, CONF_TEMP_PATH)

        # copy bin files
        os.mkdir(SCRIPTS_TEMP_PATH)
        script_paths = extracted_output_files(FLINK_DIST, FLINK_BIN_XML_FILE, 'bin')
        copy_files(script_paths, SCRIPTS_TEMP_PATH)
        copy(os.path.join(this_directory, "pyflink", "bin", PYFLINK_UDF_RUNNER_SH),
             os.path.join(SCRIPTS_TEMP_PATH, PYFLINK_UDF_RUNNER_SH))
        copy(os.path.join(this_directory, "pyflink", "bin", PYFLINK_UDF_RUNNER_BAT),
             os.path.join(SCRIPTS_TEMP_PATH, PYFLINK_UDF_RUNNER_BAT))

        try:
            os.symlink(EXAMPLES_PATH, EXAMPLES_TEMP_PATH)
            os.symlink(LICENSE_FILE_PATH, LICENSE_FILE_TEMP_PATH)
            os.symlink(README_FILE_PATH, README_FILE_TEMP_PATH)
        except BaseException:  # pylint: disable=broad-except
            copytree(EXAMPLES_PATH, EXAMPLES_TEMP_PATH)
            copy(LICENSE_FILE_PATH, LICENSE_FILE_TEMP_PATH)
            copy(README_FILE_PATH, README_FILE_TEMP_PATH)
        os.mkdir(LOG_TEMP_PATH)
        with open(os.path.join(LOG_TEMP_PATH, "empty.txt"), 'w') as f:
            f.write("This file is used to force setuptools to include the log directory. "
                    "You can delete it at any time after installation.")

    else:
        if not os.path.isdir(SCRIPTS_TEMP_PATH):
            print("The flink core files are not found. Please make sure your installation package "
                  "is complete, or do this in the flink-python directory of the flink source "
                  "directory.")
            sys.exit(-1)
    if re.search('dev.*$', VERSION) is not None:
        apache_flink_libraries_dependency = 'apache-flink-libraries==%s' % VERSION
    else:
        split_versions = VERSION.split('.')
        split_versions[-1] = str(int(split_versions[-1]) + 1)
        NEXT_VERSION = '.'.join(split_versions)
        apache_flink_libraries_dependency = 'apache-flink-libraries>=%s,<%s' % \
                                            (VERSION, NEXT_VERSION)

    script_names = ["pyflink-shell.sh", "find-flink-home.sh"]
    scripts = [os.path.join(SCRIPTS_TEMP_PATH, script) for script in script_names]
    scripts.append("pyflink/find_flink_home.py")

    PACKAGES = ['pyflink',
                'pyflink.table',
                'pyflink.util',
                'pyflink.datastream',
                'pyflink.datastream.connectors',
                'pyflink.datastream.formats',
                'pyflink.common',
                'pyflink.fn_execution',
                'pyflink.fn_execution.beam',
                'pyflink.fn_execution.datastream',
                'pyflink.fn_execution.datastream.embedded',
                'pyflink.fn_execution.datastream.process',
                'pyflink.fn_execution.datastream.window',
                'pyflink.fn_execution.embedded',
                'pyflink.fn_execution.formats',
                'pyflink.fn_execution.metrics',
                'pyflink.fn_execution.metrics.embedded',
                'pyflink.fn_execution.metrics.process',
                'pyflink.fn_execution.table',
                'pyflink.fn_execution.utils',
                'pyflink.metrics',
                'pyflink.conf',
                'pyflink.log',
                'pyflink.examples',
                'pyflink.bin',
                'pyflink.testing']

    PACKAGE_DIR = {
        'pyflink.conf': TEMP_PATH + '/conf',
        'pyflink.log': TEMP_PATH + '/log',
        'pyflink.examples': TEMP_PATH + '/examples',
        'pyflink.bin': TEMP_PATH + '/bin'}

    PACKAGE_DATA = {
        'pyflink': ['README.txt'],
        'pyflink.conf': ['*'],
        'pyflink.log': ['*'],
        'pyflink.examples': ['*.py', '*/*.py'],
        'pyflink.bin': ['*']}

    install_requires = ['py4j==0.10.9.7', 'python-dateutil>=2.8.0,<3',
                        'apache-beam>=2.43.0,<2.49.0',
                        'cloudpickle>=2.2.0', 'avro-python3>=1.8.1,!=1.9.2',
                        'pytz>=2018.3', 'fastavro>=1.1.0,!=1.8.0', 'requests>=2.26.0',
                        'protobuf>=3.19.0',
                        'numpy>=1.21.4',
                        'pandas>=1.3.0',
                        'pyarrow>=5.0.0',
                        'pemja==0.3.0;platform_system != "Windows"',
                        'httplib2>=0.19.0', apache_flink_libraries_dependency]

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
        python_requires='>=3.8',
        install_requires=install_requires,
        cmdclass={'build_ext': build_ext},
        description='Apache Flink Python API',
        long_description=long_description,
        long_description_content_type='text/markdown',
        zip_safe=False,
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
            'Programming Language :: Python :: 3.10'],
        ext_modules=extensions
    )
finally:
    if in_flink_source:
        remove_if_exists(TEMP_PATH)
        remove_if_exists(LICENSE_FILE_TEMP_PATH)
        remove_if_exists(README_FILE_TEMP_PATH)
