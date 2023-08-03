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

from __future__ import absolute_import
from __future__ import print_function

import glob
import logging
import multiprocessing
import os
import platform
import shutil
import subprocess
import sys
import time
import warnings

import pkg_resources

GRPC_TOOLS = 'grpcio-tools>=1.29.0,<=1.48.2'
PROTO_PATHS = ['proto']
PYFLINK_ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
DEFAULT_PYTHON_OUTPUT_PATH = os.path.join(PYFLINK_ROOT_PATH, 'fn_execution')


def generate_proto_files(force=True, output_dir=DEFAULT_PYTHON_OUTPUT_PATH):
    try:
        import grpc_tools  # noqa  # pylint: disable=unused-import
    except ImportError:
        warnings.warn('Installing grpcio-tools is recommended for development.')

    proto_dirs = [os.path.join(PYFLINK_ROOT_PATH, path) for path in PROTO_PATHS]
    proto_files = sum(
        [glob.glob(os.path.join(d, '*.proto')) for d in proto_dirs], [])
    out_dir = os.path.join(PYFLINK_ROOT_PATH, output_dir)
    out_files = [path for path in glob.glob(os.path.join(out_dir, '*_pb2.py'))]

    if out_files and not proto_files and not force:
        # We have out_files but no protos; assume they're up to date.
        # This is actually the common case (e.g. installation from an sdist).
        logging.info('No proto files; using existing generated files.')
        return

    elif not out_files and not proto_files:
        raise RuntimeError(
            'No proto files found in %s.' % proto_dirs)

    # Regenerate iff the proto files or this file are newer.
    elif force or not out_files or len(out_files) < len(proto_files) or (
            min(os.path.getmtime(path) for path in out_files)
            <= max(os.path.getmtime(path)
                   for path in proto_files + [os.path.realpath(__file__)])):
        try:
            from grpc_tools import protoc
        except ImportError:
            if platform.system() == 'Windows':
                # For Windows, grpcio-tools has to be installed manually.
                raise RuntimeError(
                    'Cannot generate protos for Windows since grpcio-tools package is '
                    'not installed. Please install this package manually '
                    'using \'pip install "grpcio-tools>=1.29.0,<=1.48.2"\'.')

            # Use a subprocess to avoid messing with this process' path and imports.
            # Note that this requires a separate module from setup.py for Windows:
            # https://docs.python.org/2/library/multiprocessing.html#windows
            p = multiprocessing.Process(
                target=_install_grpcio_tools_and_generate_proto_files(force, output_dir))
            p.start()
            p.join()
            if p.exitcode:
                raise ValueError("Proto generation failed (see log for details).")
        else:
            _check_grpcio_tools_version()
            logging.info('Regenerating out-of-date Python proto definitions.')
            builtin_protos = pkg_resources.resource_filename('grpc_tools', '_proto')
            args = (
                [sys.executable] +  # expecting to be called from command line
                ['--proto_path=%s' % builtin_protos] +
                ['--proto_path=%s' % d for d in proto_dirs] +
                ['--python_out=%s' % out_dir] +
                proto_files)
            ret_code = protoc.main(args)
            if ret_code:
                raise RuntimeError(
                    'Protoc returned non-zero status (see logs for details): '
                    '%s' % ret_code)

            for output_file in os.listdir(output_dir):
                if output_file.endswith('_pb2.py'):
                    _add_license_header(output_dir, output_file)


# Though wheels are available for grpcio-tools, setup_requires uses
# easy_install which doesn't understand them. This means that it is
# compiled from scratch (which is expensive as it compiles the full
# protoc compiler). Instead, we attempt to install a wheel in a temporary
# directory and add it to the path as needed.
# See https://github.com/pypa/setuptools/issues/377
def _install_grpcio_tools_and_generate_proto_files(force, output_dir):
    install_path = os.path.join(PYFLINK_ROOT_PATH, '..', '.eggs', 'grpcio-wheels')
    build_path = install_path + '-build'
    if os.path.exists(build_path):
        shutil.rmtree(build_path)
    logging.warning('Installing grpcio-tools into %s', install_path)
    try:
        start = time.time()
        # since '--prefix' option is only supported for pip 8.0+, so here we fallback to
        # use '--install-option' when the pip version is lower than 8.0.0.
        pip_version = pkg_resources.get_distribution("pip").version
        from pkg_resources import parse_version
        if parse_version(pip_version) >= parse_version('8.0.0'):
            subprocess.check_call(
                [sys.executable, '-m', 'pip', 'install',
                 '--prefix', install_path, '--build', build_path,
                 '--upgrade', GRPC_TOOLS, "-I"])
        else:
            subprocess.check_call(
                [sys.executable, '-m', 'pip', 'install',
                 '--install-option', '--prefix=' + install_path, '--build', build_path,
                 '--upgrade', GRPC_TOOLS, "-I"])
        from distutils.dist import Distribution
        install_obj = Distribution().get_command_obj('install', create=True)
        install_obj.prefix = install_path
        install_obj.finalize_options()
        logging.warning(
            'Installing grpcio-tools took %0.2f seconds.', time.time() - start)
    finally:
        sys.stderr.flush()
        shutil.rmtree(build_path, ignore_errors=True)
    sys.path.append(install_obj.install_purelib)
    pkg_resources.working_set.add_entry(install_obj.install_purelib)
    if install_obj.install_purelib != install_obj.install_platlib:
        sys.path.append(install_obj.install_platlib)
        pkg_resources.working_set.add_entry(install_obj.install_platlib)
    try:
        generate_proto_files(force, output_dir)
    finally:
        sys.stderr.flush()


def _add_license_header(dir, file_name):
    with open(os.path.join(dir, file_name), 'r') as original_file:
        original_data = original_file.read()
        tmp_file_name = file_name + '.tmp'
        with open(os.path.join(dir, tmp_file_name), 'w') as tmp_file:
            tmp_file.write(
                '################################################################################\n'
                '#  Licensed to the Apache Software Foundation (ASF) under one\n'
                '#  or more contributor license agreements.  See the NOTICE file\n'
                '#  distributed with this work for additional information\n'
                '#  regarding copyright ownership.  The ASF licenses this file\n'
                '#  to you under the Apache License, Version 2.0 (the\n'
                '#  "License"); you may not use this file except in compliance\n'
                '#  with the License.  You may obtain a copy of the License at\n'
                '#\n'
                '#      http://www.apache.org/licenses/LICENSE-2.0\n'
                '#\n'
                '#  Unless required by applicable law or agreed to in writing, software\n'
                '#  distributed under the License is distributed on an "AS IS" BASIS,\n'
                '#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n'
                '#  See the License for the specific language governing permissions and\n'
                '# limitations under the License.\n'
                '################################################################################\n'
            )
            tmp_file.write(original_data)
    if os.path.exists(os.path.join(dir, file_name)):
        os.remove(os.path.join(dir, file_name))
    os.rename(os.path.join(dir, tmp_file_name), os.path.join(dir, file_name))


def _check_grpcio_tools_version():
    version = pkg_resources.get_distribution("grpcio-tools").parsed_version
    from pkg_resources import parse_version
    if version < parse_version('1.29.0') or version > parse_version('1.48.2'):
        raise RuntimeError(
            "Version of grpcio-tools must be between 1.29.0 and 1.48.2, got %s" % version)


if __name__ == '__main__':
    generate_proto_files()
