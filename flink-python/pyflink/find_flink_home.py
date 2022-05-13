#!/usr/bin/env python
#################################################################################
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
import glob
import logging
import os
import sys


def _is_flink_home(path):
    flink_script_file = path + "/bin/flink"
    if len(glob.glob(flink_script_file)) > 0:
        return True
    else:
        return False


def _is_apache_flink_libraries_home(path):
    flink_dist_jar_file = path + "/lib/flink-dist*.jar"
    if len(glob.glob(flink_dist_jar_file)) > 0:
        return True
    else:
        return False


def _find_flink_home():
    """
    Find the FLINK_HOME.
    """
    # If the environment has set FLINK_HOME, trust it.
    if 'FLINK_HOME' in os.environ:
        return os.environ['FLINK_HOME']
    else:
        try:
            current_dir = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
            flink_root_dir = os.path.abspath(current_dir + "/../../")
            build_target = glob.glob(flink_root_dir + "/flink-dist/target/flink-*-bin/flink-*")
            if len(build_target) > 0 and _is_flink_home(build_target[0]):
                os.environ['FLINK_HOME'] = build_target[0]
                return build_target[0]

            FLINK_HOME = None
            for module_home in __import__('pyflink').__path__:
                if _is_apache_flink_libraries_home(module_home):
                    os.environ['FLINK_LIB_DIR'] = os.path.join(module_home, 'lib')
                    os.environ['FLINK_PLUGINS_DIR'] = os.path.join(module_home, 'plugins')
                    os.environ['FLINK_OPT_DIR'] = os.path.join(module_home, 'opt')
                if _is_flink_home(module_home):
                    FLINK_HOME = module_home
            if FLINK_HOME is not None:
                os.environ['FLINK_HOME'] = FLINK_HOME
                return FLINK_HOME
        except Exception:
            pass
        logging.error("Could not find valid FLINK_HOME(Flink distribution directory) "
                      "in current environment.")
        sys.exit(-1)


def _find_flink_source_root():
    """
    Find the flink source root directory.
    """
    try:
        return os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + "/../../")
    except Exception:
        pass
    logging.error("Could not find valid flink source root directory in current environment.")
    sys.exit(-1)


if __name__ == "__main__":
    print(_find_flink_home())
