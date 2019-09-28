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
    pyflink_file = path + "/bin/pyflink-gateway-server.sh"
    flink_dist_jar_file = path + "/lib/flink-dist*.jar"
    if os.path.isfile(pyflink_file) and len(glob.glob(flink_dist_jar_file)) > 0:
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
            build_target = flink_root_dir + "/build-target"
            if _is_flink_home(build_target):
                os.environ['FLINK_HOME'] = build_target
                return build_target

            if sys.version < "3":
                import imp
                module_home = imp.find_module("pyflink")[1]
            else:
                from importlib.util import find_spec
                module_home = os.path.dirname(find_spec("pyflink").origin)

            if _is_flink_home(module_home):
                os.environ['FLINK_HOME'] = module_home
                return module_home
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
