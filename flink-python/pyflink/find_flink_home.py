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
import os
import sys


def _find_flink_home():
    """
    Find the FLINK_HOME.
    """
    # If the environment has set FLINK_HOME, trust it.
    if 'FLINK_HOME' in os.environ:
        return os.environ['FLINK_HOME']
    else:
        try:
            flink_root_dir = os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + "/../../")
            build_target = flink_root_dir + "/build-target"
            pyflink_file = build_target + "/bin/pyflink-gateway-server.sh"
            if os.path.isfile(pyflink_file):
                os.environ['FLINK_HOME'] = build_target
                return build_target
        except Exception:
            pass
        print("Could not find valid FLINK_HOME in current environment.", file=sys.stderr)
        sys.exit(-1)


if __name__ == "__main__":
    print(_find_flink_home())
