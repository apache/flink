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
import argparse
import json
import os

from pyflink.common.dependency_manager import DependencyManager
from pyflink.testing.test_case_utils import (TestEnv, TestConfiguration, replace_uuid,
                                             decode_from_base64, sort_dict_by_key)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected_parameter")
    parser.add_argument("--expected_files")
    parser.add_argument("--result_file")
    args = parser.parse_args()

    j_env = TestEnv()
    config = TestConfiguration()
    result_file = args.result_file
    dependency_manager = DependencyManager(config, j_env)
    dependency_manager.load_from_env(os.environ)

    expected_parameter = decode_from_base64(args.expected_parameter)
    expected_files = decode_from_base64(args.expected_files)
    actual_config = replace_uuid(config.to_dict())
    actual_files = replace_uuid(j_env.to_dict())

    expected_parameter[DependencyManager.PYTHON_FILES] = \
        json.loads(expected_parameter[DependencyManager.PYTHON_FILES])
    actual_config[DependencyManager.PYTHON_FILES] = \
        json.loads(actual_config[DependencyManager.PYTHON_FILES])

    expected_parameter[DependencyManager.PYTHON_ARCHIVES] = \
        json.loads(expected_parameter[DependencyManager.PYTHON_ARCHIVES])
    actual_config[DependencyManager.PYTHON_ARCHIVES] = \
        json.loads(actual_config[DependencyManager.PYTHON_ARCHIVES])

    with open(result_file, "w") as f:
        if expected_parameter == actual_config and expected_files == actual_files:
            f.write("Assertion passed.")
        elif expected_parameter != actual_config:
            f.write("expected config:\n%s\nactual config:\n%s\n" %
                    (sort_dict_by_key(expected_parameter), sort_dict_by_key(actual_config)))
        elif expected_files != actual_files:
            f.write("expected files:\n%s\nactual files:\n%s\n" %
                    (sort_dict_by_key(expected_files), sort_dict_by_key(actual_files)))
    # test is complete, encounter the ProgramAbortException
    exit(1)
