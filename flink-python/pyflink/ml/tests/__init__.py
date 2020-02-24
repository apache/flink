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

import glob
import os
import unittest

from pyflink.testing.test_case_utils import PyFlinkTestCase
from pyflink.find_flink_home import _find_flink_source_root


class MLTestCase(PyFlinkTestCase):
    """
    Base class for testing ML.
    """

    _inited = False

    @staticmethod
    def _ensure_path(pattern):
        if not glob.glob(pattern):
            raise unittest.SkipTest(
                "'%s' is not available. Will skip the related tests." % pattern)

    @classmethod
    def _ensure_initialized(cls):
        if MLTestCase._inited:
            return

        FLINK_SOURCE_ROOT_DIR = _find_flink_source_root()
        api_path_pattern = (
            "flink-ml-parent/flink-ml-api/target/flink-ml-api*-SNAPSHOT.jar")
        lib_path_pattern = (
            "flink-ml-parent/flink-ml-lib/target/flink-ml-lib*-SNAPSHOT.jar")

        MLTestCase._ensure_path(os.path.join(FLINK_SOURCE_ROOT_DIR, api_path_pattern))
        MLTestCase._ensure_path(os.path.join(FLINK_SOURCE_ROOT_DIR, lib_path_pattern))

        MLTestCase._inited = True

    def setUp(self):
        super(MLTestCase, self).setUp()
        MLTestCase._ensure_initialized()
