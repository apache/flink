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
import unittest

from pyflink.table import EnvironmentSettings
from pyflink.testing.test_case_utils import PythonAPICompletenessTestCase


class EnvironmentSettingsCompletenessTests(PythonAPICompletenessTestCase, unittest.TestCase):
    """
    Tests whether the Python :class:`EnvironmentSettings` is consistent with
    Java `org.apache.flink.table.api.EnvironmentSettings`.
    """

    @classmethod
    def python_class(cls):
        return EnvironmentSettings

    @classmethod
    def java_class(cls):
        return "org.apache.flink.table.api.EnvironmentSettings"

    @classmethod
    def excluded_methods(cls):
        # internal interfaces, no need to expose to users.
        return {'toPlannerProperties', 'toExecutorProperties'}


class EnvironmentSettingsBuilderCompletenessTests(PythonAPICompletenessTestCase, unittest.TestCase):
    """
    Tests whether the Python :class:`EnvironmentSettings.Builder` is consistent with
    Java `org.apache.flink.table.api.EnvironmentSettings$Builder`.
    """

    @classmethod
    def python_class(cls):
        return EnvironmentSettings.Builder

    @classmethod
    def java_class(cls):
        return "org.apache.flink.table.api.EnvironmentSettings$Builder"


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
