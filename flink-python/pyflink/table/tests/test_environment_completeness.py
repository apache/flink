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

from pyflink.testing.test_case_utils import PythonAPICompletenessTestCase, PyFlinkTestCase
from pyflink.table import TableEnvironment


class EnvironmentAPICompletenessTests(PythonAPICompletenessTestCase, PyFlinkTestCase):
    """
    Tests whether the Python :class:`TableEnvironment` is consistent with
    Java `org.apache.flink.table.api.TableEnvironment`.
    """
    @classmethod
    def python_class(cls):
        return TableEnvironment

    @classmethod
    def java_class(cls):
        return "org.apache.flink.table.api.TableEnvironment"

    @classmethod
    def excluded_methods(cls):
        # getCompletionHints has been deprecated. It will be removed in the next release.
        # TODO add TableEnvironment#create method with EnvironmentSettings as a parameter
        return {
            'getCompletionHints',
            'fromValues',
            'create'}

    @classmethod
    def java_method_name(cls, python_method_name):
        """
        Due to 'from' is python keyword, so we use 'from_path'
        in Python API corresponding 'from' in Java API.

        :param python_method_name:
        :return:
        """
        py_func_to_java_method_dict = {'from_path': 'from',
                                       "from_descriptor": "from",
                                       "create_java_function": "create_function"}
        return py_func_to_java_method_dict.get(python_method_name, python_method_name)


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
