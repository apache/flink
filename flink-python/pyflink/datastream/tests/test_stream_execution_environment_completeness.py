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

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.testing.test_case_utils import PythonAPICompletenessTestCase


class StreamExecutionEnvironmentCompletenessTests(PythonAPICompletenessTestCase,
                                                  unittest.TestCase):

    @classmethod
    def python_class(cls):
        return StreamExecutionEnvironment

    @classmethod
    def java_class(cls):
        return "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment"

    @classmethod
    def excluded_methods(cls):
        # Exclude these methods for the time being, because current
        # ExecutionEnvironment/StreamExecutionEnvironment do not apply to the
        # DataSet/DataStream API, but to the Table API configuration.
        # Currently only the methods for configuration is added.
        # 'isForceCheckpointing', 'getNumberOfExecutionRetries', 'setNumberOfExecutionRetries'
        # is deprecated, exclude them.
        return {'getLastJobExecutionResult', 'getId', 'getIdString',
                'registerCachedFile', 'createCollectionsEnvironment', 'createLocalEnvironment',
                'createRemoteEnvironment', 'addOperator', 'fromElements',
                'resetContextEnvironment', 'getCachedFiles', 'generateSequence',
                'getNumberOfExecutionRetries', 'getStreamGraph', 'fromParallelCollection',
                'readFileStream', 'isForceCheckpointing', 'readFile', 'clean',
                'createInput', 'createLocalEnvironmentWithWebUI', 'fromCollection',
                'socketTextStream', 'initializeContextEnvironment', 'readTextFile', 'addSource',
                'setNumberOfExecutionRetries', 'configure', 'executeAsync', 'registerJobListener',
                'clearJobListeners'}


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
