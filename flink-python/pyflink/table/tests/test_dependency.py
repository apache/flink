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
import os
import shutil
import sys
import unittest
import uuid

from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table import expressions as expr
from pyflink.table.udf import udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import (PyFlinkTestCase)


class DependencyTests(object):

    def test_add_python_file(self):
        python_file_dir = os.path.join(self.tempdir, "python_file_dir_" + str(uuid.uuid4()))
        os.mkdir(python_file_dir)
        python_file_path = os.path.join(python_file_dir, "test_dependency_manage_lib.py")
        with open(python_file_path, 'w') as f:
            f.write("def add_two(a):\n    raise Exception('This function should not be called!')")
        self.t_env.add_python_file(python_file_path)

        python_file_dir_with_higher_priority = os.path.join(
            self.tempdir, "python_file_dir_" + str(uuid.uuid4()))
        os.mkdir(python_file_dir_with_higher_priority)
        python_file_path_higher_priority = os.path.join(python_file_dir_with_higher_priority,
                                                        "test_dependency_manage_lib.py")
        with open(python_file_path_higher_priority, 'w') as f:
            f.write("def add_two(a):\n    return a + 2")
        self.t_env.add_python_file(python_file_path_higher_priority)

        def plus_two(i):
            from test_dependency_manage_lib import add_two
            return add_two(i)

        self.t_env.create_temporary_system_function(
            "add_two", udf(plus_two, DataTypes.BIGINT(), DataTypes.BIGINT()))
        sink_table_ddl = """
        CREATE TABLE Results(a BIGINT, b BIGINT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select(expr.call("add_two", t.a), t.a).execute_insert("Results").wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[3, 1]", "+I[4, 2]", "+I[5, 3]"])

    def test_add_python_archive(self):
        tmp_dir = self.tempdir
        archive_dir_path = os.path.join(tmp_dir, "archive_" + str(uuid.uuid4()))
        os.mkdir(archive_dir_path)
        with open(os.path.join(archive_dir_path, "data.txt"), 'w') as f:
            f.write("2")
        archive_file_path = \
            shutil.make_archive(os.path.dirname(archive_dir_path), 'zip', archive_dir_path)
        self.t_env.add_python_archive(archive_file_path, "data")

        def add_from_file(i):
            with open("data/data.txt", 'r') as f:
                return i + int(f.read())

        self.t_env.create_temporary_system_function("add_from_file",
                                                    udf(add_from_file, DataTypes.BIGINT(),
                                                        DataTypes.BIGINT()))

        sink_table_ddl = """
        CREATE TABLE Results(a BIGINT, b BIGINT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)

        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select(expr.call('add_from_file', t.a), t.a).execute_insert("Results").wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[3, 1]", "+I[4, 2]", "+I[5, 3]"])


class EmbeddedThreadDependencyTests(DependencyTests, PyFlinkTestCase):
    def setUp(self):
        super(EmbeddedThreadDependencyTests, self).setUp()
        self.t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        self.t_env.get_config().set("parallelism.default", "2")
        self.t_env.get_config().set("python.fn-execution.bundle.size", "1")
        self.t_env.get_config().set("python.execution-mode", "thread")


class BatchDependencyTests(DependencyTests, PyFlinkTestCase):

    def setUp(self) -> None:
        super(BatchDependencyTests, self).setUp()
        self.t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
        self.t_env.get_config().set("parallelism.default", "2")
        self.t_env.get_config().set("python.fn-execution.bundle.size", "1")


class StreamDependencyTests(DependencyTests, PyFlinkTestCase):

    def setUp(self):
        super(StreamDependencyTests, self).setUp()
        origin_execution_mode = os.environ['_python_worker_execution_mode']
        os.environ['_python_worker_execution_mode'] = "loopback"
        try:
            self.t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
            self.t_env.get_config().set("parallelism.default", "2")
            self.t_env.get_config().set("python.fn-execution.bundle.size", "1")
        finally:
            if origin_execution_mode is not None:
                os.environ['_python_worker_execution_mode'] = origin_execution_mode

    def test_set_requirements_without_cached_directory(self):
        requirements_txt_path = os.path.join(self.tempdir, str(uuid.uuid4()))
        with open(requirements_txt_path, 'w') as f:
            f.write("cloudpickle==2.2.0")
        self.t_env.set_python_requirements(requirements_txt_path)

        def check_requirements(i):
            import cloudpickle  # noqa # pylint: disable=unused-import
            assert '_PYTHON_REQUIREMENTS_INSTALL_DIR' in os.environ
            return i

        self.t_env.create_temporary_system_function(
            "check_requirements",
            udf(check_requirements, DataTypes.BIGINT(), DataTypes.BIGINT()))
        sink_table_ddl = """
                CREATE TABLE Results(a BIGINT, b BIGINT) WITH ('connector'='test-sink')
                """
        self.t_env.execute_sql(sink_table_ddl)

        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select(expr.call('check_requirements', t.a), t.a).execute_insert("Results").wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[1, 1]", "+I[2, 2]", "+I[3, 3]"])

    def test_set_requirements_with_cached_directory(self):
        tmp_dir = self.tempdir
        requirements_txt_path = os.path.join(tmp_dir, "requirements_txt_" + str(uuid.uuid4()))
        with open(requirements_txt_path, 'w') as f:
            f.write("python-package1==0.0.0")

        requirements_dir_path = os.path.join(tmp_dir, "requirements_dir_" + str(uuid.uuid4()))
        os.mkdir(requirements_dir_path)
        package_file_name = "python_package1-0.0.0-py2.py3-none-any.whl"
        with open(os.path.join(requirements_dir_path, package_file_name), 'wb') as f:
            import base64
            # This base64 data is encoded from a python package file which includes a
            # "python_package1" module. The module contains a "plus(a, b)" function.
            # The base64 can be recomputed by following code:
            # base64.b64encode(open("python_package1-0.0.0-py2.py3-none-any.whl", "rb").read())
            # .decode("utf-8")
            f.write(base64.b64decode(
                "UEsDBBQAAAAIAOZ4XVtBFc0kIQAAACEAAAAbAAAAcHl0aG9uX3BhY2thZ2UxL19faW5pdF9fLnB5S0lN"
                "UyjIKS3WSNRRSNK04lIAgqLUktKiPIVEBW2FJC4AUEsDBBQAAAAIAKF6XVukEBq3VwAAAGsAAAAoAAAA"
                "cHl0aG9uX3BhY2thZ2UxLTAuMC4wLmRpc3QtaW5mby9NRVRBREFUQfNNLUlMSSxJ1A1LLSrOzM+zUjDS"
                "M+HyS8xNtVIoqCzJyM+LL0hMzk5MTzXkgisx0ANCrqDUwtLMotRi3QCwOisFO1tjPXMul8q8xNzMZCuF"
                "Ipg8xBwuAFBLAwQUAAAACAChel1bdH4ECGAAAABtAAAAJQAAAHB5dGhvbl9wYWNrYWdlMS0wLjAuMC5k"
                "aXN0LWluZm8vV0hFRUwLz0hNzdENSy0qzszPs1Iw1DPgck/NSy1KLMkvslIoTi0pLSjJz88pVtCwMNCz"
                "1DPQ5ArKzy/R9SzWDSgtSs3JTLJSKCkqTeUKSUy3UiioNNLNy89L1U3Mq4SJGCNEuABQSwMEFAAAAAgA"
                "oXpdW1EvA6oSAAAAEAAAAC0AAABweXRob25fcGFja2FnZTEtMC4wLjAuZGlzdC1pbmZvL3RvcF9sZXZl"
                "bC50eHQrqCzJyM+LL0hMzk5MTzXkAgBQSwMEFAAAAAgAoXpdW/BQKAgDAQAAnAEAACYAAABweXRob25f"
                "cGFja2FnZTEtMC4wLjAuZGlzdC1pbmZvL1JFQ09SRIXNvXaCMABA4d1nIRqUnzp0iDEewYKECmiXnFjA"
                "oAihBJE+fdvBoZPn7t+VgxJ1xST/vPBTpk8YK6pCMTaWg9YKPjWt11uSu8fvpCNiAJ591gPf7sMNWoF9"
                "R9fGfoXaKSeCU9/stdlsJP+LAI7/SotWgaLK64lHdmiJdujBq7hz7DxWQHHAaex/oMXMuMAdpRXxOG2u"
                "wrQTeA/wgmg6tJ/6yZqQtwfu+onQV1c9SnuHxtA2TyWuDC/ucNjCtlau0wDX+jrecfSLz5/iqpaszG5Z"
                "OVZ39Zgc/Jdz4LbbOXKtgxM1JTa2rMrOMsVyA8W7pBBsTrQJlKHp1tNHSPA2XGra6AdQSwECFAMUAAAA"
                "CADmeF1bQRXNJCEAAAAhAAAAGwAAAAAAAAAAAAAApIEAAAAAcHl0aG9uX3BhY2thZ2UxL19faW5pdF9f"
                "LnB5UEsBAhQDFAAAAAgAoXpdW6QQGrdXAAAAawAAACgAAAAAAAAAAAAAAKSBWgAAAHB5dGhvbl9wYWNr"
                "YWdlMS0wLjAuMC5kaXN0LWluZm8vTUVUQURBVEFQSwECFAMUAAAACAChel1bdH4ECGAAAABtAAAAJQAA"
                "AAAAAAAAAAAApIH3AAAAcHl0aG9uX3BhY2thZ2UxLTAuMC4wLmRpc3QtaW5mby9XSEVFTFBLAQIUAxQA"
                "AAAIAKF6XVtRLwOqEgAAABAAAAAtAAAAAAAAAAAAAACkgZoBAABweXRob25fcGFja2FnZTEtMC4wLjAu"
                "ZGlzdC1pbmZvL3RvcF9sZXZlbC50eHRQSwECFAMUAAAACAChel1b8FAoCAMBAACcAQAAJgAAAAAAAAAA"
                "AAAAtIH3AQAAcHl0aG9uX3BhY2thZ2UxLTAuMC4wLmRpc3QtaW5mby9SRUNPUkRQSwUGAAAAAAUABQCh"
                "AQAAPgMAAAAA"))
        self.t_env.set_python_requirements(requirements_txt_path, requirements_dir_path)

        def add_one(i):
            from python_package1 import plus
            return plus(i, 1)

        self.t_env.create_temporary_system_function(
            "add_one",
            udf(add_one, DataTypes.BIGINT(), DataTypes.BIGINT()))
        sink_table_ddl = """
        CREATE TABLE Results(a BIGINT, b BIGINT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select(expr.call('add_one', t.a), t.a).execute_insert("Results").wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[2, 1]", "+I[3, 2]", "+I[4, 3]"])

    def test_set_environment(self):
        python_exec_link_path = sys.executable
        self.t_env.get_config().set_python_executable(python_exec_link_path)

        def check_python_exec(i):
            import os
            assert os.environ["python"] == python_exec_link_path
            return i

        self.t_env.create_temporary_system_function(
            "check_python_exec",
            udf(check_python_exec, DataTypes.BIGINT(), DataTypes.BIGINT()))

        def check_pyflink_gateway_disabled(i):
            from pyflink.java_gateway import get_gateway
            get_gateway()
            return i

        self.t_env.create_temporary_system_function(
            "check_pyflink_gateway_disabled",
            udf(check_pyflink_gateway_disabled, DataTypes.BIGINT(),
                DataTypes.BIGINT()))

        sink_table_ddl = """
        CREATE TABLE Results(a BIGINT, b BIGINT) WITH ('connector'='test-sink')
        """
        self.t_env.execute_sql(sink_table_ddl)
        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select(
            expr.call('check_python_exec', t.a),
            expr.call('check_pyflink_gateway_disabled', t.a)) \
            .execute_insert("Results").wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[1, 1]", "+I[2, 2]", "+I[3, 3]"])


if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
