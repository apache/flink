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

from pyflink.pyflink_gateway_server import on_windows
from pyflink.table import DataTypes
from pyflink.table import expressions as expr
from pyflink.table.udf import udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import (PyFlinkBlinkStreamTableTestCase,
                                             PyFlinkBlinkBatchTableTestCase,
                                             PyFlinkOldStreamTableTestCase,
                                             PyFlinkOldBatchTableTestCase)


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
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)
        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select(expr.call("add_two", t.a), t.a).execute_insert("Results").wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[3, 1]", "+I[4, 2]", "+I[5, 3]"])


class FlinkStreamDependencyTests(DependencyTests, PyFlinkOldStreamTableTestCase):

    pass


class FlinkBatchDependencyTests(PyFlinkOldBatchTableTestCase):

    def test_add_python_file(self):
        python_file_dir = os.path.join(self.tempdir, "python_file_dir_" + str(uuid.uuid4()))
        os.mkdir(python_file_dir)
        python_file_path = os.path.join(python_file_dir, "test_dependency_manage_lib.py")
        with open(python_file_path, 'w') as f:
            f.write("def add_two(a):\n    return a + 2")
        self.t_env.add_python_file(python_file_path)

        def plus_two(i):
            from test_dependency_manage_lib import add_two
            return add_two(i)

        self.t_env.create_temporary_system_function(
            "add_two", udf(plus_two, DataTypes.BIGINT(), DataTypes.BIGINT()))

        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t = t.select(expr.call('add_two', t.a), t.a)

        result = self.collect(t)
        self.assertEqual(result, ["+I[3, 1]", "+I[4, 2]", "+I[5, 3]"])


class BlinkBatchDependencyTests(DependencyTests, PyFlinkBlinkBatchTableTestCase):

    pass


class BlinkStreamDependencyTests(DependencyTests, PyFlinkBlinkStreamTableTestCase):

    def test_set_requirements_without_cached_directory(self):
        requirements_txt_path = os.path.join(self.tempdir, str(uuid.uuid4()))
        with open(requirements_txt_path, 'w') as f:
            f.write("cloudpickle==1.2.2")
        self.t_env.set_python_requirements(requirements_txt_path)

        def check_requirements(i):
            import cloudpickle
            assert os.path.abspath(cloudpickle.__file__).startswith(
                os.environ['_PYTHON_REQUIREMENTS_INSTALL_DIR'])
            return i

        self.t_env.create_temporary_system_function("check_requirements",
                                                    udf(check_requirements, DataTypes.BIGINT(),
                                                        DataTypes.BIGINT()))
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)
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
        package_file_name = "python-package1-0.0.0.tar.gz"
        with open(os.path.join(requirements_dir_path, package_file_name), 'wb') as f:
            import base64
            # This base64 data is encoded from a python package file which includes a
            # "python_package1" module. The module contains a "plus(a, b)" function.
            # The base64 can be recomputed by following code:
            # base64.b64encode(open("python-package1-0.0.0.tar.gz", "rb").read()).decode("utf-8")
            f.write(base64.b64decode(
                "H4sICNefrV0C/2Rpc3QvcHl0aG9uLXBhY2thZ2UxLTAuMC4wLnRhcgDtmVtv2jAYhnPtX2H1CrRCY+ckI"
                "XEx7axuUA11u5imyICTRc1JiVnHfv1MKKWjYxwKEdPehws7xkmUfH5f+3PyqfqWpa1cjG5EKFnLbOvfhX"
                "FQTI3nOPPSdavS5Pa8nGMwy3Esi3ke9wyTObbnGNQxamBSKlFQavzUryG8ldG6frpbEGx4yNmDLMp/hPy"
                "P8b+6fNN613vdP1z8XdteG3+ug/17/F3Hcw1qIv5H54NUYiyUaH2SRRllaYeytkl6IpEdujI2yH2XapCQ"
                "wSRJRDHt0OveZa//uUfeZonUvUO5bHo+0ZcoVo9bMhFRvGx9H41kWj447aUsR0WUq+pui8arWKggK5Jli"
                "wGOo/95q79ovXi6/nfyf246Dof/n078fT9KI+X77Xx6BP83bX4Xf5NxT7dz7toO/L8OxjKgeTwpG+KcDp"
                "sdQjWFVJMipYI+o0MCk4X/t2UYtqI0yPabCHb3f861XcD/Ty/+Y5nLdCzT0dSPo/SmbKsf6un+b7KV+Ls"
                "W4/D/OoC9w/930P9eGwM75//csrD+Q/6P/P/k9D/oX3988Wqw1bS/tf6tR+s/m3EG/ddBqXO9XKf15C8p"
                "P9k4HZBtBgzZaVW5vrfKcj+W32W82ygEB9D/Xu9+4/qfP9L/rBv0X1v87yONKRX61/qfzwqjIDzIPTbv/"
                "7or3/88i0H/tfBFW7s/s/avRInQH06ieEy7tDrQeYHUdRN7wP+n/vf62LOH/pld7f9xz7a5Pfufedy0oP"
                "86iJI8KxStAq6yLC4JWdbbVbWRikR2z1ZGytk5vauW3QdnBFE6XqwmykazCesAAAAAAAAAAAAAAAAAAAA"
                "AAAAAAAAAAAAAAOBw/AJw5CHBAFAAAA=="))
        self.t_env.set_python_requirements(requirements_txt_path, requirements_dir_path)

        def add_one(i):
            from python_package1 import plus
            return plus(i, 1)

        self.t_env.create_temporary_system_function("add_one",
                                                    udf(add_one, DataTypes.BIGINT(),
                                                        DataTypes.BIGINT()))
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)
        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select(expr.call('add_one', t.a), t.a).execute_insert("Results").wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[2, 1]", "+I[3, 2]", "+I[4, 3]"])

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
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)
        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select(expr.call('add_from_file', t.a), t.a).execute_insert("Results").wait()

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["+I[3, 1]", "+I[4, 2]", "+I[5, 3]"])

    @unittest.skipIf(on_windows(), "Symbolic link is not supported on Windows, skipping.")
    def test_set_environment(self):
        python_exec = sys.executable
        tmp_dir = self.tempdir
        python_exec_link_path = os.path.join(tmp_dir, "py_exec")
        os.symlink(python_exec, python_exec_link_path)
        self.t_env.get_config().set_python_executable(python_exec_link_path)

        def check_python_exec(i):
            import os
            assert os.environ["python"] == python_exec_link_path
            return i

        self.t_env.create_temporary_system_function("check_python_exec",
                                                    udf(check_python_exec, DataTypes.BIGINT(),
                                                        DataTypes.BIGINT()))

        def check_pyflink_gateway_disabled(i):
            try:
                from pyflink.java_gateway import get_gateway
                get_gateway()
            except Exception as e:
                assert str(e).startswith("It's launching the PythonGatewayServer during Python UDF"
                                         " execution which is unexpected.")
            else:
                raise Exception("The gateway server is not disabled!")
            return i

        self.t_env.create_temporary_system_function(
            "check_pyflink_gateway_disabled",
            udf(check_pyflink_gateway_disabled, DataTypes.BIGINT(),
                DataTypes.BIGINT()))

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)
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
