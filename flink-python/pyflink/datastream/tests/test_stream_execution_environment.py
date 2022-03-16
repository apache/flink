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
import datetime
import decimal
import glob
import json
import os
import shutil
import tempfile
import time
import unittest
import uuid

from pyflink.common import Configuration, ExecutionConfig, RestartStrategies
from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import (StreamExecutionEnvironment, CheckpointConfig,
                                CheckpointingMode, MemoryStateBackend, TimeCharacteristic,
                                SlotSharingGroup)
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.functions import SourceFunction
from pyflink.datastream.slot_sharing_group import MemorySize
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.find_flink_home import _find_flink_source_root
from pyflink.java_gateway import get_gateway
from pyflink.table import DataTypes, CsvTableSource, CsvTableSink, StreamTableEnvironment, \
    EnvironmentSettings
from pyflink.testing.test_case_utils import PyFlinkTestCase, exec_insert_table
from pyflink.util.java_utils import get_j_env_configuration


class StreamExecutionEnvironmentTests(PyFlinkTestCase):

    def setUp(self):
        os.environ['_python_worker_execution_mode'] = "loopback"
        self.env = StreamExecutionEnvironment.get_execution_environment()
        os.environ['_python_worker_execution_mode'] = "process"
        self.env.set_parallelism(2)
        self.test_sink = DataStreamTestSinkFunction()

    def test_get_config(self):
        execution_config = self.env.get_config()

        self.assertIsInstance(execution_config, ExecutionConfig)

    def test_get_set_parallelism(self):
        self.env.set_parallelism(10)

        parallelism = self.env.get_parallelism()

        self.assertEqual(parallelism, 10)

    def test_get_set_buffer_timeout(self):
        self.env.set_buffer_timeout(12000)

        timeout = self.env.get_buffer_timeout()

        self.assertEqual(timeout, 12000)

    def test_get_set_default_local_parallelism(self):
        self.env.set_default_local_parallelism(8)

        parallelism = self.env.get_default_local_parallelism()

        self.assertEqual(parallelism, 8)

    def test_set_get_restart_strategy(self):
        self.env.set_restart_strategy(RestartStrategies.no_restart())

        restart_strategy = self.env.get_restart_strategy()

        self.assertEqual(restart_strategy, RestartStrategies.no_restart())

    def test_add_default_kryo_serializer(self):
        self.env.add_default_kryo_serializer(
            "org.apache.flink.runtime.state.StateBackendTestBase$TestPojo",
            "org.apache.flink.runtime.state.StateBackendTestBase$CustomKryoTestSerializer")

        class_dict = self.env.get_config().get_default_kryo_serializer_classes()

        self.assertEqual(class_dict,
                         {'org.apache.flink.runtime.state.StateBackendTestBase$TestPojo':
                          'org.apache.flink.runtime.state'
                          '.StateBackendTestBase$CustomKryoTestSerializer'})

    def test_register_type_with_kryo_serializer(self):
        self.env.register_type_with_kryo_serializer(
            "org.apache.flink.runtime.state.StateBackendTestBase$TestPojo",
            "org.apache.flink.runtime.state.StateBackendTestBase$CustomKryoTestSerializer")

        class_dict = self.env.get_config().get_registered_types_with_kryo_serializer_classes()

        self.assertEqual(class_dict,
                         {'org.apache.flink.runtime.state.StateBackendTestBase$TestPojo':
                          'org.apache.flink.runtime.state'
                          '.StateBackendTestBase$CustomKryoTestSerializer'})

    def test_register_type(self):
        self.env.register_type("org.apache.flink.runtime.state.StateBackendTestBase$TestPojo")

        type_list = self.env.get_config().get_registered_pojo_types()

        self.assertEqual(type_list,
                         ['org.apache.flink.runtime.state.StateBackendTestBase$TestPojo'])

    def test_get_set_max_parallelism(self):
        self.env.set_max_parallelism(12)

        parallelism = self.env.get_max_parallelism()

        self.assertEqual(parallelism, 12)

    def test_set_runtime_mode(self):
        self.env.set_runtime_mode(RuntimeExecutionMode.BATCH)

        config = get_j_env_configuration(self.env._j_stream_execution_environment)
        runtime_mode = config.getValue(
            get_gateway().jvm.org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE)

        self.assertEqual(runtime_mode, "BATCH")

    def test_operation_chaining(self):
        self.assertTrue(self.env.is_chaining_enabled())

        self.env.disable_operator_chaining()

        self.assertFalse(self.env.is_chaining_enabled())

    def test_get_checkpoint_config(self):
        checkpoint_config = self.env.get_checkpoint_config()

        self.assertIsInstance(checkpoint_config, CheckpointConfig)

    def test_get_set_checkpoint_interval(self):
        self.env.enable_checkpointing(30000)

        interval = self.env.get_checkpoint_interval()

        self.assertEqual(interval, 30000)

    def test_get_set_checkpointing_mode(self):
        mode = self.env.get_checkpointing_mode()
        self.assertEqual(mode, CheckpointingMode.EXACTLY_ONCE)

        self.env.enable_checkpointing(30000, CheckpointingMode.AT_LEAST_ONCE)

        mode = self.env.get_checkpointing_mode()

        self.assertEqual(mode, CheckpointingMode.AT_LEAST_ONCE)

    def test_get_state_backend(self):
        state_backend = self.env.get_state_backend()

        self.assertIsNone(state_backend)

    def test_set_state_backend(self):
        input_backend = MemoryStateBackend()

        self.env.set_state_backend(input_backend)

        output_backend = self.env.get_state_backend()

        self.assertEqual(output_backend._j_memory_state_backend,
                         input_backend._j_memory_state_backend)

    def test_is_changelog_state_backend_enabled(self):
        self.assertIsNone(self.env.is_changelog_state_backend_enabled())

    def test_enable_changelog_state_backend(self):

        self.env.enable_changelog_state_backend(True)

        self.assertTrue(self.env.is_changelog_state_backend_enabled())

        self.env.enable_changelog_state_backend(False)

        self.assertFalse(self.env.is_changelog_state_backend_enabled())

    def test_get_set_stream_time_characteristic(self):
        default_time_characteristic = self.env.get_stream_time_characteristic()

        self.assertEqual(default_time_characteristic, TimeCharacteristic.EventTime)

        self.env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

        time_characteristic = self.env.get_stream_time_characteristic()

        self.assertEqual(time_characteristic, TimeCharacteristic.ProcessingTime)

    def test_configure(self):
        configuration = Configuration()
        configuration.set_string('pipeline.operator-chaining', 'false')
        configuration.set_string('pipeline.time-characteristic', 'IngestionTime')
        configuration.set_string('execution.buffer-timeout', '1 min')
        configuration.set_string('execution.checkpointing.timeout', '12000')
        configuration.set_string('state.backend', 'jobmanager')
        self.env.configure(configuration)
        self.assertEqual(self.env.is_chaining_enabled(), False)
        self.assertEqual(self.env.get_stream_time_characteristic(),
                         TimeCharacteristic.IngestionTime)
        self.assertEqual(self.env.get_buffer_timeout(), 60000)
        self.assertEqual(self.env.get_checkpoint_config().get_checkpoint_timeout(), 12000)
        self.assertTrue(isinstance(self.env.get_state_backend(), MemoryStateBackend))

    @unittest.skip("Python API does not support DataStream now. refactor this test later")
    def test_get_execution_plan(self):
        tmp_dir = tempfile.gettempdir()
        source_path = os.path.join(tmp_dir + '/streaming.csv')
        tmp_csv = os.path.join(tmp_dir + '/streaming2.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]

        t_env = StreamTableEnvironment.create(self.env)
        csv_source = CsvTableSource(source_path, field_names, field_types)
        t_env.register_table_source("Orders", csv_source)
        t_env.register_table_sink(
            "Results",
            CsvTableSink(field_names, field_types, tmp_csv))
        t_env.from_path("Orders").execute_insert("Results").wait()

        plan = self.env.get_execution_plan()

        json.loads(plan)

    def test_execute(self):
        tmp_dir = tempfile.gettempdir()
        field_names = ['a', 'b', 'c']
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env = StreamTableEnvironment.create(self.env)
        t_env.register_table_sink(
            'Results',
            CsvTableSink(field_names, field_types,
                         os.path.join('{}/{}.csv'.format(tmp_dir, round(time.time())))))
        execution_result = exec_insert_table(
            t_env.from_elements([(1, 'Hi', 'Hello')], ['a', 'b', 'c']),
            'Results')
        self.assertIsNotNone(execution_result.get_job_id())
        self.assertIsNotNone(execution_result.get_net_runtime())
        self.assertEqual(len(execution_result.get_all_accumulator_results()), 0)
        self.assertIsNone(execution_result.get_accumulator_result('accumulator'))
        self.assertIsNotNone(str(execution_result))

    def test_from_collection_without_data_types(self):
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')])
        ds.add_sink(self.test_sink)
        self.env.execute("test from collection")
        results = self.test_sink.get_results(True)
        # user does not specify data types for input data, the collected result should be in
        # in tuple format as inputs.
        expected = ["(1, 'Hi', 'Hello')", "(2, 'Hello', 'Hi')"]
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_from_collection_with_data_types(self):
        # verify from_collection for the collection with single object.
        ds = self.env.from_collection(['Hi', 'Hello'], type_info=Types.STRING())
        ds.add_sink(self.test_sink)
        self.env.execute("test from collection with single object")
        results = self.test_sink.get_results(False)
        expected = ['Hello', 'Hi']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

        # verify from_collection for the collection with multiple objects like tuple.
        ds = self.env.from_collection([(1, None, 1, True, 32767, -2147483648, 1.23, 1.98932,
                                        bytearray(b'flink'), 'pyflink', datetime.date(2014, 9, 13),
                                        datetime.time(hour=12, minute=0, second=0,
                                                      microsecond=123000),
                                        datetime.datetime(2018, 3, 11, 3, 0, 0, 123000), [1, 2, 3],
                                        decimal.Decimal('1000000000000000000.05'),
                                        decimal.Decimal('1000000000000000000.0599999999999'
                                                        '9999899999999999')),
                                       (2, None, 2, True, 43878, 9147483648, 9.87, 2.98936,
                                        bytearray(b'flink'), 'pyflink', datetime.date(2015, 10, 14),
                                        datetime.time(hour=11, minute=2, second=2,
                                                      microsecond=234500),
                                        datetime.datetime(2020, 4, 15, 8, 2, 6, 235000), [2, 4, 6],
                                        decimal.Decimal('2000000000000000000.74'),
                                        decimal.Decimal('2000000000000000000.061111111111111'
                                                        '11111111111111'))],
                                      type_info=Types.ROW(
                                          [Types.LONG(), Types.LONG(), Types.SHORT(),
                                           Types.BOOLEAN(), Types.SHORT(), Types.INT(),
                                           Types.FLOAT(), Types.DOUBLE(),
                                           Types.PICKLED_BYTE_ARRAY(),
                                           Types.STRING(), Types.SQL_DATE(), Types.SQL_TIME(),
                                           Types.SQL_TIMESTAMP(),
                                           Types.BASIC_ARRAY(Types.LONG()), Types.BIG_DEC(),
                                           Types.BIG_DEC()]))
        ds.add_sink(self.test_sink)
        self.env.execute("test from collection with tuple object")
        results = self.test_sink.get_results(False)
        # if user specifies data types of input data, the collected result should be in row format.
        expected = [
            '+I[1, null, 1, true, 32767, -2147483648, 1.23, 1.98932, [102, 108, 105, 110, 107], '
            'pyflink, 2014-09-13, 12:00:00, 2018-03-11 03:00:00.123, [1, 2, 3], '
            '1000000000000000000.05, 1000000000000000000.05999999999999999899999999999]',
            '+I[2, null, 2, true, -21658, 557549056, 9.87, 2.98936, [102, 108, 105, 110, 107], '
            'pyflink, 2015-10-14, 11:02:02, 2020-04-15 08:02:06.235, [2, 4, 6], '
            '2000000000000000000.74, 2000000000000000000.06111111111111111111111111111]']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_add_custom_source(self):
        custom_source = SourceFunction("org.apache.flink.python.util.MyCustomSourceFunction")
        ds = self.env.add_source(custom_source, type_info=Types.ROW([Types.INT(), Types.STRING()]))
        ds.add_sink(self.test_sink)
        self.env.execute("test add custom source")
        results = self.test_sink.get_results(False)
        expected = [
            '+I[3, Mike]',
            '+I[1, Marry]',
            '+I[4, Ted]',
            '+I[5, Jack]',
            '+I[0, Bob]',
            '+I[2, Henry]']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_read_text_file(self):
        texts = ["Mike", "Marry", "Ted", "Jack", "Bob", "Henry"]
        text_file_path = self.tempdir + '/text_file'
        with open(text_file_path, 'a') as f:
            for text in texts:
                f.write(text)
                f.write('\n')

        ds = self.env.read_text_file(text_file_path)
        ds.add_sink(self.test_sink)
        self.env.execute("test read text file")
        results = self.test_sink.get_results()
        results.sort()
        texts.sort()
        self.assertEqual(texts, results)

    def test_execute_async(self):
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')],
                                      type_info=Types.ROW(
                                          [Types.INT(), Types.STRING(), Types.STRING()]))
        ds.add_sink(self.test_sink)
        job_client = self.env.execute_async("test execute async")
        job_id = job_client.get_job_id()
        self.assertIsNotNone(job_id)
        execution_result = job_client.get_job_execution_result().result()
        self.assertEqual(str(job_id), str(execution_result.get_job_id()))

    def test_add_python_file(self):
        import uuid
        env = self.env
        python_file_dir = os.path.join(self.tempdir, "python_file_dir_" + str(uuid.uuid4()))
        os.mkdir(python_file_dir)
        python_file_path = os.path.join(python_file_dir, "test_dep1.py")
        with open(python_file_path, 'w') as f:
            f.write("def add_two(a):\n    return a + 2")

        def plus_two_map(value):
            from test_dep1 import add_two
            return add_two(value)

        get_j_env_configuration(env._j_stream_execution_environment).\
            setString("taskmanager.numberOfTaskSlots", "10")
        env.add_python_file(python_file_path)
        ds = env.from_collection([1, 2, 3, 4, 5])
        ds = ds.map(plus_two_map, Types.LONG()) \
               .slot_sharing_group("data_stream") \
               .map(lambda i: i, Types.LONG()) \
               .slot_sharing_group("table")

        python_file_path = os.path.join(python_file_dir, "test_dep2.py")
        with open(python_file_path, 'w') as f:
            f.write("def add_three(a):\n    return a + 3")

        def plus_three(value):
            from test_dep2 import add_three
            return add_three(value)

        t_env = StreamTableEnvironment.create(
            stream_execution_environment=env,
            environment_settings=EnvironmentSettings.in_streaming_mode())
        env.add_python_file(python_file_path)

        from pyflink.table.udf import udf
        from pyflink.table.expressions import col
        add_three = udf(plus_three, result_type=DataTypes.BIGINT())

        tab = t_env.from_data_stream(ds, 'a') \
                   .select(add_three(col('a')))
        t_env.to_append_stream(tab, Types.ROW([Types.LONG()])) \
             .map(lambda i: i[0]) \
             .add_sink(self.test_sink)
        env.execute("test add_python_file")
        result = self.test_sink.get_results(True)
        expected = ['6', '7', '8', '9', '10']
        result.sort()
        expected.sort()
        self.assertEqual(expected, result)

    def test_add_python_file_2(self):
        import uuid
        env = self.env
        python_file_dir = os.path.join(self.tempdir, "python_file_dir_" + str(uuid.uuid4()))
        os.mkdir(python_file_dir)
        python_file_path = os.path.join(python_file_dir, "test_dep1.py")
        with open(python_file_path, 'w') as f:
            f.write("def add_two(a):\n    return a + 2")

        def plus_two_map(value):
            from test_dep1 import add_two
            return add_two(value)

        get_j_env_configuration(env._j_stream_execution_environment).\
            setString("taskmanager.numberOfTaskSlots", "10")
        env.add_python_file(python_file_path)
        ds = env.from_collection([1, 2, 3, 4, 5])
        ds = ds.map(plus_two_map, Types.LONG()) \
               .slot_sharing_group("data_stream") \
               .map(lambda i: i, Types.LONG()) \
               .slot_sharing_group("table")

        python_file_path = os.path.join(python_file_dir, "test_dep2.py")
        with open(python_file_path, 'w') as f:
            f.write("def add_three(a):\n    return a + 3")

        def plus_three(value):
            from test_dep2 import add_three
            return add_three(value)

        t_env = StreamTableEnvironment.create(
            stream_execution_environment=env,
            environment_settings=EnvironmentSettings.in_streaming_mode())
        env.add_python_file(python_file_path)

        from pyflink.table.udf import udf
        from pyflink.table.expressions import col
        add_three = udf(plus_three, result_type=DataTypes.BIGINT())

        tab = t_env.from_data_stream(ds, col('a')) \
                   .select(add_three(col('a')))
        result = [i[0] for i in tab.execute().collect()]
        expected = [6, 7, 8, 9, 10]
        result.sort()
        expected.sort()
        self.assertEqual(expected, result)

    def test_set_requirements_without_cached_directory(self):
        import uuid
        requirements_txt_path = os.path.join(self.tempdir, str(uuid.uuid4()))
        with open(requirements_txt_path, 'w') as f:
            f.write("cloudpickle==1.2.2")
        self.env.set_python_requirements(requirements_txt_path)

        def check_requirements(i):
            import cloudpickle  # noqa # pylint: disable=unused-import
            return i

        ds = self.env.from_collection([1, 2, 3, 4, 5])
        ds.map(check_requirements).add_sink(self.test_sink)
        self.env.execute("test set requirements without cache dir")
        result = self.test_sink.get_results(True)
        expected = ['1', '2', '3', '4', '5']
        result.sort()
        expected.sort()
        self.assertEqual(expected, result)

    def test_set_requirements_with_cached_directory(self):
        import uuid
        tmp_dir = self.tempdir
        env = self.env
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
        env.set_python_requirements(requirements_txt_path, requirements_dir_path)

        def add_one(i):
            from python_package1 import plus
            return plus(i, 1)

        ds = env.from_collection([1, 2, 3, 4, 5])
        ds.map(add_one).add_sink(self.test_sink)
        env.execute("test set requirements with cachd dir")
        result = self.test_sink.get_results(True)
        expected = ['2', '3', '4', '5', '6']
        result.sort()
        expected.sort()
        self.assertEqual(expected, result)

    def test_add_python_archive(self):
        import uuid
        import shutil
        tmp_dir = self.tempdir
        env = self.env
        archive_dir_path = os.path.join(tmp_dir, "archive_" + str(uuid.uuid4()))
        os.mkdir(archive_dir_path)
        with open(os.path.join(archive_dir_path, "data.txt"), 'w') as f:
            f.write("2")
        archive_file_path = \
            shutil.make_archive(os.path.dirname(archive_dir_path), 'zip', archive_dir_path)
        env.add_python_archive(archive_file_path, "data")

        def add_from_file(i):
            with open("data/data.txt", 'r') as f:
                return i + int(f.read())

        ds = env.from_collection([1, 2, 3, 4, 5])
        ds.map(add_from_file).add_sink(self.test_sink)
        env.execute("test set python archive")
        result = self.test_sink.get_results(True)
        expected = ['3', '4', '5', '6', '7']
        result.sort()
        expected.sort()
        self.assertEqual(expected, result)

    def test_set_stream_env(self):
        import sys
        env = self.env
        python_exec_link_path = sys.executable
        env.set_python_executable(python_exec_link_path)

        def check_python_exec(i):
            import os
            assert os.environ["python"] == python_exec_link_path
            return i

        ds = env.from_collection([1, 2, 3, 4, 5])
        ds.map(check_python_exec).add_sink(self.test_sink)
        env.execute("test set python executable")
        result = self.test_sink.get_results(True)
        expected = ['1', '2', '3', '4', '5']
        result.sort()
        expected.sort()
        self.assertEqual(expected, result)

    def test_add_jars(self):
        # find kafka connector jars
        flink_source_root = _find_flink_source_root()
        jars_abs_path = flink_source_root + '/flink-connectors/flink-sql-connector-kafka'
        specific_jars = glob.glob(jars_abs_path + '/target/flink*.jar')
        specific_jars = ['file://' + specific_jar for specific_jar in specific_jars]

        self.env.add_jars(*specific_jars)
        source_topic = 'test_source_topic'
        props = {'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
        type_info = Types.ROW([Types.INT(), Types.STRING()])

        # Test for kafka consumer
        deserialization_schema = JsonRowDeserializationSchema.builder() \
            .type_info(type_info=type_info).build()

        # Will get a ClassNotFoundException if not add the kafka connector into the pipeline jars.
        kafka_consumer = FlinkKafkaConsumer(source_topic, deserialization_schema, props)
        self.env.add_source(kafka_consumer).print()
        self.env.get_execution_plan()

    def test_add_classpaths(self):
        # find kafka connector jars
        flink_source_root = _find_flink_source_root()
        jars_abs_path = flink_source_root + '/flink-connectors/flink-sql-connector-kafka'
        specific_jars = glob.glob(jars_abs_path + '/target/flink*.jar')
        specific_jars = ['file://' + specific_jar for specific_jar in specific_jars]

        self.env.add_classpaths(*specific_jars)
        source_topic = 'test_source_topic'
        props = {'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
        type_info = Types.ROW([Types.INT(), Types.STRING()])

        # Test for kafka consumer
        deserialization_schema = JsonRowDeserializationSchema.builder() \
            .type_info(type_info=type_info).build()

        # It Will raise a ClassNotFoundException if the kafka connector is not added into the
        # pipeline classpaths.
        kafka_consumer = FlinkKafkaConsumer(source_topic, deserialization_schema, props)
        self.env.add_source(kafka_consumer).print()
        self.env.get_execution_plan()

    def test_generate_stream_graph_with_dependencies(self):
        python_file_dir = os.path.join(self.tempdir, "python_file_dir_" + str(uuid.uuid4()))
        os.mkdir(python_file_dir)
        python_file_path = os.path.join(python_file_dir, "test_stream_dependency_manage_lib.py")
        with open(python_file_path, 'w') as f:
            f.write("def add_two(a):\n    return a + 2")
        env = self.env
        env.add_python_file(python_file_path)

        def plus_two_map(value):
            from test_stream_dependency_manage_lib import add_two
            return value[0], add_two(value[1])

        def add_from_file(i):
            with open("data/data.txt", 'r') as f:
                return i[0], i[1] + int(f.read())

        from_collection_source = env.from_collection([('a', 0), ('b', 0), ('c', 1), ('d', 1),
                                                      ('e', 2)],
                                                     type_info=Types.ROW([Types.STRING(),
                                                                          Types.INT()]))
        from_collection_source.name("From Collection")
        keyed_stream = from_collection_source.key_by(lambda x: x[1], key_type=Types.INT())

        plus_two_map_stream = keyed_stream.map(plus_two_map).name("Plus Two Map").set_parallelism(3)

        add_from_file_map = plus_two_map_stream.map(add_from_file).name("Add From File Map")

        test_stream_sink = add_from_file_map.add_sink(self.test_sink).name("Test Sink")
        test_stream_sink.set_parallelism(4)

        archive_dir_path = os.path.join(self.tempdir, "archive_" + str(uuid.uuid4()))
        os.mkdir(archive_dir_path)
        with open(os.path.join(archive_dir_path, "data.txt"), 'w') as f:
            f.write("3")
        archive_file_path = \
            shutil.make_archive(os.path.dirname(archive_dir_path), 'zip', archive_dir_path)
        env.add_python_archive(archive_file_path, "data")

        nodes = eval(env.get_execution_plan())['nodes']

        # The StreamGraph should be as bellow:
        # Source: From Collection -> _stream_key_by_map_operator ->
        # Plus Two Map -> Add From File Map -> Sink: Test Sink.

        # Source: From Collection and _stream_key_by_map_operator should have same parallelism.
        self.assertEqual(nodes[0]['parallelism'], nodes[1]['parallelism'])

        # The parallelism of Plus Two Map should be 3
        self.assertEqual(nodes[2]['parallelism'], 3)

        # The ship_strategy for Source: From Collection and _stream_key_by_map_operator should be
        # FORWARD
        self.assertEqual(nodes[1]['predecessors'][0]['ship_strategy'], "FORWARD")

        # The ship_strategy for _keyed_stream_values_operator and Plus Two Map should be
        # HASH
        self.assertEqual(nodes[2]['predecessors'][0]['ship_strategy'], "HASH")

        # The parallelism of Sink: Test Sink should be 4
        self.assertEqual(nodes[4]['parallelism'], 4)

        env_config_with_dependencies = dict(get_gateway().jvm.org.apache.flink.python.util
                                            .PythonConfigUtil.getEnvConfigWithDependencies(
            env._j_stream_execution_environment).toMap())

        # Make sure that user specified files and archives are correctly added.
        self.assertIsNotNone(env_config_with_dependencies['python.files'])
        self.assertIsNotNone(env_config_with_dependencies['python.archives'])

    def test_register_slot_sharing_group(self):
        slot_sharing_group_1 = SlotSharingGroup.builder('slot_sharing_group_1') \
            .set_cpu_cores(1.0).set_task_heap_memory_mb(100).build()
        slot_sharing_group_2 = SlotSharingGroup.builder('slot_sharing_group_2') \
            .set_cpu_cores(2.0).set_task_heap_memory_mb(200).build()
        slot_sharing_group_3 = SlotSharingGroup.builder('slot_sharing_group_3').build()
        self.env.register_slot_sharing_group(slot_sharing_group_1)
        self.env.register_slot_sharing_group(slot_sharing_group_2)
        self.env.register_slot_sharing_group(slot_sharing_group_3)
        ds = self.env.from_collection([1, 2, 3]).slot_sharing_group(
            'slot_sharing_group_1')
        ds.map(lambda x: x + 1).set_parallelism(3) \
            .slot_sharing_group('slot_sharing_group_2') \
            .add_sink(self.test_sink)

        j_generated_stream_graph = self.env._j_stream_execution_environment \
            .getStreamGraph(True)
        j_resource_profile_1 = j_generated_stream_graph.getSlotSharingGroupResource(
            'slot_sharing_group_1').get()
        j_resource_profile_2 = j_generated_stream_graph.getSlotSharingGroupResource(
            'slot_sharing_group_2').get()
        j_resource_profile_3 = j_generated_stream_graph.getSlotSharingGroupResource(
            'slot_sharing_group_3')
        self.assertEqual(j_resource_profile_1.getCpuCores().getValue(), 1.0)
        self.assertEqual(MemorySize(j_memory_size=j_resource_profile_1.getTaskHeapMemory()),
                         MemorySize.of_mebi_bytes(100))
        self.assertEqual(j_resource_profile_2.getCpuCores().getValue(), 2.0)
        self.assertEqual(MemorySize(j_memory_size=j_resource_profile_2.getTaskHeapMemory()),
                         MemorySize.of_mebi_bytes(200))
        self.assertFalse(j_resource_profile_3.isPresent())

    def test_register_cached_file(self):
        texts = ['machen', 'zeit', 'heerscharen', 'keiner', 'meine']
        text_path = self.tempdir + '/text_file'
        with open(text_path, 'a') as f:
            for text in texts:
                f.write(text)
                f.write('\n')
        self.env.register_cached_file(text_path, 'cache_test')
        cached_files = self.env._j_stream_execution_environment.getCachedFiles()
        self.assertEqual(cached_files.size(), 1)
        self.assertEqual(cached_files[0].getField(0), 'cache_test')

    def tearDown(self) -> None:
        self.test_sink.clear()
