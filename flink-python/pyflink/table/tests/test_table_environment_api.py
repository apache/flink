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
# #  distributed under the License is distributed on an "AS IS" BASIS,
# #  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# #  See the License for the specific language governing permissions and
# # limitations under the License.
################################################################################
import datetime
import decimal
import os
import sys
from py4j.protocol import Py4JJavaError

from pyflink.common import RowKind
from pyflink.common.typeinfo import Types
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.java_gateway import get_gateway
from pyflink.table import DataTypes, CsvTableSink, StreamTableEnvironment, EnvironmentSettings, \
    Module, ResultKind, ModuleEntry
from pyflink.table.catalog import ObjectPath, CatalogBaseTable
from pyflink.table.descriptors import FileSystem, OldCsv
from pyflink.table.explain_detail import ExplainDetail
from pyflink.table.expressions import col
from pyflink.table.table_descriptor import TableDescriptor
from pyflink.table.types import RowType, Row
from pyflink.table.udf import udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import \
    PyFlinkBatchTableTestCase, PyFlinkStreamTableTestCase, \
    _load_specific_flink_module_jars
from pyflink.util.java_utils import get_j_env_configuration


class TableEnvironmentTest(object):

    def test_set_sys_executable_for_local_mode(self):
        jvm = get_gateway().jvm
        actual_executable = get_j_env_configuration(self.t_env._get_j_env()) \
            .getString(jvm.PythonOptions.PYTHON_EXECUTABLE.key(), None)
        self.assertEqual(sys.executable, actual_executable)

    def test_explain(self):
        schema = RowType() \
            .add('a', DataTypes.INT()) \
            .add('b', DataTypes.STRING()) \
            .add('c', DataTypes.STRING())
        t_env = self.t_env
        t = t_env.from_elements([], schema)
        result = t.select(t.a + 1, t.b, t.c)

        actual = result.explain()

        assert isinstance(actual, str)

    def test_explain_with_extended(self):
        schema = RowType() \
            .add('a', DataTypes.INT()) \
            .add('b', DataTypes.STRING()) \
            .add('c', DataTypes.STRING())
        t_env = self.t_env
        t = t_env.from_elements([], schema)
        result = t.select(t.a + 1, t.b, t.c)

        actual = result.explain(ExplainDetail.ESTIMATED_COST, ExplainDetail.CHANGELOG_MODE,
                                ExplainDetail.JSON_EXECUTION_PLAN)

        assert isinstance(actual, str)

    def test_register_functions(self):
        t_env = self.t_env

        t_env.register_function(
            "python_scalar_func", udf(lambda i: i, result_type=DataTypes.INT()))

        t_env.register_java_function("scalar_func",
                                     "org.apache.flink.table.legacyutils.RichFunc0")
        t_env.register_java_function(
            "agg_func", "org.apache.flink.table.legacyutils.ByteMaxAggFunction")
        t_env.register_java_function("table_func", "org.apache.flink.table.legacyutils.TableFunc1")

        actual = t_env.list_user_defined_functions()
        expected = ['python_scalar_func', 'scalar_func', 'agg_func', 'table_func']
        self.assert_equals(actual, expected)

    def test_load_module_twice(self):
        t_env = self.t_env
        self.check_list_modules('core')
        self.check_list_full_modules(1, 'core')
        self.assertRaisesRegex(
            Py4JJavaError, "A module with name 'core' already exists",
            t_env.load_module, 'core', Module(
                get_gateway().jvm.org.apache.flink.table.module.CoreModule.INSTANCE))

    def test_unload_module_twice(self):
        t_env = self.t_env
        t_env.unload_module('core')
        self.check_list_modules()
        self.check_list_full_modules(0)
        self.assertRaisesRegex(
            Py4JJavaError, "No module with name 'core' exists",
            t_env.unload_module, 'core')

    def test_use_modules(self):
        # please do not change this order since ModuleMock depends on FunctionDefinitionMock
        _load_specific_flink_module_jars('/flink-table/flink-table-common')
        _load_specific_flink_module_jars('/flink-table/flink-table-api-java')

        t_env = self.t_env
        t_env.load_module('x', Module(
            get_gateway().jvm.org.apache.flink.table.utils.ModuleMock("x")
        ))
        t_env.load_module('y', Module(
            get_gateway().jvm.org.apache.flink.table.utils.ModuleMock("y")
        ))
        self.check_list_modules('core', 'x', 'y')
        self.check_list_full_modules(3, 'core', 'x', 'y')

        t_env.use_modules('y', 'core')
        self.check_list_modules('y', 'core')
        self.check_list_full_modules(2, 'y', 'core', 'x')

    def check_list_modules(self, *expected_used_modules: str):
        self.assert_equals(self.t_env.list_modules(), list(expected_used_modules))

    def check_list_full_modules(self, used_module_cnt: int, *expected_loaded_modules: str):
        self.assert_equals(self.t_env.list_full_modules(),
                           [ModuleEntry(module,
                                        expected_loaded_modules.index(module) < used_module_cnt)
                            for module in expected_loaded_modules])

    def test_unload_and_load_module(self):
        t_env = self.t_env
        t_env.unload_module('core')
        t_env.load_module('core', Module(
            get_gateway().jvm.org.apache.flink.table.module.CoreModule.INSTANCE))
        table_result = t_env.execute_sql("select concat('unload', 'load') as test_module")
        self.assertEqual(table_result.get_result_kind(), ResultKind.SUCCESS_WITH_CONTENT)
        self.assert_equals(table_result.get_table_schema().get_field_names(), ['test_module'])

    def test_create_and_drop_java_function(self):
        t_env = self.t_env

        t_env.create_java_temporary_system_function(
            "scalar_func", "org.apache.flink.table.legacyutils.RichFunc0")
        t_env.create_java_function(
            "agg_func", "org.apache.flink.table.legacyutils.ByteMaxAggFunction")
        t_env.create_java_temporary_function(
            "table_func", "org.apache.flink.table.legacyutils.TableFunc1")
        self.assert_equals(t_env.list_user_defined_functions(),
                           ['scalar_func', 'agg_func', 'table_func'])

        t_env.drop_temporary_system_function("scalar_func")
        t_env.drop_function("agg_func")
        t_env.drop_temporary_function("table_func")
        self.assert_equals(t_env.list_user_defined_functions(), [])

    def test_temporary_tables(self):
        from pyflink.table.descriptors import Schema

        t_env = self.t_env
        t_env.connect(FileSystem().path(os.path.join(self.tempdir + '/temp_1.csv'))) \
            .with_format(OldCsv()
                         .field_delimiter(',')
                         .field("a", DataTypes.INT())
                         .field("b", DataTypes.STRING())) \
            .with_schema(Schema()
                         .field("a", DataTypes.INT())
                         .field("b", DataTypes.STRING())) \
            .create_temporary_table("temporary_table_1")

        t_env.connect(FileSystem().path(os.path.join(self.tempdir + '/temp_2.csv'))) \
            .with_format(OldCsv()
                         .field_delimiter(',')
                         .field("a", DataTypes.INT())
                         .field("b", DataTypes.STRING())) \
            .with_schema(Schema()
                         .field("a", DataTypes.INT())
                         .field("b", DataTypes.STRING())) \
            .create_temporary_table("temporary_table_2")

        actual = t_env.list_temporary_tables()
        expected = ['temporary_table_1', 'temporary_table_2']
        self.assert_equals(actual, expected)

        t_env.drop_temporary_table("temporary_table_1")
        actual = t_env.list_temporary_tables()
        expected = ['temporary_table_2']
        self.assert_equals(actual, expected)

    def test_create_temporary_table_from_descriptor(self):
        from pyflink.table.schema import Schema

        t_env = self.t_env
        catalog = t_env.get_current_catalog()
        database = t_env.get_current_database()
        schema = Schema.new_builder().column("f0", DataTypes.INT()).build()
        t_env.create_temporary_table(
            "T",
            TableDescriptor.for_connector("fake")
             .schema(schema)
             .option("a", "Test")
             .build())

        self.assertFalse(t_env.get_catalog(catalog).table_exists(ObjectPath(database, "T")))
        gateway = get_gateway()

        catalog_table = CatalogBaseTable(
            t_env._j_tenv.getCatalogManager()
                 .getTable(gateway.jvm.ObjectIdentifier.of(catalog, database, "T"))
                 .get()
                 .getTable())
        self.assertEqual(schema, catalog_table.get_unresolved_schema())
        self.assertEqual("fake", catalog_table.get_options().get("connector"))
        self.assertEqual("Test", catalog_table.get_options().get("a"))

    def test_create_table_from_descriptor(self):
        from pyflink.table.schema import Schema

        catalog = self.t_env.get_current_catalog()
        database = self.t_env.get_current_database()
        schema = Schema.new_builder().column("f0", DataTypes.INT()).build()
        self.t_env.create_table(
            "T",
            TableDescriptor.for_connector("fake")
                  .schema(schema)
                  .option("a", "Test")
                  .build())
        object_path = ObjectPath(database, "T")
        self.assertTrue(self.t_env.get_catalog(catalog).table_exists(object_path))

        catalog_table = self.t_env.get_catalog(catalog).get_table(object_path)
        self.assertEqual(schema, catalog_table.get_unresolved_schema())
        self.assertEqual("fake", catalog_table.get_options().get("connector"))
        self.assertEqual("Test", catalog_table.get_options().get("a"))

    def test_table_from_descriptor(self):
        from pyflink.table.schema import Schema

        schema = Schema.new_builder().column("f0", DataTypes.INT()).build()
        descriptor = TableDescriptor.for_connector("fake").schema(schema).build()

        table = self.t_env.from_descriptor(descriptor)
        self.assertEqual(schema,
                         Schema(Schema.new_builder()._j_builder
                                .fromResolvedSchema(table._j_table.getResolvedSchema()).build()))
        table = CatalogBaseTable(self.t_env._j_tenv
                                 .getCatalogManager()
                                 .getTable(table._j_table
                                           .getQueryOperation()
                                           .getTableIdentifier())
                                 .get()
                                 .getTable())
        self.assertEqual("fake", table.get_options().get("connector"))


class DataStreamConversionTestCases(object):

    def test_from_data_stream(self):
        self.env.set_parallelism(1)

        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')],
                                      type_info=Types.ROW([Types.INT(),
                                                           Types.STRING(),
                                                           Types.STRING()]))
        t_env = self.t_env
        table = t_env.from_data_stream(ds)
        field_names = ['a', 'b', 'c']
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink("Sink",
                                  source_sink_utils.TestAppendSink(field_names, field_types))
        t_env.insert_into("Sink", table)
        t_env.execute("test_from_data_stream")
        result = source_sink_utils.results()
        expected = ['+I[1, Hi, Hello]', '+I[2, Hello, Hi]']
        self.assert_equals(result, expected)

        ds = ds.map(lambda x: x, Types.ROW([Types.INT(), Types.STRING(), Types.STRING()]))
        table = t_env.from_data_stream(ds, col('a'), col('b'), col('c'))
        t_env.register_table_sink("ExprSink",
                                  source_sink_utils.TestAppendSink(field_names, field_types))
        t_env.insert_into("ExprSink", table)
        t_env.execute("test_from_data_stream_with_expr")
        result = source_sink_utils.results()
        self.assert_equals(result, expected)

    def test_to_append_stream(self):
        self.env.set_parallelism(1)
        t_env = StreamTableEnvironment.create(
            self.env,
            environment_settings=EnvironmentSettings.in_streaming_mode())
        table = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hi")], ["a", "b", "c"])
        new_table = table.select("a + 1, b + 'flink', c")
        ds = t_env.to_append_stream(table=new_table, type_info=Types.ROW([Types.LONG(),
                                                                          Types.STRING(),
                                                                          Types.STRING()]))
        test_sink = DataStreamTestSinkFunction()
        ds.add_sink(test_sink)
        self.env.execute("test_to_append_stream")
        result = test_sink.get_results(False)
        expected = ['+I[2, Hiflink, Hello]', '+I[3, Helloflink, Hi]']
        self.assertEqual(result, expected)

    def test_to_retract_stream(self):
        self.env.set_parallelism(1)
        t_env = StreamTableEnvironment.create(
            self.env,
            environment_settings=EnvironmentSettings.in_streaming_mode())
        table = t_env.from_elements([(1, "Hi", "Hello"), (1, "Hi", "Hello")], ["a", "b", "c"])
        new_table = table.group_by("c").select("a.sum, c as b")
        ds = t_env.to_retract_stream(table=new_table, type_info=Types.ROW([Types.LONG(),
                                                                           Types.STRING()]))
        test_sink = DataStreamTestSinkFunction()
        ds.map(lambda x: x).add_sink(test_sink)
        self.env.execute("test_to_retract_stream")
        result = test_sink.get_results(True)
        expected = ["(True, Row(f0=1, f1='Hello'))", "(False, Row(f0=1, f1='Hello'))",
                    "(True, Row(f0=2, f1='Hello'))"]
        self.assertEqual(result, expected)


class StreamTableEnvironmentTests(TableEnvironmentTest, PyFlinkStreamTableTestCase):

    def test_collect_with_retract(self):
        expected_row_kinds = [RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER,
                              RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER]
        element_data = [(1, 2, 'a'),
                        (3, 4, 'b'),
                        (5, 6, 'a'),
                        (7, 8, 'b')]
        field_names = ['a', 'b', 'c']
        source = self.t_env.from_elements(element_data, field_names)
        table_result = self.t_env.execute_sql(
            "SELECT SUM(a), c FROM %s group by c" % source)
        with table_result.collect() as result:
            collected_result = []
            for i in result:
                collected_result.append(i)

            collected_result = [str(result) + ',' + str(result.get_row_kind())
                                for result in collected_result]
            expected_result = [Row(1, 'a'), Row(1, 'a'), Row(6, 'a'), Row(3, 'b'),
                               Row(3, 'b'), Row(10, 'b')]
            for i in range(len(expected_result)):
                expected_result[i] = str(expected_result[i]) + ',' + str(expected_row_kinds[i])
            expected_result.sort()
            collected_result.sort()
            self.assertEqual(expected_result, collected_result)

    def test_collect_for_all_data_types(self):
        expected_result = [Row(1, None, 1, True, 32767, -2147483648, 1.23,
                               1.98932, bytearray(b'pyflink'), 'pyflink',
                               datetime.date(2014, 9, 13), datetime.time(12, 0, 0, 123000),
                               datetime.datetime(2018, 3, 11, 3, 0, 0, 123000),
                               [Row(['[pyflink]']), Row(['[pyflink]']), Row(['[pyflink]'])],
                               {1: Row(['[flink]']), 2: Row(['[pyflink]'])},
                               decimal.Decimal('1000000000000000000.050000000000000000'),
                               decimal.Decimal('1000000000000000000.059999999999999999'))]
        source = self.t_env.from_elements(
            [(1, None, 1, True, 32767, -2147483648, 1.23, 1.98932, bytearray(b'pyflink'), 'pyflink',
              datetime.date(2014, 9, 13), datetime.time(hour=12, minute=0, second=0,
                                                        microsecond=123000),
              datetime.datetime(2018, 3, 11, 3, 0, 0, 123000),
              [Row(['pyflink']), Row(['pyflink']), Row(['pyflink'])],
              {1: Row(['flink']), 2: Row(['pyflink'])}, decimal.Decimal('1000000000000000000.05'),
              decimal.Decimal('1000000000000000000.05999999999999999899999999999'))], DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.BIGINT()), DataTypes.FIELD("b", DataTypes.BIGINT()),
                 DataTypes.FIELD("c", DataTypes.TINYINT()),
                 DataTypes.FIELD("d", DataTypes.BOOLEAN()),
                 DataTypes.FIELD("e", DataTypes.SMALLINT()),
                 DataTypes.FIELD("f", DataTypes.INT()),
                 DataTypes.FIELD("g", DataTypes.FLOAT()),
                 DataTypes.FIELD("h", DataTypes.DOUBLE()),
                 DataTypes.FIELD("i", DataTypes.BYTES()),
                 DataTypes.FIELD("j", DataTypes.STRING()),
                 DataTypes.FIELD("k", DataTypes.DATE()),
                 DataTypes.FIELD("l", DataTypes.TIME()),
                 DataTypes.FIELD("m", DataTypes.TIMESTAMP(3)),
                 DataTypes.FIELD("n", DataTypes.ARRAY(DataTypes.ROW([DataTypes.FIELD('ss2',
                                                                     DataTypes.STRING())]))),
                 DataTypes.FIELD("o", DataTypes.MAP(DataTypes.BIGINT(), DataTypes.ROW(
                     [DataTypes.FIELD('ss', DataTypes.STRING())]))),
                 DataTypes.FIELD("p", DataTypes.DECIMAL(38, 18)), DataTypes.FIELD("q",
                 DataTypes.DECIMAL(38, 18))]))
        table_result = source.execute()
        with table_result.collect() as result:
            collected_result = []
            for i in result:
                collected_result.append(i)
            self.assertEqual(expected_result, collected_result)


class BatchTableEnvironmentTests(PyFlinkBatchTableTestCase):

    def test_explain_with_multi_sinks(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "sink1",
            CsvTableSink(field_names, field_types, "path1"))
        t_env.register_table_sink(
            "sink2",
            CsvTableSink(field_names, field_types, "path2"))

        stmt_set = t_env.create_statement_set()
        stmt_set.add_insert_sql("insert into sink1 select * from %s where a > 100" % source)
        stmt_set.add_insert_sql("insert into sink2 select * from %s where a < 100" % source)

        actual = stmt_set.explain(ExplainDetail.ESTIMATED_COST, ExplainDetail.CHANGELOG_MODE,
                                  ExplainDetail.JSON_EXECUTION_PLAN)
        self.assertIsInstance(actual, str)

    def test_register_java_function(self):
        t_env = self.t_env

        t_env.register_java_function(
            "scalar_func", "org.apache.flink.table.legacyutils.RichFunc0")

        t_env.register_java_function(
            "agg_func", "org.apache.flink.table.legacyutils.ByteMaxAggFunction")

        t_env.register_java_function(
            "table_func", "org.apache.flink.table.legacyutils.TableFunc1")

        actual = t_env.list_user_defined_functions()
        expected = ['scalar_func', 'agg_func', 'table_func']
        self.assert_equals(actual, expected)

    def test_load_module_twice(self):
        self.check_list_modules('core')
        self.check_list_full_modules(1, 'core')
        self.assertRaisesRegex(
            Py4JJavaError, "A module with name 'core' already exists",
            self.t_env.load_module, 'core', Module(
                get_gateway().jvm.org.apache.flink.table.module.CoreModule.INSTANCE))

    def test_unload_module_twice(self):
        self.t_env.unload_module('core')
        self.check_list_modules()
        self.check_list_full_modules(0)
        self.assertRaisesRegex(
            Py4JJavaError, "No module with name 'core' exists",
            self.t_env.unload_module, 'core')

    def test_use_duplicated_modules(self):
        self.assertRaisesRegex(
            Py4JJavaError, "Module 'core' appears more than once",
            self.t_env.use_modules, 'core', 'core')

    def test_use_nonexistent_module(self):
        self.assertRaisesRegex(
            Py4JJavaError, "No module with name 'dummy' exists",
            self.t_env.use_modules, 'core', 'dummy')

    def test_use_modules(self):
        # please do not change this order since ModuleMock depends on FunctionDefinitionMock
        _load_specific_flink_module_jars('/flink-table/flink-table-common')
        _load_specific_flink_module_jars('/flink-table/flink-table-api-java')

        self.t_env.load_module('x', Module(
            get_gateway().jvm.org.apache.flink.table.utils.ModuleMock("x")
        ))
        self.t_env.load_module('y', Module(
            get_gateway().jvm.org.apache.flink.table.utils.ModuleMock("y")
        ))
        self.check_list_modules('core', 'x', 'y')
        self.check_list_full_modules(3, 'core', 'x', 'y')

        self.t_env.use_modules('y', 'core')
        self.check_list_modules('y', 'core')
        self.check_list_full_modules(2, 'y', 'core', 'x')

    def check_list_modules(self, *expected_used_modules: str):
        self.assert_equals(self.t_env.list_modules(), list(expected_used_modules))

    def check_list_full_modules(self, used_module_cnt: int, *expected_loaded_modules: str):
        self.assert_equals(self.t_env.list_full_modules(),
                           [ModuleEntry(module,
                                        expected_loaded_modules.index(module) < used_module_cnt)
                            for module in expected_loaded_modules])

    def test_unload_and_load_module(self):
        t_env = self.t_env
        t_env.unload_module('core')
        t_env.load_module('core', Module(
            get_gateway().jvm.org.apache.flink.table.module.CoreModule.INSTANCE))
        table_result = t_env.execute_sql("select concat('unload', 'load') as test_module")
        self.assertEqual(table_result.get_result_kind(), ResultKind.SUCCESS_WITH_CONTENT)
        self.assert_equals(table_result.get_table_schema().get_field_names(), ['test_module'])

    def test_create_and_drop_java_function(self):
        t_env = self.t_env

        t_env.create_java_temporary_system_function(
            "scalar_func", "org.apache.flink.table.legacyutils.RichFunc0")
        t_env.create_java_function(
            "agg_func", "org.apache.flink.table.legacyutils.ByteMaxAggFunction")
        t_env.create_java_temporary_function(
            "table_func", "org.apache.flink.table.legacyutils.TableFunc1")
        self.assert_equals(t_env.list_user_defined_functions(),
                           ['scalar_func', 'agg_func', 'table_func'])

        t_env.drop_temporary_system_function("scalar_func")
        t_env.drop_function("agg_func")
        t_env.drop_temporary_function("table_func")
        self.assert_equals(t_env.list_user_defined_functions(), [])
