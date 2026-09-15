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
import unittest
from unittest.mock import MagicMock, patch

import pyflink.dataframe as pf
from pyflink.dataframe import DataType
from pyflink.table import DataTypes
from pyflink.testing.test_case_utils import (
    PyFlinkDataFrameUTTestCase,
    PyFlinkStreamDataFrameTestCase,
)
from py4j.protocol import Py4JJavaError


class GenericIOTests(PyFlinkDataFrameUTTestCase):
    _SCHEMA = {"id": DataType.int64(), "name": DataType.string()}

    def test_read_generic_builds_source_descriptor(self):
        with patch.object(
            self.t_env,
            "from_descriptor",
            wraps=self.t_env.from_descriptor,
        ) as from_descriptor:
            dataframe = pf.read_generic(
                "datagen",
                schema={
                    "id": DataType.int64(),
                    "ts_millis": DataType.int64(),
                },
                options={"number-of-rows": "1"},
                computed_columns={
                    "event_time": "TO_TIMESTAMP_LTZ(ts_millis, 3)"
                },
                watermark=(
                    "event_time",
                    "event_time - INTERVAL '5' SECOND",
                ),
            )

        descriptor = from_descriptor.call_args.args[0]
        self.assertEqual(descriptor.get_options().get("connector"), "datagen")
        self.assertEqual(descriptor.get_options().get("number-of-rows"), "1")
        self.assert_dataframe_schema(
            dataframe,
            ["id", "ts_millis", "event_time"],
        )
        watermark_specs = dataframe._table.get_resolved_schema().get_watermark_specs()
        self.assertEqual(len(watermark_specs), 1)
        self.assertEqual(
            watermark_specs[0].get_rowtime_attribute(), "event_time"
        )

    def test_read_generic_rejects_invalid_arguments(self):
        cases = [
            (
                "connector type",
                {"connector": None},
                TypeError,
                "connector must be a string",
            ),
            (
                "empty connector",
                {"connector": ""},
                ValueError,
                "connector must not be empty",
            ),
            (
                "schema type",
                {"schema": []},
                TypeError,
                "schema must be a dict",
            ),
            (
                "empty schema",
                {"schema": {}},
                ValueError,
                "schema must not be empty",
            ),
            (
                "schema name",
                {"schema": {1: DataType.int64()}},
                TypeError,
                "schema column names must be strings",
            ),
            (
                "empty schema name",
                {"schema": {"": DataType.int64()}},
                ValueError,
                "schema column names must not be empty",
            ),
            (
                "schema data type",
                {"schema": {"id": object()}},
                TypeError,
                "must use a DataType value",
            ),
            (
                "options type",
                {"options": []},
                TypeError,
                "options must be a dict",
            ),
            (
                "option name",
                {"options": {"": "value"}},
                ValueError,
                "option keys must not be empty",
            ),
            (
                "option key type",
                {"options": {1: "value"}},
                TypeError,
                "option keys must be strings",
            ),
            (
                "reserved connector option",
                {"options": {"connector": "filesystem"}},
                ValueError,
                "connector argument",
            ),
            (
                "option value",
                {"options": {"rows": 1}},
                TypeError,
                "must have a string value",
            ),
            (
                "computed columns type",
                {"computed_columns": []},
                TypeError,
                "computed_columns must be a dict",
            ),
            (
                "duplicate computed column",
                {"computed_columns": {"id": "id + 1"}},
                ValueError,
                "conflicts with a physical column",
            ),
            (
                "computed column name type",
                {"computed_columns": {1: "id + 1"}},
                TypeError,
                "computed column names must be strings",
            ),
            (
                "empty computed column name",
                {"computed_columns": {"": "id + 1"}},
                ValueError,
                "computed column names must not be empty",
            ),
            (
                "computed expression type",
                {"computed_columns": {"computed": 1}},
                TypeError,
                "must use a string expression",
            ),
            (
                "empty computed expression",
                {"computed_columns": {"computed": ""}},
                ValueError,
                "expression must not be empty",
            ),
            (
                "watermark shape",
                {"watermark": ("id",)},
                TypeError,
                "watermark must be a tuple",
            ),
            (
                "watermark type",
                {"watermark": ("id", 1)},
                TypeError,
                "watermark column and expression must be strings",
            ),
            (
                "watermark value",
                {"watermark": ("", "id")},
                ValueError,
                "must not be empty",
            ),
        ]

        for name, overrides, error_type, message in cases:
            with self.subTest(name=name):
                arguments = {
                    "connector": "datagen",
                    "schema": self._SCHEMA,
                    "options": {},
                }
                arguments.update(overrides)
                with self.assertRaisesRegex(error_type, message):
                    pf.read_generic(**arguments)

    def test_write_generic_builds_sink_descriptor(self):
        dataframe = pf.from_records([(1, "a")], schema=["id", "name"])

        with patch.object(dataframe._table, "execute_insert") as execute_insert:
            execute_insert.return_value = MagicMock()
            result = dataframe.write_generic(
                "blackhole", options={"sink.parallelism": "1"}
            )

        descriptor = execute_insert.call_args.args[0]
        self.assertIsNone(result)
        self.assertIsNone(descriptor.get_schema())
        self.assertEqual(descriptor.get_options().get("connector"), "blackhole")
        self.assertEqual(descriptor.get_options().get("sink.parallelism"), "1")
        self.assertEqual(execute_insert.call_args.kwargs, {"overwrite": False})

    def test_write_generic_waits_for_local_and_minicluster_execution(self):
        dataframe = pf.from_records([(1,)], schema=["id"])

        for execution_target, waits in [
            ("local", True),
            ("minicluster", True),
            ("remote", False),
        ]:
            with self.subTest(execution_target=execution_target):
                table_result = MagicMock()
                table_config = MagicMock()
                table_config.get.return_value = execution_target
                with patch.object(
                    dataframe._table,
                    "execute_insert",
                    return_value=table_result,
                ), patch.object(
                    dataframe._table._t_env,
                    "get_config",
                    return_value=table_config,
                ):
                    dataframe.write_generic("blackhole", options={})

                if waits:
                    table_result.wait.assert_called_once_with()
                else:
                    table_result.wait.assert_not_called()

    def test_write_generic_uses_shared_validation(self):
        dataframe = pf.from_records([(1,)], schema=["id"])
        cases = [
            (None, {}, TypeError, "connector must be a string"),
            ("", {}, ValueError, "connector must not be empty"),
            ("blackhole", [], TypeError, "options must be a dict"),
            (
                "blackhole",
                {"connector": "filesystem"},
                ValueError,
                "connector argument",
            ),
            (
                "blackhole",
                {"sink.parallelism": 1},
                TypeError,
                "must have a string value",
            ),
        ]

        for connector, options, error_type, message in cases:
            with self.subTest(connector=connector, options=options):
                with self.assertRaisesRegex(error_type, message):
                    dataframe.write_generic(connector, options=options)

    def test_read_generic_translates_flink_errors(self):
        with self.assertRaises(ValueError) as context:
            pf.read_generic(
                "datagen",
                schema=self._SCHEMA,
                options={},
                computed_columns={"bad": "NO_SUCH_FUNCTION(id)"},
            )
        self.assertEqual(
            str(context.exception), "Invalid expression for computed column 'bad'."
        )
        self.assertIsInstance(context.exception.__cause__, Py4JJavaError)

    def test_write_generic_translates_flink_errors(self):
        dataframe = pf.from_records([(1,)], schema=["id"])

        with self.assertRaises(ValueError) as context:
            dataframe.write_generic("no_such_connector", options={})
        self.assertIn("no_such_connector", str(context.exception))
        self.assertIsInstance(context.exception.__cause__, Py4JJavaError)


class CatalogTableIOTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        pf.create_catalog("my_catalog", {"type": "generic_in_memory"})
        self.t_env.execute_sql("CREATE DATABASE my_catalog.my_database")
        self.t_env.execute_sql(
            "CREATE TABLE my_catalog.my_database.events ("
            "  id BIGINT, name STRING"
            ") WITH ('connector' = 'datagen', 'number-of-rows' = '1')"
        )

    def test_read_catalog_table_resolves_paths(self):
        cases = [
            ("default_catalog", "default_database", "my_catalog.my_database.events"),
            ("my_catalog", "default", "my_database.events"),
            ("my_catalog", "my_database", "events"),
        ]

        for catalog, database, path in cases:
            with self.subTest(path=path):
                pf.use_catalog(catalog)
                pf.use_database(database)
                with patch.object(
                    self.t_env, "from_path", wraps=self.t_env.from_path
                ) as from_path:
                    dataframe = pf.read_catalog_table(path)

                from_path.assert_called_once_with(path)
                self.assertIsInstance(dataframe, pf.DataFrame)
                self.assert_dataframe_schema(
                    dataframe,
                    ["id", "name"],
                    [DataTypes.BIGINT(), DataTypes.STRING()],
                )

    def test_read_catalog_table_rejects_invalid_path(self):
        with self.assertRaisesRegex(TypeError, "path must be a string"):
            pf.read_catalog_table(None)
        with self.assertRaisesRegex(ValueError, "path must not be empty"):
            pf.read_catalog_table("")

    def test_read_catalog_table_translates_flink_errors(self):
        with self.assertRaises(ValueError) as context:
            pf.read_catalog_table("my_catalog.my_database.missing")
        self.assertEqual(
            str(context.exception),
            "Table `my_catalog`.`my_database`.`missing` was not found.",
        )
        self.assertIsInstance(context.exception.__cause__, Py4JJavaError)

        with self.assertRaises(ValueError) as context:
            pf.read_catalog_table("too.many.path.parts")
        self.assertEqual(
            str(context.exception), "Invalid SQL identifier too.many.path.parts."
        )
        self.assertIsInstance(context.exception.__cause__, Py4JJavaError)

    def test_write_catalog_table_translates_flink_errors(self):
        dataframe = pf.from_records([(1, "a")], schema=["id", "name"])

        with self.assertRaises(ValueError) as context:
            dataframe.write_catalog_table("my_catalog.my_database.missing")
        self.assertIn(
            "Cannot find table '`my_catalog`.`my_database`.`missing`'",
            str(context.exception),
        )

        self.t_env.execute_sql(
            "CREATE TABLE my_catalog.my_database.sink (id BIGINT) "
            "WITH ('connector' = 'blackhole')"
        )
        with self.assertRaises(ValueError) as context:
            dataframe.write_catalog_table("my_catalog.my_database.sink")
        self.assertIn("Column types of query result and sink", str(context.exception))
        self.assertIsInstance(context.exception.__cause__, Py4JJavaError)

    def test_write_catalog_table_passes_path_and_overwrite(self):
        dataframe = pf.from_records([(1, "a")], schema=["id", "name"])

        for overwrite in [False, True]:
            with self.subTest(overwrite=overwrite):
                with patch.object(
                    dataframe._table, "execute_insert", return_value=MagicMock()
                ) as execute_insert:
                    result = dataframe.write_catalog_table(
                        "my_catalog.my_database.events", overwrite=overwrite
                    )

                self.assertIsNone(result)
                execute_insert.assert_called_once_with(
                    "my_catalog.my_database.events", overwrite=overwrite
                )

    def test_write_catalog_table_waits_for_local_and_minicluster_execution(self):
        dataframe = pf.from_records([(1,)], schema=["id"])

        for execution_target, waits in [
            ("local", True),
            ("minicluster", True),
            ("remote", False),
        ]:
            with self.subTest(execution_target=execution_target):
                table_result = MagicMock()
                table_config = MagicMock()
                table_config.get.return_value = execution_target
                with patch.object(
                    dataframe._table,
                    "execute_insert",
                    return_value=table_result,
                ), patch.object(
                    dataframe._table._t_env,
                    "get_config",
                    return_value=table_config,
                ):
                    dataframe.write_catalog_table("events")

                if waits:
                    table_result.wait.assert_called_once_with()
                else:
                    table_result.wait.assert_not_called()

    def test_write_catalog_table_rejects_invalid_arguments(self):
        dataframe = pf.from_records([(1,)], schema=["id"])
        cases = [
            (None, False, TypeError, "path must be a string"),
            ("", False, ValueError, "path must not be empty"),
            ("events", "yes", TypeError, "overwrite must be a bool"),
        ]

        for path, overwrite, error_type, message in cases:
            with self.subTest(path=path, overwrite=overwrite):
                with self.assertRaisesRegex(error_type, message):
                    dataframe.write_catalog_table(path, overwrite=overwrite)


class GenericIOITTests(PyFlinkStreamDataFrameTestCase):
    def test_catalog_table_round_trip(self):
        output_path = os.path.join(self.tempdir, "catalog_output")
        pf.create_catalog("my_catalog", {"type": "generic_in_memory"})
        self.addCleanup(pf.use_catalog, pf.get_current_catalog())
        self.t_env.execute_sql("CREATE DATABASE my_catalog.my_database")
        self.t_env.execute_sql(
            "CREATE TABLE my_catalog.my_database.events ("
            "  id BIGINT, name STRING"
            ") WITH ("
            "  'connector' = 'filesystem',"
            f"  'path' = '{output_path}',"
            "  'format' = 'csv'"
            ")"
        )

        pf.use_catalog("my_catalog")
        pf.use_database("my_database")
        source = pf.from_records([(1, "a"), (2, "b"), (3, "c")], schema=["id", "name"])
        source.write_catalog_table("events")

        result = pf.read_catalog_table("events").collect()
        self.assertEqual(
            sorted(tuple(row) for row in result), [(1, "a"), (2, "b"), (3, "c")]
        )

    def test_filesystem_csv_round_trip(self):
        input_path = os.path.join(self.tempdir, "input.csv")
        with open(input_path, "w", encoding="utf-8") as input_file:
            input_file.write("1,a\n2,b\n3,c\n")

        source = pf.read_generic(
            "filesystem",
            schema={
                "id": DataType.int64(),
                "name": DataType.string(),
            },
            options={"path": input_path, "format": "csv"},
        )

        output_path = os.path.join(self.tempdir, "output")
        source.write_generic(
            "filesystem",
            options={"path": output_path, "format": "csv"},
        )

        output_lines = []
        for file_name in os.listdir(output_path):
            file_path = os.path.join(output_path, file_name)
            if os.path.isfile(file_path) and not file_name.startswith((".", "_")):
                with open(file_path, encoding="utf-8") as output_file:
                    output_lines.extend(line.rstrip("\n") for line in output_file)

        self.assertEqual(sorted(output_lines), ["1,a", "2,b", "3,c"])


if __name__ == "__main__":
    unittest.main()
