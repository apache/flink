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
from pyflink.testing.test_case_utils import (
    PyFlinkDataFrameUTTestCase,
    PyFlinkStreamDataFrameTestCase,
)


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


class GenericIOITTests(PyFlinkStreamDataFrameTestCase):
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
