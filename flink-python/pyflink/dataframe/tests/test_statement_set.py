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

import json
import os
import unittest
from unittest.mock import patch

import pyflink.dataframe as pf
from pyflink.datastream import RuntimeExecutionMode
from pyflink.table import (
    EnvironmentSettings,
    StatementSet,
    StreamTableEnvironment,
    TableEnvironment,
)
from pyflink.testing.test_case_utils import PyFlinkDataFrameUTTestCase


class StatementSetTests(PyFlinkDataFrameUTTestCase):
    def test_factory_uses_dataframe_environment(self):
        statement_set = pf.create_statement_set()

        self.assertIsInstance(statement_set, StatementSet)
        self.assertIs(statement_set._t_env, self.t_env)
        self.assertIsNot(statement_set, pf.create_statement_set())
        self.assertIn("create_statement_set", pf.__all__)

    def test_factory_creates_default_environment(self):
        pf.set_table_environment(None)

        statement_set = pf.create_statement_set()

        self.assertIs(statement_set._t_env, pf.get_table_environment())
        self.assertIsNotNone(pf.get_table_environment())

    def _writers(self, dataframe):
        return [
            (dataframe.write_generic, ("blackhole",), {"options": {}}),
            (dataframe.write_json, (os.path.join(self.tempdir, "json"),), {}),
            (dataframe.write_parquet, (os.path.join(self.tempdir, "parquet"),), {}),
            (dataframe.write_catalog_table, ("sink_table",), {}),
        ]

    def test_writers_stage_without_execution(self):
        dataframe = pf.from_records([(1, "a")], schema=["id", "name"])
        for writer, arguments, options in self._writers(dataframe):
            with self.subTest(writer=writer.__name__):
                statement_set = pf.create_statement_set()
                with patch.object(statement_set, "add_insert") as add_insert, \
                        patch.object(statement_set, "execute") as execute, \
                        patch.object(dataframe._table, "execute_insert") as execute_insert:
                    result = writer(*arguments, statement_set=statement_set, **options)

                self.assertIsNone(result)
                add_insert.assert_called_once()
                self.assertIs(add_insert.call_args.args[1], dataframe._table)
                self.assertEqual(add_insert.call_args.kwargs, {"overwrite": False})
                if writer.__name__ == "write_catalog_table":
                    self.assertEqual(add_insert.call_args.args[0], "sink_table")
                else:
                    descriptor = add_insert.call_args.args[0]
                    connector = "blackhole" if writer.__name__ == "write_generic" else "filesystem"
                    self.assertEqual(descriptor.get_options()["connector"], connector)
                execute.assert_not_called()
                execute_insert.assert_not_called()

    def test_staged_writes_preserve_overwrite_and_partition_options(self):
        dataframe = pf.from_records([(1, "a")], schema=["id", "name"])
        for writer in (dataframe.write_json, dataframe.write_parquet):
            with self.subTest(writer=writer.__name__):
                statement_set = pf.create_statement_set()
                with patch.object(statement_set, "add_insert") as add_insert:
                    writer("/output", mode="overwrite", partition_by="name",
                           statement_set=statement_set)

                descriptor = add_insert.call_args.args[0]
                self.assertEqual(list(descriptor.get_partition_keys()), ["name"])
                self.assertEqual(add_insert.call_args.kwargs, {"overwrite": True})

        statement_set = pf.create_statement_set()
        with patch.object(statement_set, "add_insert") as add_insert:
            dataframe.write_catalog_table("sink_table", overwrite=True,
                                          statement_set=statement_set)
        self.assertEqual(add_insert.call_args.kwargs, {"overwrite": True})

    def test_writers_reject_invalid_statement_set(self):
        dataframe = pf.from_records([(1, "a")], schema=["id", "name"])
        for writer, arguments, options in self._writers(dataframe):
            with self.subTest(writer=writer.__name__):
                with patch.object(dataframe._table, "execute_insert") as execute_insert:
                    with self.assertRaisesRegex(TypeError, "statement_set must be a StatementSet"):
                        writer(*arguments, statement_set=object(), **options)
                execute_insert.assert_not_called()

    def test_writers_reject_different_environments(self):
        dataframe = pf.from_records([(1, "a")], schema=["id", "name"])
        other = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        statement_set = other.create_statement_set()
        for writer, arguments, options in self._writers(dataframe):
            with self.subTest(writer=writer.__name__):
                with patch.object(statement_set, "add_insert") as add_insert:
                    with self.assertRaisesRegex(ValueError, "same TableEnvironment"):
                        writer(*arguments, statement_set=statement_set, **options)
                add_insert.assert_not_called()

    def test_explain_keeps_staged_writes_without_execution(self):
        dataframe = pf.from_records([(1,)], schema=["id"])
        for name in ("sink_one", "sink_two"):
            self.t_env.execute_sql(
                f"CREATE TABLE {name} (id BIGINT) WITH ('connector' = 'blackhole')"
            )
        statement_set = pf.create_statement_set()
        with patch.object(dataframe._table, "execute_insert") as execute_insert, \
                patch.object(statement_set, "execute") as execute:
            dataframe.write_catalog_table("sink_one", statement_set=statement_set)
            dataframe.write_catalog_table("sink_two", statement_set=statement_set)
            for _ in range(2):
                plan = statement_set.explain()
                self.assertIn("sink_one", plan)
                self.assertIn("sink_two", plan)

        execute_insert.assert_not_called()
        execute.assert_not_called()

    def test_staged_catalog_errors_use_existing_translation(self):
        dataframe = pf.from_records([(1,)], schema=["id"])
        statement_set = pf.create_statement_set()
        with self.assertRaises(ValueError):
            dataframe.write_catalog_table("missing_sink", statement_set=statement_set)


class StatementSetITTests(PyFlinkDataFrameUTTestCase):
    def test_multi_sink_batch_write(self):
        self._check_multi_sink_write(RuntimeExecutionMode.BATCH)

    def test_multi_sink_streaming_write(self):
        self._check_multi_sink_write(RuntimeExecutionMode.STREAMING)

    def _check_multi_sink_write(self, mode):
        self.env.set_runtime_mode(mode)
        self.env.set_parallelism(1)
        settings = (EnvironmentSettings.in_batch_mode() if mode == RuntimeExecutionMode.BATCH
                    else EnvironmentSettings.in_streaming_mode())
        self.t_env = StreamTableEnvironment.create(self.env, environment_settings=settings)
        pf.set_table_environment(self.t_env)
        json_path = os.path.join(self.tempdir, "json-" + mode.name)
        csv_path = os.path.join(self.tempdir, "csv-" + mode.name)
        self.t_env.execute_sql(
            "CREATE TABLE csv_sink (id BIGINT, name STRING) WITH ("
            "'connector' = 'filesystem', "
            f"'path' = '{csv_path}', 'format' = 'csv')"
        )
        dataframe = pf.from_records([(1, "a"), (2, "b"), (3, "c")], schema=["id", "name"])
        dataframe = dataframe.filter(pf.col("id") > 1)
        statement_set = pf.create_statement_set()
        dataframe.write_json(json_path, statement_set=statement_set)
        dataframe.write_catalog_table("csv_sink", statement_set=statement_set)
        self.assertFalse(os.path.exists(json_path))
        self.assertFalse(os.path.exists(csv_path))

        result = statement_set.execute()
        self.assertIsNotNone(result.get_job_client())
        result.wait()

        json_rows = [json.loads(line) for line in self._read_lines(json_path)]
        self.assertEqual(sorted((row["id"], row["name"]) for row in json_rows),
                         [(2, "b"), (3, "c")])
        self.assertEqual(sorted(self._read_lines(csv_path)), ["2,b", "3,c"])

    @staticmethod
    def _read_lines(path):
        lines = []
        for directory, _, files in os.walk(path):
            for name in files:
                if not name.startswith((".", "_")):
                    with open(os.path.join(directory, name), encoding="utf-8") as output:
                        lines.extend(line.rstrip("\n") for line in output)
        return lines


if __name__ == "__main__":
    unittest.main()
