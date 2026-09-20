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
from datetime import datetime
from unittest.mock import patch

from py4j.protocol import Py4JJavaError

import pyflink.dataframe as pf
from pyflink.dataframe import DataType
from pyflink.dataframe.io import _build_filesystem_options
from pyflink.datastream import RuntimeExecutionMode
from pyflink.java_gateway import get_gateway
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.testing.test_case_utils import PyFlinkDataFrameUTTestCase


class FilesystemOptionsTests(unittest.TestCase):
    def test_format_options_are_namespaced_without_mutating_input(self):
        format_options = {
            "ignore-parse-errors": "true",
            "json.timestamp-format.standard": "ISO-8601",
        }
        original = dict(format_options)
        options = _build_filesystem_options(
            "/input", "json", {}, format_options
        )
        self.assertEqual(options, {
            "path": "/input",
            "format": "json",
            "json.ignore-parse-errors": "true",
            "json.timestamp-format.standard": "ISO-8601",
        })
        self.assertEqual(format_options, original)

    def test_format_options_cannot_override_filesystem_options(self):
        options = _build_filesystem_options(
            "/input", "json", {}, {"path": "/other", "format": "parquet"}
        )
        self.assertEqual(options["path"], "/input")
        self.assertEqual(options["format"], "json")

    def test_rejects_invalid_format_options(self):
        cases = [
            ([], TypeError, "options must be a dict"),
            ({1: "true"}, TypeError, "option keys must be strings"),
            ({"": "true"}, ValueError, "option keys must not be empty"),
            ({"ignore-parse-errors": True}, TypeError, "must have a string value"),
            ({"connector": "blackhole"}, ValueError, "connector argument"),
            ({"ignore-parse-errors": "true", "json.ignore-parse-errors": "false"},
             ValueError, "duplicate format option"),
        ]
        for options, error, message in cases:
            with self.subTest(options=options):
                with self.assertRaisesRegex(error, message):
                    _build_filesystem_options("/input", "json", {}, options)

    def test_readers_reject_invalid_paths_and_option_types(self):
        cases = [
            ({"path": None}, TypeError, "path must be a string"),
            ({"path": ""}, ValueError, "path must not be empty"),
            ({"monitor_interval": 60}, TypeError, "must have a string value"),
            ({"path_regex_pattern": 1}, TypeError, "must have a string value"),
        ]
        for reader in (pf.read_json, pf.read_parquet):
            for overrides, error, message in cases:
                with self.subTest(reader=reader.__name__, overrides=overrides):
                    arguments = {"path": "/input", "schema": {"id": DataType.int64()}}
                    arguments.update(overrides)
                    with self.assertRaisesRegex(error, message):
                        reader(**arguments)

    def test_writers_reject_invalid_arguments_before_execution(self):
        dataframe = pf.DataFrame(None)
        cases = [
            ({"path": None}, TypeError, "path must be a string"),
            ({"path": ""}, ValueError, "path must not be empty"),
            ({"mode": None}, TypeError, "mode must be a string"),
            ({"mode": "ignore"}, ValueError, "mode must be 'append' or 'overwrite'"),
            ({"rolling_policy_file_size": 128}, TypeError, "must have a string value"),
            ({"rolling_policy_rollover_interval": None}, TypeError, "must have a string value"),
            ({"rolling_policy_check_interval": 5}, TypeError, "must have a string value"),
            ({"partition_commit_delay": 1}, TypeError, "must have a string value"),
        ]
        for writer in (dataframe.write_json, dataframe.write_parquet):
            for overrides, error, message in cases:
                with self.subTest(writer=writer.__name__, overrides=overrides):
                    arguments = {"path": "/output"}
                    arguments.update(overrides)
                    with self.assertRaisesRegex(error, message):
                        writer(**arguments)
        with self.assertRaisesRegex(TypeError, "must have a string value"):
            dataframe.write_parquet("/output", compression=None)


class FilesystemIOTests(PyFlinkDataFrameUTTestCase):
    _SCHEMA = {"id": DataType.int64(), "name": DataType.string()}

    def test_readers_build_filesystem_descriptors(self):
        for file_format, reader in [("json", pf.read_json), ("parquet", pf.read_parquet)]:
            for continuous in (False, True):
                with self.subTest(file_format=file_format, continuous=continuous):
                    arguments = {}
                    expected = {"connector": "filesystem", "path": "/input",
                                "format": file_format}
                    if continuous:
                        arguments = {"monitor_interval": "60s", "path_regex_pattern": ".*data.*"}
                        expected.update({"source.monitor-interval": "60s",
                                         "source.path.regex-pattern": ".*data.*"})
                    with patch.object(self.t_env, "from_descriptor",
                                      wraps=self.t_env.from_descriptor) as from_descriptor:
                        dataframe = reader("/input", schema=self._SCHEMA, **arguments)
                    descriptor = from_descriptor.call_args.args[0]
                    self.assertEqual(dict(descriptor.get_options()), expected)
                    self.assert_dataframe_schema(dataframe, ["id", "name"])

    def test_json_reader_forwards_format_options(self):
        with patch.object(self.t_env, "from_descriptor",
                          wraps=self.t_env.from_descriptor) as from_descriptor:
            pf.read_json("/input", schema=self._SCHEMA,
                         format_options={"ignore-parse-errors": "true"})
        options = from_descriptor.call_args.args[0].get_options()
        self.assertEqual(options.get("json.ignore-parse-errors"), "true")

    def test_writers_build_descriptors_and_apply_write_mode(self):
        dataframe = pf.from_records([(1, "a")], schema=["id", "name"])
        for file_format in ("json", "parquet"):
            writer = getattr(dataframe, "write_" + file_format)
            for mode in (None, "append", "overwrite"):
                with self.subTest(file_format=file_format, mode=mode):
                    arguments = {} if mode is None else {"mode": mode}
                    with patch.object(dataframe._table, "execute_insert") as execute_insert:
                        self.assertIsNone(writer("/output", **arguments))
                    descriptor = execute_insert.call_args.args[0]
                    expected = {
                        "connector": "filesystem", "path": "/output", "format": file_format,
                        "sink.rolling-policy.file-size": "128mb",
                        "sink.rolling-policy.rollover-interval": "30min",
                        "sink.partition-commit.trigger": "process-time",
                        "sink.partition-commit.delay": "0s",
                    }
                    if file_format == "parquet":
                        expected["parquet.compression"] = "SNAPPY"
                    self.assertEqual(dict(descriptor.get_options()), expected)
                    self.assertIsNone(descriptor.get_schema())
                    self.assertEqual(execute_insert.call_args.kwargs,
                                     {"overwrite": mode != "append"})

    def test_writers_forward_custom_sink_options_and_wait_locally(self):
        dataframe = pf.from_records([(1,)], schema=["id"])
        for file_format in ("json", "parquet"):
            for target in ("local", "minicluster", "remote"):
                with self.subTest(file_format=file_format, target=target):
                    self.t_env.get_config().set("execution.target", target)
                    arguments = ({"compression": "GZIP"} if file_format == "parquet" else
                                 {"format_options": {"timestamp-format.standard": "ISO-8601"}})
                    with patch.object(dataframe._table, "execute_insert") as execute_insert:
                        getattr(dataframe, "write_" + file_format)(
                            "/output", mode="append", rolling_policy_file_size="64mb",
                            rolling_policy_rollover_interval="10min",
                            rolling_policy_check_interval="1min",
                            partition_commit_trigger="partition-time", partition_commit_delay="1h",
                            partition_commit_policy_kind="success-file", **arguments)
                    options = dict(execute_insert.call_args.args[0].get_options())
                    self.assertEqual(options["sink.rolling-policy.file-size"], "64mb")
                    self.assertEqual(options["sink.rolling-policy.rollover-interval"], "10min")
                    self.assertEqual(options["sink.rolling-policy.check-interval"], "1min")
                    self.assertEqual(options["sink.partition-commit.trigger"], "partition-time")
                    self.assertEqual(options["sink.partition-commit.delay"], "1h")
                    self.assertEqual(options["sink.partition-commit.policy.kind"], "success-file")
                    if file_format == "parquet":
                        self.assertEqual(options["parquet.compression"], "GZIP")
                    else:
                        self.assertEqual(options["json.timestamp-format.standard"], "ISO-8601")
                    if target in ("local", "minicluster"):
                        execute_insert.return_value.wait.assert_called_once_with()
                    else:
                        execute_insert.return_value.wait.assert_not_called()


class FilesystemIOITTests(PyFlinkDataFrameUTTestCase):
    _SCHEMA = {"id": DataType.int64(), "name": DataType.string()}

    def setUp(self):
        super().setUp()
        self.env.set_runtime_mode(RuntimeExecutionMode.BATCH)
        self.env.set_parallelism(1)
        self.t_env = StreamTableEnvironment.create(
            self.env, environment_settings=EnvironmentSettings.in_batch_mode()
        )
        pf.set_table_environment(self.t_env)

    def test_json_round_trip_overwrite_and_append(self):
        self._check_round_trip_overwrite_and_append("json")

    @unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                     'Hadoop libraries are required for Parquet format tests')
    def test_parquet_round_trip_overwrite_and_append(self):
        self._check_round_trip_overwrite_and_append("parquet")

    def _check_round_trip_overwrite_and_append(self, file_format):
        output_path = os.path.join(self.tempdir, file_format)
        reader = getattr(pf, "read_" + file_format)
        original = pf.from_records([(1, "a"), (2, None)], schema=["id", "name"])
        getattr(original, "write_" + file_format)(output_path)
        if file_format == "parquet":
            self._assert_parquet_compression(output_path, "SNAPPY")
        rows = reader(output_path, schema=self._SCHEMA).collect()
        self.assertEqual(sorted(tuple(row) for row in rows), [(1, "a"), (2, None)])

        replacement = pf.from_records([(3, "c")], schema=["id", "name"])
        options = {"compression": "GZIP"} if file_format == "parquet" else {}
        getattr(replacement, "write_" + file_format)(output_path, **options)
        if file_format == "parquet":
            self._assert_parquet_compression(output_path, "GZIP")
        rows = reader(output_path, schema=self._SCHEMA).collect()
        self.assertEqual([tuple(row) for row in rows], [(3, "c")])

        getattr(original, "write_" + file_format)(output_path, mode="append")
        rows = reader(output_path, schema=self._SCHEMA).collect()
        self.assertEqual(sorted(tuple(row) for row in rows),
                         [(1, "a"), (2, None), (3, "c")])

    def _assert_parquet_compression(self, output_path, expected_codec):
        jvm = get_gateway().jvm
        hadoop_conf = jvm.org.apache.hadoop.conf.Configuration()
        codecs = set()
        for filename in os.listdir(output_path):
            if filename.startswith("part-"):
                path = jvm.org.apache.hadoop.fs.Path(os.path.join(output_path, filename))
                footer = jvm.org.apache.parquet.hadoop.ParquetFileReader.readFooter(
                    hadoop_conf, path
                )
                for block in footer.getBlocks():
                    codecs.update(column.getCodec().name() for column in block.getColumns())
        self.assertEqual(codecs, {expected_codec})

    def test_json_streaming_append_and_overwrite_rejection(self):
        self._check_streaming_append_and_overwrite_rejection("json")

    @unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                     'Hadoop libraries are required for Parquet format tests')
    def test_parquet_streaming_append_and_overwrite_rejection(self):
        self._check_streaming_append_and_overwrite_rejection("parquet")

    def _check_streaming_append_and_overwrite_rejection(self, file_format):
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.t_env = StreamTableEnvironment.create(self.env)
        pf.set_table_environment(self.t_env)
        dataframe = pf.from_records([(1, "a")], schema=["id", "name"])
        output_path = os.path.join(self.tempdir, "stream-" + file_format)
        writer = getattr(dataframe, "write_" + file_format)
        with self.assertRaisesRegex(Py4JJavaError, "Streaming mode not support overwrite"):
            writer(output_path)
        writer(output_path, mode="append")
        rows = getattr(pf, "read_" + file_format)(
            output_path, schema=self._SCHEMA
        ).collect()
        self.assertEqual([tuple(row) for row in rows], [(1, "a")])

    def test_json_format_options_and_path_filter(self):
        input_path = os.path.join(self.tempdir, "json-input")
        os.makedirs(input_path)
        with open(os.path.join(input_path, "data.json"), "w", encoding="utf-8") as source:
            source.write('{"id":1,"name":"a"}\nnot-json\n{"id":2,"name":null}\n')
        with open(os.path.join(input_path, "excluded.json"), "w", encoding="utf-8") as source:
            source.write('{"id":99,"name":"excluded"}\n')
        rows = pf.read_json(
            input_path, schema=self._SCHEMA, path_regex_pattern=".*data[.]json",
            format_options={"ignore-parse-errors": "true"},
        ).collect()
        self.assertEqual(sorted(tuple(row) for row in rows), [(1, "a"), (2, None)])

    def test_json_timestamp_format_options_round_trip(self):
        output_path = os.path.join(self.tempdir, "json-timestamp")
        timestamp = datetime(2026, 1, 2, 3, 4, 5)
        dataframe = pf.sql("SELECT TIMESTAMP '2026-01-02 03:04:05' AS ts")
        dataframe.write_json(
            output_path, format_options={"timestamp-format.standard": "ISO-8601"}
        )
        records = []
        for filename in os.listdir(output_path):
            if filename.startswith("part-"):
                with open(os.path.join(output_path, filename), encoding="utf-8") as output:
                    records.extend(json.loads(line) for line in output)
        self.assertEqual(records, [{"ts": timestamp.isoformat()}])

        rows = pf.read_json(
            output_path, schema={"ts": DataType.timestamp()},
            format_options={"json.timestamp-format.standard": "ISO-8601"},
        ).collect()
        self.assertEqual([tuple(row) for row in rows], [(timestamp,)])


if __name__ == "__main__":
    unittest.main()
