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
import time
import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from py4j.protocol import Py4JJavaError

import pyflink.dataframe as pf
from pyflink.common import JobStatus
from pyflink.dataframe import DataType
from pyflink.dataframe.io import _build_filesystem_options
from pyflink.datastream import RuntimeExecutionMode
from pyflink.java_gateway import get_gateway
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, TableResult
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

    def test_connector_options_are_validated_without_mutating_input(self):
        options = {"source.monitor-interval": "1s", "source.report-statistics": "NONE"}
        original = dict(options)
        actual = _build_filesystem_options(
            "/input", "json", {"source.monitor-interval": "1s"},
            connector_options=options,
        )
        self.assertEqual(actual["source.report-statistics"], "NONE")
        self.assertEqual(options, original)
        for invalid in ({"path": "/other"}, {"format": "parquet"},
                        {"connector": "blackhole"}, {"json.ignore-parse-errors": "true"}):
            with self.subTest(options=invalid):
                with self.assertRaises(ValueError):
                    _build_filesystem_options("/input", "json", {}, connector_options=invalid)
        for invalid in ([], {1: "value"}, {"source.monitor-interval": 1}):
            with self.subTest(options=invalid):
                with self.assertRaises(TypeError):
                    _build_filesystem_options("/input", "json", {}, connector_options=invalid)

    def test_conflicting_explicit_options_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "conflicting values.*source.monitor-interval"):
            _build_filesystem_options(
                "/input", "json", {"source.monitor-interval": "1s"},
                connector_options={"source.monitor-interval": "2s"},
            )
        for prefix in ("", "parquet."):
            with self.subTest(prefix=prefix):
                options = {prefix + "compression": "GZIP"}
                with self.assertRaisesRegex(ValueError, "conflicting values.*parquet.compression"):
                    _build_filesystem_options(
                        "/input", "parquet", {}, options,
                        format_parameters={"compression": "SNAPPY"},
                    )
                actual = _build_filesystem_options(
                    "/input", "parquet", {}, options,
                    format_parameters={"compression": "GZIP"},
                )
                self.assertEqual(actual["parquet.compression"], "GZIP")

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
            ({"rolling_policy_inactivity_interval": 1}, TypeError, "must have a string value"),
            ({"rolling_policy_check_interval": 5}, TypeError, "must have a string value"),
            ({"partition_commit_delay": 1}, TypeError, "must have a string value"),
            ({"sink_parallelism": True}, TypeError, "sink_parallelism must be an int"),
            ({"sink_parallelism": "1"}, TypeError, "sink_parallelism must be an int"),
            ({"partition_by": [1]}, TypeError, "partition_by must be a string"),
            ({"partition_by": []}, ValueError, "partition_by must not be empty"),
            ({"partition_by": ""}, ValueError, "column names must not be empty"),
            ({"partition_by": ["id", "id"]}, ValueError, "must not be duplicated"),
        ]
        for writer in (dataframe.write_json, dataframe.write_parquet):
            for overrides, error, message in cases:
                with self.subTest(writer=writer.__name__, overrides=overrides):
                    arguments = {"path": "/output"}
                    arguments.update(overrides)
                    with self.assertRaisesRegex(error, message):
                        writer(**arguments)
        with self.assertRaisesRegex(TypeError, "must have a string value"):
            dataframe.write_parquet("/output", compression=1)
        with self.assertRaisesRegex(TypeError, "utc_timezone must be a bool"):
            dataframe.write_parquet("/output", utc_timezone="true")
        with self.assertRaisesRegex(TypeError, "ignore_parse_errors must be a bool"):
            pf.read_json("/input", schema={"id": DataType.int64()}, ignore_parse_errors=1)


class FilesystemIOTests(PyFlinkDataFrameUTTestCase):
    _SCHEMA = {"id": DataType.int64(), "name": DataType.string()}

    def test_readers_declare_computed_columns_and_watermarks(self):
        for reader in (pf.read_json, pf.read_parquet):
            with self.subTest(reader=reader.__name__):
                dataframe = reader(
                    "/input", schema={"ts_millis": DataType.int64()},
                    computed_columns={"ts": "TO_TIMESTAMP_LTZ(ts_millis, 3)"},
                    watermark=("ts", "ts - INTERVAL '5' SECOND"),
                )
                resolved = dataframe.to_table().get_resolved_schema()
                self.assertEqual(list(resolved.get_column_names()), ["ts_millis", "ts"])
                self.assertEqual(len(resolved.get_watermark_specs()), 1)
                physical = reader(
                    "/input", schema={"ts": DataType.timestamp(3)},
                    watermark=("ts", "ts - INTERVAL '5' SECOND"),
                )
                self.assertEqual(len(physical.to_table().get_resolved_schema()
                                     .get_watermark_specs()), 1)

    def test_readers_forward_connector_and_format_parameters(self):
        cases = [
            (pf.read_json, {"ignore_parse_errors": True, "fail_on_missing_field": False,
                            "timestamp_format": "ISO-8601"},
             {"json.ignore-parse-errors": "true", "json.fail-on-missing-field": "false",
              "json.timestamp-format.standard": "ISO-8601"}),
            (pf.read_parquet, {"utc_timezone": True}, {"parquet.utc-timezone": "true"}),
        ]
        for reader, parameters, expected in cases:
            with self.subTest(reader=reader.__name__):
                with patch.object(self.t_env, "from_descriptor",
                                  wraps=self.t_env.from_descriptor) as from_descriptor:
                    reader("/input", schema=self._SCHEMA, **parameters,
                           connector_options={"source.report-statistics": "NONE"})
                options = dict(from_descriptor.call_args.args[0].get_options())
                self.assertEqual(options["source.report-statistics"], "NONE")
                for key, value in expected.items():
                    self.assertEqual(options[key], value)

    def test_invalid_partition_columns_are_rejected_by_the_table_api(self):
        for reader in (pf.read_json, pf.read_parquet):
            with self.subTest(reader=reader.__name__):
                with self.assertRaisesRegex(Py4JJavaError, "missing"):
                    reader("/input", schema=self._SCHEMA, partition_by="missing")
        dataframe = pf.from_records([(1,)], schema=["id"])
        for writer in (dataframe.write_json, dataframe.write_parquet):
            with self.subTest(writer=writer.__name__):
                with self.assertRaisesRegex(Py4JJavaError, "missing"):
                    writer("/output", partition_by="missing")

    def test_writers_accept_raw_options_without_overriding_them_with_defaults(self):
        dataframe = pf.from_records([(1,)], schema=["id"])
        with patch.object(dataframe._table, "execute_insert") as execute_insert:
            dataframe.write_parquet(
                "/output", format_options={"compression": "GZIP"},
                connector_options={"sink.rolling-policy.file-size": "64mb"},
            )
        options = dict(execute_insert.call_args.args[0].get_options())
        self.assertEqual(options["parquet.compression"], "GZIP")
        self.assertEqual(options["sink.rolling-policy.file-size"], "64mb")

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
                                 {"timestamp_format": "ISO-8601"})
                    with patch.object(dataframe._table, "execute_insert") as execute_insert:
                        getattr(dataframe, "write_" + file_format)(
                            "/output", mode="append", rolling_policy_file_size="64mb",
                            rolling_policy_rollover_interval="10min",
                            rolling_policy_inactivity_interval="2min", sink_parallelism=2,
                            rolling_policy_check_interval="1min",
                            partition_commit_trigger="partition-time", partition_commit_delay="1h",
                            partition_commit_policy_kind="success-file", **arguments)
                    options = dict(execute_insert.call_args.args[0].get_options())
                    self.assertEqual(options["sink.rolling-policy.file-size"], "64mb")
                    self.assertEqual(options["sink.rolling-policy.rollover-interval"], "10min")
                    self.assertEqual(options["sink.rolling-policy.inactivity-interval"], "2min")
                    self.assertEqual(options["sink.parallelism"], "2")
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

    def test_json_partitioned_round_trip_and_overwrite(self):
        self._check_partitioned_round_trip_and_overwrite("json")

    @unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                     'Hadoop libraries are required for Parquet format tests')
    def test_parquet_partitioned_round_trip_and_overwrite(self):
        self._check_partitioned_round_trip_and_overwrite("parquet")

    def _check_partitioned_round_trip_and_overwrite(self, file_format):
        output_path = os.path.join(self.tempdir, "partitioned-" + file_format)
        reader = getattr(pf, "read_" + file_format)
        original = pf.from_records([(1, "a"), (2, "b")], schema=["id", "name"])
        getattr(original, "write_" + file_format)(output_path, partition_by="name")
        self.assertTrue(os.path.isdir(os.path.join(output_path, "name=a")))
        self.assertTrue(os.path.isdir(os.path.join(output_path, "name=b")))
        rows = reader(output_path, schema=self._SCHEMA, partition_by="name").collect()
        self.assertEqual(sorted(tuple(row) for row in rows), [(1, "a"), (2, "b")])

        replacement = pf.from_records([(3, "a")], schema=["id", "name"])
        getattr(replacement, "write_" + file_format)(output_path, partition_by=["name"])
        rows = reader(output_path, schema=self._SCHEMA, partition_by=["name"]).collect()
        self.assertEqual(sorted(tuple(row) for row in rows), [(2, "b"), (3, "a")])

        additional = pf.from_records([(4, "a/b=c")], schema=["id", "name"])
        getattr(additional, "write_" + file_format)(
            output_path, partition_by="name", mode="append")
        rows = reader(output_path, schema=self._SCHEMA, partition_by="name").collect()
        self.assertEqual(sorted(tuple(row) for row in rows),
                         [(2, "b"), (3, "a"), (4, "a/b=c")])

        default_path = os.path.join(self.tempdir, "default-partition-" + file_format)
        default_values = pf.from_records([(5, None), (6, "")], schema=["id", "name"])
        getattr(default_values, "write_" + file_format)(
            default_path, partition_by="name",
            connector_options={"partition.default-name": "unspecified"},
        )
        self.assertEqual([name for name in os.listdir(default_path) if not name.startswith(".")],
                         ["name=unspecified"])

    def test_partition_column_order_and_payload(self):
        output_path = os.path.join(self.tempdir, "partition-order")
        dataframe = pf.from_records([(1, "a", "b")], schema=["id", "first", "second"])
        dataframe.write_json(output_path, partition_by=["second", "first"])
        partition_path = os.path.join(output_path, "second=b", "first=a")
        self.assertTrue(os.path.isdir(partition_path))
        records = []
        for filename in os.listdir(partition_path):
            if filename.startswith("part-"):
                with open(os.path.join(partition_path, filename), encoding="utf-8") as output:
                    records.extend(json.loads(line) for line in output)
        self.assertEqual(records, [{"id": 1}])

    def test_json_continuous_discovery_and_partition_commit(self):
        self._check_continuous_discovery_and_partition_commit("json")

    def test_json_partition_time_commit(self):
        self._check_continuous_discovery_and_partition_commit("json", "partition-time")

    def test_json_partition_time_commit_with_local_timezone(self):
        self._check_continuous_discovery_and_partition_commit(
            "json", "partition-time", "Asia/Shanghai")

    @unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                     'Hadoop libraries are required for Parquet format tests')
    def test_parquet_continuous_discovery_and_partition_commit(self):
        self._check_continuous_discovery_and_partition_commit("parquet")

    def _check_continuous_discovery_and_partition_commit(
        self, file_format, partition_commit_trigger="process-time", local_timezone="UTC"
    ):
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.env.enable_checkpointing(100)
        self.t_env = StreamTableEnvironment.create(self.env)
        self.t_env.get_config().set_local_timezone(local_timezone)
        pf.set_table_environment(self.t_env)
        suffix = self._testMethodName
        input_path = os.path.join(self.tempdir, "continuous-input-" + suffix)
        output_path = os.path.join(self.tempdir, "continuous-output-" + suffix)
        os.makedirs(input_path)
        partition_value = "2026-01-01"
        # Keep the watermark near the partition boundary to expose time zone mismatches.
        timestamp = datetime(2026, 1, 1, 0, 1, tzinfo=ZoneInfo(local_timezone))
        record = {"id": 1, "name": partition_value,
                  "ts_millis": int(timestamp.timestamp() * 1000)}
        self._publish_json_file(input_path, "first.json", record)
        dataframe = pf.read_json(
            input_path, schema={**self._SCHEMA, "ts_millis": DataType.int64()},
            monitor_interval="50ms",
            computed_columns={"ts": "TO_TIMESTAMP_LTZ(ts_millis, 3)"},
            watermark=("ts", "ts - INTERVAL '1' SECOND"),
        )
        results = []
        execute_insert = dataframe._table.execute_insert

        def capture_result(*args, **kwargs):
            result = execute_insert(*args, **kwargs)
            results.append(result)
            return result

        # Keep the public writer's actual execution, but manage this unbounded job in the test.
        with patch.object(dataframe._table, "execute_insert", side_effect=capture_result), \
                patch.object(TableResult, "wait"):
            getattr(dataframe, "write_" + file_format)(
                output_path, mode="append", partition_by="name",
                rolling_policy_rollover_interval="100ms", rolling_policy_check_interval="50ms",
                partition_commit_policy_kind="success-file",
                partition_commit_trigger=partition_commit_trigger,
                connector_options={
                    "sink.partition-commit.success-file.name": "_READY",
                    "partition.time-extractor.timestamp-pattern": "$name 00:00:00",
                    "sink.partition-commit.watermark-time-zone": local_timezone,
                },
            )
        job_client = results[0].get_job_client()
        partition_path = os.path.join(output_path, "name=" + partition_value)
        try:
            self._wait_for(
                lambda: os.path.exists(os.path.join(partition_path, "_READY"))
                and len(self._finished_files(partition_path)) >= 1,
                "first checkpoint did not finish and commit the partition",
            )
            self._publish_json_file(input_path, "second.json", {**record, "id": 2})
            self._wait_for(
                lambda: len(self._finished_files(partition_path)) >= 2,
                "newly discovered file was not committed by a later checkpoint",
            )
            self.assertEqual(job_client.get_job_status().result(), JobStatus.RUNNING)
        finally:
            cancellation = job_client.cancel()
            self._wait_for(cancellation.done, "streaming job cancellation did not complete")
            cancellation.result()

        self.env.set_runtime_mode(RuntimeExecutionMode.BATCH)
        self.t_env = StreamTableEnvironment.create(
            self.env, environment_settings=EnvironmentSettings.in_batch_mode())
        pf.set_table_environment(self.t_env)
        rows = getattr(pf, "read_" + file_format)(
            output_path, schema=self._SCHEMA, partition_by="name").collect()
        self.assertEqual(sorted(tuple(row) for row in rows),
                         [(1, partition_value), (2, partition_value)])

    @staticmethod
    def _publish_json_file(directory, filename, record):
        temporary_path = os.path.join(directory, ".pending")
        with open(temporary_path, "w", encoding="utf-8") as source:
            source.write(json.dumps(record) + "\n")
        os.replace(temporary_path, os.path.join(directory, filename))

    @staticmethod
    def _finished_files(directory):
        if not os.path.isdir(directory):
            return []
        return [name for name in os.listdir(directory) if name.startswith("part-")]

    def _wait_for(self, predicate, message):
        deadline = time.monotonic() + 60
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail(message)
            time.sleep(0.05)

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
        rows = pf.read_json(
            input_path, schema=self._SCHEMA, path_regex_pattern="data[.]json",
        ).collect()
        self.assertEqual(rows, [])

    def test_json_continuous_source_is_rejected_in_batch_mode(self):
        self._check_continuous_source_is_rejected_in_batch_mode(pf.read_json)

    @unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                     'Hadoop libraries are required for Parquet format tests')
    def test_parquet_continuous_source_is_rejected_in_batch_mode(self):
        self._check_continuous_source_is_rejected_in_batch_mode(pf.read_parquet)

    def _check_continuous_source_is_rejected_in_batch_mode(self, reader):
        input_path = os.path.join(self.tempdir, self._testMethodName)
        os.makedirs(input_path)
        dataframe = reader(input_path, schema=self._SCHEMA, monitor_interval="1s")
        with self.assertRaisesRegex(Py4JJavaError, "[Uu]nbounded"):
            dataframe.to_table().explain()

    def test_computed_watermark_supports_event_time_windows(self):
        input_path = os.path.join(self.tempdir, "event-time.json")
        with open(input_path, "w", encoding="utf-8") as source:
            source.write('{"ts_millis":1000}\n{"ts_millis":2000}\n')
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.t_env = StreamTableEnvironment.create(self.env)
        pf.set_table_environment(self.t_env)
        dataframe = pf.read_json(
            input_path, schema={"ts_millis": DataType.int64()},
            computed_columns={"ts": "TO_TIMESTAMP_LTZ(ts_millis, 3)"},
            watermark=("ts", "ts - INTERVAL '1' SECOND"),
        )
        self.t_env.create_temporary_view("events", dataframe.to_table())
        result = pf.sql(
            "SELECT COUNT(*) AS cnt FROM TABLE("
            "TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '5' SECOND)) "
            "GROUP BY window_start, window_end"
        ).collect()
        self.assertEqual([tuple(row) for row in result], [(2,)])

    def test_json_timestamp_format_options_round_trip(self):
        output_path = os.path.join(self.tempdir, "json-timestamp")
        timestamp = datetime(2026, 1, 2, 3, 4, 5)
        dataframe = pf.sql("SELECT TIMESTAMP '2026-01-02 03:04:05' AS ts")
        dataframe.write_json(
            output_path, timestamp_format="ISO-8601",
            format_options={"timestamp-format.standard": "ISO-8601"},
        )
        records = []
        for filename in os.listdir(output_path):
            if filename.startswith("part-"):
                with open(os.path.join(output_path, filename), encoding="utf-8") as output:
                    records.extend(json.loads(line) for line in output)
        self.assertEqual(records, [{"ts": timestamp.isoformat()}])

        rows = pf.read_json(
            output_path, schema={"ts": DataType.timestamp()},
            timestamp_format="ISO-8601",
            format_options={"json.timestamp-format.standard": "ISO-8601"},
        ).collect()
        self.assertEqual([tuple(row) for row in rows], [(timestamp,)])


if __name__ == "__main__":
    unittest.main()
