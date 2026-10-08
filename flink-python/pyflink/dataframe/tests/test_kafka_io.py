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
#  limitations under the License.
################################################################################

import inspect
import unittest
from unittest.mock import MagicMock, patch

import pyflink.dataframe as pf
from pyflink.dataframe import DataType
from pyflink.dataframe.io import (
    _build_kafka_options,
    _build_kafka_sink_options,
    _convert_specific_offsets,
)
from pyflink.testing.test_case_utils import PyFlinkDataFrameUTTestCase


class KafkaOptionTests(unittest.TestCase):
    def test_reader_schema_is_first_keyword_only_parameter(self):
        parameters = inspect.signature(pf.read_kafka).parameters
        self.assertEqual(list(parameters)[:2], ["bootstrap_servers", "schema"])
        self.assertEqual(parameters["schema"].kind, inspect.Parameter.KEYWORD_ONLY)
        self.assertIs(parameters["schema"].default, inspect.Parameter.empty)

    def test_writer_uses_sink_parallelism_parameter(self):
        parameters = inspect.signature(pf.DataFrame.write_kafka).parameters
        self.assertIn("sink_parallelism", parameters)
        self.assertNotIn("parallelism", parameters)
        self.assertEqual(parameters["sink_parallelism"].kind, inspect.Parameter.KEYWORD_ONLY)
        self.assertIsNone(parameters["sink_parallelism"].default)

    def _source_options(self, **overrides):
        arguments = {
            "bootstrap_servers": "localhost:9092", "topic": "events", "topic_pattern": None,
            "group_id": None, "value_format": "json", "format_options": None,
            "value_format_options": None, "key_format": None, "key_format_options": None,
            "key_fields": None, "key_fields_prefix": None, "value_fields_include": "ALL",
            "startup_mode": "group-offsets", "startup_specific_offsets": None,
            "startup_timestamp_millis": None, "topic_partition_discovery_interval": "5 min",
            "bounded_mode": "unbounded", "bounded_timestamp_millis": None,
            "bounded_specific_offsets": None, "properties": None,
        }
        arguments.update(overrides)
        return _build_kafka_options(**arguments)

    def _sink_options(self, **overrides):
        arguments = {
            "bootstrap_servers": "localhost:9092", "topic": "events", "value_format": "json",
            "format": "json", "format_options": None, "value_format_options": None,
            "key_format": None, "key_format_options": None, "key_fields": None,
            "value_fields_include": "ALL", "delivery_guarantee": "at-least-once",
            "properties": None, "sink_parallelism": None,
        }
        arguments.update(overrides)
        return _build_kafka_sink_options(**arguments)

    def test_source_discovery_interval(self):
        for interval, expected in ((None, "0"), ("0", "0"), ("5 min", "5 min")):
            with self.subTest(interval=interval):
                options = self._source_options(topic_partition_discovery_interval=interval)
                self.assertEqual(options["scan.topic-partition-discovery.interval"], expected)

    def test_bounded_specific_offsets(self):
        options = self._source_options(
            bounded_mode="specific-offsets", bounded_specific_offsets={0: 10, 1: 20})
        self.assertEqual(options["scan.bounded.specific-offsets"],
                         "partition:0,offset:10;partition:1,offset:20")

    def test_sink_transactional_id_prefix_and_partitioner(self):
        options = self._sink_options(
            delivery_guarantee="exactly-once", transactional_id_prefix="dataframe-events",
            partitioner="fixed")
        self.assertEqual(options["sink.transactional-id-prefix"], "dataframe-events")
        self.assertEqual(options["sink.partitioner"], "fixed")
        defaults = self._sink_options()
        self.assertNotIn("sink.transactional-id-prefix", defaults)
        self.assertNotIn("sink.partitioner", defaults)
        custom = self._sink_options(partitioner="org.example.CustomPartitioner")
        self.assertEqual(custom["sink.partitioner"], "org.example.CustomPartitioner")

    def test_sink_rejects_missing_or_invalid_transaction_options(self):
        for overrides, error_type, message in (
            ({"delivery_guarantee": "exactly-once"}, ValueError, "transactional-id-prefix"),
            ({"transactional_id_prefix": 1}, TypeError, "transactional_id_prefix"),
            ({"transactional_id_prefix": ""}, ValueError, "transactional_id_prefix"),
            ({"partitioner": 1}, TypeError, "partitioner"),
            ({"partitioner": ""}, ValueError, "partitioner"),
            ({"delivery_guarantee": "exactly-once",
              "extra_options": {"sink.transactional-id-prefix": ""}}, ValueError,
             "transactional-id-prefix"),
        ):
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(error_type, message):
                    self._sink_options(**overrides)

    def test_raw_connector_options(self):
        source = self._source_options(
            extra_options={"scan.parallelism": "3", "properties.client.id": "reader"})
        self.assertEqual(source["scan.parallelism"], "3")
        self.assertEqual(source["properties.client.id"], "reader")
        sink = self._sink_options(delivery_guarantee="exactly-once", extra_options={
            "sink.transactional-id-prefix": "writer", "sink.partitioner": "round-robin"})
        self.assertEqual(sink["sink.transactional-id-prefix"], "writer")
        self.assertEqual(sink["sink.partitioner"], "round-robin")

    def test_raw_options_reject_invalid_or_conflicting_values(self):
        for build in (self._source_options, self._sink_options):
            raw_options = {"topic": "events", "properties.client.id": "reader"}
            original_options = raw_options.copy()
            build(extra_options=raw_options)
            self.assertEqual(raw_options, original_options)
            for extra, error_type in (
                ([], TypeError), ({"scan.parallelism": 3}, TypeError), ({1: "value"}, TypeError),
                ({"": "value"}, ValueError), ({"connector": "filesystem"}, ValueError),
                ({"topic": "other"}, ValueError), ({"value.format": "csv"}, ValueError),
                ({"properties.bootstrap.servers": "other:9092"}, ValueError),
            ):
                with self.subTest(build=build.__name__, extra=extra):
                    with self.assertRaises(error_type):
                        build(extra_options=extra)
            self.assertEqual(build(extra_options={"topic": "events"})["topic"], "events")
            with self.assertRaisesRegex(ValueError, "properties.client.id"):
                build(properties={"client.id": "one"},
                      extra_options={"properties.client.id": "two"})
        with self.assertRaisesRegex(ValueError, "sink.partitioner"):
            self._sink_options(partitioner="fixed", extra_options={"sink.partitioner": "default"})
        with self.assertRaisesRegex(ValueError, "sink.transactional-id-prefix"):
            self._sink_options(transactional_id_prefix="one",
                               extra_options={"sink.transactional-id-prefix": "two"})
        with self.assertRaisesRegex(ValueError, "scan.startup.mode"):
            self._source_options(extra_options={"scan.startup.mode": "latest-offset"})

    def test_builds_source_options(self):
        options = _build_kafka_options(
            "localhost:9092",
            topic="events",
            topic_pattern=None,
            group_id="group",
            value_format="json",
            format_options={"ignore-parse-errors": "true"},
            value_format_options=None,
            key_format="json",
            key_format_options={"ignore-parse-errors": "false"},
            key_fields=["id", "name"],
            key_fields_prefix="k_",
            value_fields_include="EXCEPT_KEY",
            startup_mode="specific-offsets",
            startup_specific_offsets={0: 10, 1: 20},
            startup_timestamp_millis=None,
            topic_partition_discovery_interval="1 min",
            bounded_mode="timestamp",
            bounded_timestamp_millis=123456,
            bounded_specific_offsets=None,
            properties={"enable.idempotence": "true"},
        )

        self.assertEqual(options, {
            "properties.bootstrap.servers": "localhost:9092",
            "properties.group.id": "group",
            "topic": "events",
            "value.format": "json",
            "value.json.ignore-parse-errors": "true",
            "key.format": "json",
            "key.json.ignore-parse-errors": "false",
            "key.fields": "id;name",
            "key.fields-prefix": "k_",
            "value.fields-include": "EXCEPT_KEY",
            "scan.startup.mode": "specific-offsets",
            "scan.startup.specific-offsets": "partition:0,offset:10;partition:1,offset:20",
            "scan.topic-partition-discovery.interval": "1 min",
            "scan.bounded.mode": "timestamp",
            "scan.bounded.timestamp-millis": "123456",
            "properties.enable.idempotence": "true",
        })

    def test_source_topic_list_is_semicolon_joined(self):
        options = _build_kafka_options(
            "localhost:9092",
            topic=["events-1", "events-2"],
            topic_pattern=None,
            group_id=None,
            value_format="json",
            format_options=None,
            value_format_options=None,
            key_format=None,
            key_format_options=None,
            key_fields=None,
            key_fields_prefix=None,
            value_fields_include="ALL",
            startup_mode="group-offsets",
            startup_specific_offsets=None,
            startup_timestamp_millis=None,
            topic_partition_discovery_interval=None,
            bounded_mode="unbounded",
            bounded_timestamp_millis=None,
            bounded_specific_offsets=None,
            properties=None,
        )

        self.assertEqual(options["topic"], "events-1;events-2")
        self.assertNotIn("topic-pattern", options)

    def test_source_topic_pattern(self):
        options = _build_kafka_options(
            "localhost:9092",
            topic=None,
            topic_pattern="events-.*",
            group_id=None,
            value_format="json",
            format_options=None,
            value_format_options=None,
            key_format=None,
            key_format_options=None,
            key_fields=None,
            key_fields_prefix=None,
            value_fields_include="ALL",
            startup_mode="group-offsets",
            startup_specific_offsets=None,
            startup_timestamp_millis=None,
            topic_partition_discovery_interval=None,
            bounded_mode="unbounded",
            bounded_timestamp_millis=None,
            bounded_specific_offsets=None,
            properties=None,
        )

        self.assertEqual(options["topic-pattern"], "events-.*")
        self.assertNotIn("topic", options)

    def test_source_rejects_conflicting_format_option_aliases(self):
        with self.assertRaisesRegex(ValueError, "conflicting values"):
            _build_kafka_options(
                "localhost:9092",
                topic="events",
                topic_pattern=None,
                group_id=None,
                value_format="json",
                format_options={"ignore-parse-errors": "true"},
                value_format_options={"ignore-parse-errors": "false"},
                key_format=None,
                key_format_options=None,
                key_fields=None,
                key_fields_prefix=None,
                value_fields_include="ALL",
                startup_mode="group-offsets",
                startup_specific_offsets=None,
                startup_timestamp_millis=None,
                topic_partition_discovery_interval=None,
                bounded_mode="unbounded",
                bounded_timestamp_millis=None,
                bounded_specific_offsets=None,
                properties=None,
            )

    def test_source_rejects_invalid_arguments(self):
        base_arguments = {
            "topic": "events",
            "topic_pattern": None,
            "group_id": None,
            "value_format": "json",
            "format_options": None,
            "value_format_options": None,
            "key_format": None,
            "key_format_options": None,
            "key_fields": None,
            "key_fields_prefix": None,
            "value_fields_include": "ALL",
            "startup_mode": "group-offsets",
            "startup_specific_offsets": None,
            "startup_timestamp_millis": None,
            "topic_partition_discovery_interval": None,
            "bounded_mode": "unbounded",
            "bounded_timestamp_millis": None,
            "bounded_specific_offsets": None,
            "properties": None,
        }
        cases = [
            ({"bootstrap_servers": None}, TypeError, "bootstrap_servers must be a string"),
            ({"bootstrap_servers": ""}, ValueError, "bootstrap_servers must not be empty"),
            ({"topic": None}, ValueError, "either 'topic' or 'topic_pattern'"),
            ({"topic": "a", "topic_pattern": "b"}, ValueError, "mutually exclusive"),
            ({"topic": []}, ValueError, "topic must not be an empty list"),
            ({"topic": [1]}, TypeError, "topic list elements must be strings"),
            ({"topic": 1}, TypeError, "topic must be a string"),
            ({"topic": None, "topic_pattern": 1}, TypeError, "topic_pattern must be a string"),
            ({"group_id": 1}, TypeError, "group_id must be a string"),
            ({"value_fields_include": "BAD"}, ValueError, "value_fields_include"),
            ({"startup_mode": "bad"}, ValueError, "startup_mode"),
            ({"startup_mode": "specific-offsets"}, ValueError, "startup_specific_offsets"),
            ({"startup_mode": "timestamp"}, ValueError, "startup_timestamp_millis"),
            ({"startup_mode": "timestamp", "startup_timestamp_millis": True}, TypeError,
             "startup_timestamp_millis must be an int"),
            ({"bounded_mode": "bad"}, ValueError, "bounded_mode"),
            ({"bounded_mode": "specific-offsets"}, ValueError, "bounded_specific_offsets"),
            ({"bounded_mode": "timestamp"}, ValueError, "bounded_timestamp_millis"),
            ({"bounded_mode": "timestamp", "bounded_timestamp_millis": True}, TypeError,
             "bounded_timestamp_millis must be an int"),
            ({"topic_partition_discovery_interval": 1}, TypeError,
             "topic_partition_discovery_interval must be a string"),
            ({"key_format": 1}, TypeError, "key_format must be a string"),
            ({"key_fields": "id"}, TypeError, "key_fields must be a list"),
            ({"key_fields": []}, ValueError, "key_fields must not be empty"),
            ({"key_fields": [1]}, TypeError, "key_fields elements must be strings"),
            ({"key_fields_prefix": 1}, TypeError, "key_fields_prefix must be a string"),
            ({"properties": {"properties.bootstrap.servers": "other:9092"}}, ValueError,
             "properties.bootstrap.servers"),
            ({"properties": {"properties.group.id": "other"}}, ValueError,
             "properties.group.id"),
        ]

        for overrides, error_type, message in cases:
            with self.subTest(overrides=overrides):
                arguments = {"bootstrap_servers": "localhost:9092"}
                arguments.update(base_arguments)
                arguments.update(overrides)
                with self.assertRaisesRegex(error_type, message):
                    _build_kafka_options(**arguments)

    def test_source_accepts_explicit_value_format_options(self):
        arguments = {
            "bootstrap_servers": "localhost:9092",
            "topic": "events",
            "topic_pattern": None,
            "group_id": None,
            "value_format": "json",
            "format_options": {"ignore-parse-errors": "true"},
            "value_format_options": {"ignore-parse-errors": "false"},
            "key_format": None,
            "key_format_options": None,
            "key_fields": None,
            "key_fields_prefix": None,
            "value_fields_include": "ALL",
            "startup_mode": "group-offsets",
            "startup_specific_offsets": None,
            "startup_timestamp_millis": None,
            "topic_partition_discovery_interval": None,
            "bounded_mode": "unbounded",
            "bounded_timestamp_millis": None,
            "bounded_specific_offsets": None,
            "properties": None,
        }
        arguments["format_options"] = {"json.ignore-parse-errors": "true"}
        arguments["value_format_options"] = {"json.ignore-parse-errors": "true"}
        options = _build_kafka_options(**arguments)

        self.assertEqual(options["value.json.ignore-parse-errors"], "true")

    def test_specific_offsets_are_converted(self):
        self.assertEqual(_convert_specific_offsets({1: 2}, "offsets"), "partition:1,offset:2")
        self.assertEqual(_convert_specific_offsets("partition:0,offset:1", "offsets"),
                         "partition:0,offset:1")
        self.assertEqual(
            _convert_specific_offsets({0: 10, 1: 20}, "offsets"),
            "partition:0,offset:10;partition:1,offset:20")
        self.assertIsNone(_convert_specific_offsets(None, "offsets"))
        with self.assertRaisesRegex(TypeError, "offsets keys"):
            _convert_specific_offsets({"1": 2}, "offsets")
        with self.assertRaisesRegex(TypeError, "offsets values"):
            _convert_specific_offsets({1: "2"}, "offsets")
        with self.assertRaisesRegex(TypeError, "offsets must be"):
            _convert_specific_offsets([], "offsets")

    def test_source_prefixes_kafka_properties(self):
        options = _build_kafka_options(
            "localhost:9092",
            topic="events",
            topic_pattern=None,
            group_id=None,
            value_format="json",
            format_options=None,
            value_format_options=None,
            key_format=None,
            key_format_options=None,
            key_fields=None,
            key_fields_prefix=None,
            value_fields_include="ALL",
            startup_mode="group-offsets",
            startup_specific_offsets=None,
            startup_timestamp_millis=None,
            topic_partition_discovery_interval=None,
            bounded_mode="unbounded",
            bounded_timestamp_millis=None,
            bounded_specific_offsets=None,
            properties={"security.protocol": "SASL_SSL"},
        )

        self.assertEqual(options["properties.security.protocol"], "SASL_SSL")

    def test_builds_sink_options(self):
        options = _build_kafka_sink_options(
            "localhost:9092",
            topic="events",
            value_format="json",
            format="json",
            format_options={"ignore-parse-errors": "true"},
            value_format_options=None,
            key_format="json",
            key_format_options={"ignore-parse-errors": "false"},
            key_fields=["id", "name"],
            value_fields_include="EXCEPT_KEY",
            delivery_guarantee="exactly-once",
            transactional_id_prefix="events-writer",
            properties={"transaction.timeout.ms": "60000"},
            sink_parallelism=3,
        )

        self.assertEqual(options, {
            "properties.bootstrap.servers": "localhost:9092",
            "topic": "events",
            "value.format": "json",
            "value.json.ignore-parse-errors": "true",
            "key.format": "json",
            "key.json.ignore-parse-errors": "false",
            "key.fields": "id;name",
            "value.fields-include": "EXCEPT_KEY",
            "sink.delivery-guarantee": "exactly-once",
            "sink.transactional-id-prefix": "events-writer",
            "properties.transaction.timeout.ms": "60000",
            "sink.parallelism": "3",
        })

    def test_sink_value_format_takes_precedence(self):
        options = _build_kafka_sink_options(
            "localhost:9092",
            topic="events",
            value_format="csv",
            format=None,
            format_options=None,
            value_format_options=None,
            key_format=None,
            key_format_options=None,
            key_fields=None,
            value_fields_include="ALL",
            delivery_guarantee="at-least-once",
            properties=None,
            sink_parallelism=None,
        )

        self.assertEqual(options["value.format"], "csv")

    def test_sink_value_format_overrides_format(self):
        options = _build_kafka_sink_options(
            "localhost:9092",
            topic="events",
            value_format="csv",
            format="json",
            format_options=None,
            value_format_options=None,
            key_format=None,
            key_format_options=None,
            key_fields=None,
            value_fields_include="ALL",
            delivery_guarantee="at-least-once",
            properties=None,
            sink_parallelism=None,
        )

        self.assertEqual(options["value.format"], "csv")

    def test_sink_rejects_invalid_arguments(self):
        base_arguments = {
            "value_format": "json",
            "format": "json",
            "format_options": None,
            "value_format_options": None,
            "key_format": None,
            "key_format_options": None,
            "key_fields": None,
            "value_fields_include": "ALL",
            "delivery_guarantee": "at-least-once",
            "properties": None,
            "sink_parallelism": None,
        }
        cases = [
            ({"bootstrap_servers": None}, TypeError, "bootstrap_servers must be a string"),
            ({"topic": None}, TypeError, "topic must be a string"),
            ({"topic": ""}, ValueError, "topic must not be empty"),
            ({"value_fields_include": "BAD"}, ValueError, "value_fields_include"),
            ({"delivery_guarantee": "bad"}, ValueError, "delivery_guarantee"),
            ({"key_format": 1}, TypeError, "key_format must be a string"),
            ({"key_fields": "id"}, TypeError, "key_fields must be a list"),
            ({"key_fields": []}, ValueError, "key_fields must not be empty"),
            ({"key_fields": [1]}, TypeError, "key_fields elements must be strings"),
            ({"sink_parallelism": "3"}, TypeError, "sink_parallelism must be an int"),
            ({"sink_parallelism": True}, TypeError, "sink_parallelism must be an int"),
            ({"properties": {"properties.bootstrap.servers": "other:9092"}}, ValueError,
             "properties.bootstrap.servers"),

        ]

        for overrides, error_type, message in cases:
            with self.subTest(overrides=overrides):
                arguments = {
                    "bootstrap_servers": "localhost:9092",
                    "topic": "events",
                }
                arguments.update(base_arguments)
                arguments.update(overrides)
                with self.assertRaisesRegex(error_type, message):
                    _build_kafka_sink_options(**arguments)


class KafkaDescriptorTests(PyFlinkDataFrameUTTestCase):
    _SCHEMA = {"id": DataType.int64(), "name": DataType.string()}

    def test_read_kafka_builds_source_descriptor(self):
        with patch.object(
            self.t_env,
            "from_descriptor",
            wraps=self.t_env.from_descriptor,
        ) as from_descriptor:
            dataframe = pf.read_kafka(
                "localhost:9092",
                topic="events",
                schema={
                    "id": DataType.int64(),
                    "ts_millis": DataType.int64(),
                },
                format="json",
                startup_mode="earliest-offset",
                computed_columns={
                    "event_time": "TO_TIMESTAMP_LTZ(ts_millis, 3)"
                },
                watermark=(
                    "event_time",
                    "event_time - INTERVAL '5' SECOND",
                ),
            )

        descriptor = from_descriptor.call_args.args[0]
        self.assertEqual(descriptor.get_options().get("connector"), "kafka")
        self.assertEqual(descriptor.get_options().get("topic"), "events")
        self.assertEqual(descriptor.get_options().get("value.format"), "json")
        self.assertEqual(
            descriptor.get_options().get("scan.startup.mode"), "earliest-offset")
        self.assert_dataframe_schema(dataframe, ["id", "ts_millis", "event_time"])
        watermark_specs = dataframe._table.get_resolved_schema().get_watermark_specs()
        self.assertEqual(len(watermark_specs), 1)
        self.assertEqual(watermark_specs[0].get_rowtime_attribute(), "event_time")

    def test_read_kafka_uses_value_format(self):
        with patch.object(
            self.t_env,
            "from_descriptor",
            wraps=self.t_env.from_descriptor,
        ) as from_descriptor:
            pf.read_kafka(
                "localhost:9092",
                topic="events",
                schema=self._SCHEMA,
                format="json",
                value_format="csv",
            )

        options = from_descriptor.call_args.args[0].get_options()
        self.assertEqual(options.get("value.format"), "csv")

    def test_read_kafka_forwards_raw_options(self):
        with patch.object(
            self.t_env, "from_descriptor", wraps=self.t_env.from_descriptor,
        ) as from_descriptor:
            pf.read_kafka(
                "localhost:9092", schema=self._SCHEMA, topic=["events-1", "events-2"],
                key_format="json", key_fields=["id", "name"],
                startup_mode="specific-offsets", startup_specific_offsets={0: 10, 1: 20},
                topic_partition_discovery_interval=None, options={"scan.parallelism": "3"},
            )

        options = from_descriptor.call_args.args[0].get_options()
        self.assertEqual(options.get("topic"), "events-1;events-2")
        self.assertEqual(options.get("key.fields"), "id;name")
        self.assertEqual(options.get("scan.startup.specific-offsets"),
                         "partition:0,offset:10;partition:1,offset:20")
        self.assertEqual(options.get("scan.topic-partition-discovery.interval"), "0")
        self.assertEqual(options.get("scan.parallelism"), "3")

    def test_write_kafka_forwards_transaction_and_raw_options(self):
        dataframe = pf.from_records([(1, "a")], schema=["id", "name"])
        for use_raw_options in (False, True):
            with self.subTest(use_raw_options=use_raw_options):
                raw_options = {"sink.parallelism": "3"}
                transaction_options = {
                    "sink.transactional-id-prefix": "events-writer", "sink.partitioner": "fixed",
                }
                if use_raw_options:
                    raw_options.update(transaction_options)
                with patch.object(dataframe._table, "execute_insert") as execute_insert:
                    dataframe.write_kafka(
                        "localhost:9092", topic="events", delivery_guarantee="exactly-once",
                        transactional_id_prefix=None if use_raw_options else "events-writer",
                        partitioner=None if use_raw_options else "fixed", options=raw_options,
                        key_format="json", key_fields=["id", "name"],
                    )

                options = execute_insert.call_args.args[0].get_options()
                self.assertEqual(options.get("sink.transactional-id-prefix"), "events-writer")
                self.assertEqual(options.get("sink.partitioner"), "fixed")
                self.assertEqual(options.get("sink.parallelism"), "3")
                self.assertEqual(options.get("key.fields"), "id;name")

    def test_write_kafka_stages_options_without_execution(self):
        dataframe = pf.from_records([(1, "a")], schema=["id", "name"])
        statement_set = pf.create_statement_set()
        with patch.object(statement_set, "add_insert") as add_insert, \
                patch.object(statement_set, "execute") as execute, \
                patch.object(dataframe._table, "execute_insert") as execute_insert:
            dataframe.write_kafka(
                "localhost:9092", topic="events", delivery_guarantee="exactly-once",
                transactional_id_prefix="events-writer", partitioner="fixed",
                options={"sink.parallelism": "3"}, statement_set=statement_set,
            )

        add_insert.assert_called_once()
        self.assertIs(add_insert.call_args.args[1], dataframe._table)
        options = add_insert.call_args.args[0].get_options()
        self.assertEqual(options.get("sink.transactional-id-prefix"), "events-writer")
        self.assertEqual(options.get("sink.partitioner"), "fixed")
        self.assertEqual(options.get("sink.parallelism"), "3")
        execute.assert_not_called()
        execute_insert.assert_not_called()

    def test_write_kafka_builds_sink_descriptor(self):
        dataframe = pf.from_records([(1, "a")], schema=["id", "name"])

        with patch.object(dataframe._table, "execute_insert") as execute_insert:
            execute_insert.return_value = MagicMock()
            result = dataframe.write_kafka(
                "localhost:9092",
                topic="events",
                format="json",
                delivery_guarantee="at-least-once",
                sink_parallelism=3,
            )

        descriptor = execute_insert.call_args.args[0]
        self.assertIsNone(result)
        self.assertIsNone(descriptor.get_schema())
        self.assertEqual(descriptor.get_options().get("connector"), "kafka")
        self.assertEqual(descriptor.get_options().get("topic"), "events")
        self.assertEqual(descriptor.get_options().get("value.format"), "json")
        self.assertEqual(
            descriptor.get_options().get("sink.delivery-guarantee"),
            "at-least-once",
        )
        self.assertEqual(descriptor.get_options().get("sink.parallelism"), "3")
        self.assertEqual(execute_insert.call_args.kwargs, {"overwrite": False})

    def test_write_kafka_uses_value_format(self):
        dataframe = pf.from_records([(1, "a")], schema=["id", "name"])
        with patch.object(dataframe._table, "execute_insert") as execute_insert:
            dataframe.write_kafka(
                "localhost:9092",
                topic="events",
                value_format="csv",
            )

        options = execute_insert.call_args.args[0].get_options()
        self.assertEqual(options.get("value.format"), "csv")


if __name__ == "__main__":
    unittest.main()
