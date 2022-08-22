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

"""
Common classes used by both Flink DataStream API and Table API:

    - :class:`Configuration`:
      Lightweight configuration object which stores key/value pairs.
    - :class:`ExecutionConfig`:
      A config to define the behavior of the program execution.
    - :class:`ExecutionMode`:
      Specifies how a batch program is executed in terms of data exchange: pipelining or batched.
    - :class:`TypeInformation`:
      TypeInformation is the core class of Flink's type system. FLink requires a type information
      for all types that are used as input or return type of a user function.
    - :class:`Types`:
      Contains utilities to access the :class:`TypeInformation` of the most common types for which
      Flink has provided built-in implementation.
    - :class:`WatermarkStrategy`:
      Defines how to generate Watermarks in the stream sources.
    - :class:`Row`:
      A row is a fixed-length, null-aware composite type for storing multiple values in a
      deterministic field order.
    - :class:`SerializationSchema`:
      Base class to describes how to turn a data object into a different serialized representation.
      Most data sinks (for example Apache Kafka) require the data to be handed to them in a specific
      format (for example as byte strings). See
      :class:`~pyflink.datastream.formats.json.JsonRowSerializationSchema`,
      :class:`~pyflink.datastream.formats.json.JsonRowDeserializationSchema`,
      :class:`~pyflink.datastream.formats.csv.CsvRowSerializationSchema`,
      :class:`~pyflink.datastream.formats.csv.CsvRowDeserializationSchema`,
      :class:`~pyflink.datastream.formats.avro.AvroRowSerializationSchema`,
      :class:`~pyflink.datastream.formats.avro.AvroRowDeserializationSchema` and
      :class:`~SimpleStringSchema` for more details.
"""
from pyflink.common.completable_future import CompletableFuture
from pyflink.common.config_options import ConfigOption, ConfigOptions
from pyflink.common.configuration import Configuration
from pyflink.common.execution_config import ExecutionConfig
from pyflink.common.execution_mode import ExecutionMode
from pyflink.common.input_dependency_constraint import InputDependencyConstraint
from pyflink.common.job_client import JobClient
from pyflink.common.job_execution_result import JobExecutionResult
from pyflink.common.job_id import JobID
from pyflink.common.job_status import JobStatus
from pyflink.common.restart_strategy import RestartStrategies, RestartStrategyConfiguration
from pyflink.common.serialization import SerializationSchema, DeserializationSchema, \
    SimpleStringSchema, Encoder
from pyflink.common.serializer import TypeSerializer
from pyflink.common.typeinfo import Types, TypeInformation
from pyflink.common.types import Row, RowKind
from pyflink.common.time import Duration, Instant, Time
from pyflink.common.watermark_strategy import WatermarkStrategy, \
    AssignerWithPeriodicWatermarksWrapper

__all__ = [
    'Configuration',
    'ConfigOption',
    'ConfigOptions',
    'ExecutionConfig',
    "TypeInformation",
    "TypeSerializer",
    "Types",
    'SerializationSchema',
    'DeserializationSchema',
    'SimpleStringSchema',
    'Encoder',
    'CompletableFuture',
    'ExecutionMode',
    'InputDependencyConstraint',
    'JobClient',
    'JobExecutionResult',
    'JobID',
    'JobStatus',
    'RestartStrategies',
    'RestartStrategyConfiguration',
    "Row",
    "RowKind",
    "WatermarkStrategy",
    "Duration",
    "Instant",
    "Time",
    "AssignerWithPeriodicWatermarksWrapper"
]


def _install():
    from pyflink import common

    # json
    from pyflink.datastream.formats.json import JsonRowDeserializationSchema
    from pyflink.datastream.formats.json import JsonRowSerializationSchema
    setattr(common, 'JsonRowDeserializationSchema', JsonRowDeserializationSchema)
    setattr(common, 'JsonRowSerializationSchema', JsonRowSerializationSchema)

    # csv
    from pyflink.datastream.formats.csv import CsvRowDeserializationSchema
    from pyflink.datastream.formats.csv import CsvRowSerializationSchema
    setattr(common, 'CsvRowDeserializationSchema', CsvRowDeserializationSchema)
    setattr(common, 'CsvRowSerializationSchema', CsvRowSerializationSchema)

    # avro
    from pyflink.datastream.formats.avro import AvroRowDeserializationSchema
    from pyflink.datastream.formats.avro import AvroRowSerializationSchema
    setattr(common, 'AvroRowDeserializationSchema', AvroRowDeserializationSchema)
    setattr(common, 'AvroRowSerializationSchema', AvroRowSerializationSchema)


# for backward compatibility
_install()
del _install
