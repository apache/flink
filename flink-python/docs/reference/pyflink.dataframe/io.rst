.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################

==============
Input / Output
==============

DataFrame I/O functions read from and write to external systems through Flink connectors.
Connector-specific methods provide convenient interfaces for common systems, while generic
methods expose the raw connector identifiers and string options used by Table connector factories.

Readers
-------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    read_generic
    read_json
    read_parquet

Writers
-------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.write_generic
    DataFrame.write_json
    DataFrame.write_parquet

Filesystem sources and sinks
----------------------------

``read_json`` reads newline-delimited JSON; ``read_parquet`` reads Parquet files.
Both readers require an explicit schema and use the existing filesystem connector.
The connector and format JARs must be available to Flink. Parquet also requires
Hadoop libraries on the classpath, which can be provided through ``HADOOP_CLASSPATH``.

For example, convert JSON records to partitioned Parquet files::

    import pyflink.dataframe as pf

    pf.config.set("execution.runtime-mode", "batch")
    events = pf.read_json(
        "file:///tmp/events.json",
        schema={"id": pf.DataType.int64(), "event": pf.DataType.string()},
        ignore_parse_errors=True,
    )
    events.write_parquet(
        "file:///tmp/events-parquet", compression="GZIP", partition_by="event"
    )

Partitioned data
~~~~~~~~~~~~~~~~

``partition_by`` accepts a column name or a non-empty list of column names. The order
determines the directory layout, such as ``event=login/day=2026-01-01/``. The filesystem
connector stores partition values in these paths and excludes the columns from the file
records. Readers require the partition columns in both ``schema`` and ``partition_by``
to restore their values. Null and empty string partition values share the default
partition directory. Its name can be configured using
``connector_options={"partition.default-name": "..."}``. The current filesystem connector
cannot read these default partitions: planning fails while reconstructing their paths.
Use non-null, non-empty partition values when the output needs to be read back.

The writers default to ``mode="overwrite"``. For a non-partitioned sink this replaces the
existing output data. For a partitioned sink only partitions present in the input are
overwritten; other partitions are retained. Use ``mode="append"`` to add files. Overwrite
is supported only in batch execution; streaming execution must use append mode. Configure
``execution.runtime-mode`` before creating the DataFrame environment. Writes wait
for completion with local or MiniCluster execution, just like ``write_generic``.
An unbounded local write therefore continues waiting until the job terminates.

Continuous reads and event time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Without ``monitor_interval``, readers scan the configured paths once and produce a bounded
source. Setting a positive interval, such as ``"60s"``, enables continuous discovery and
requires streaming execution. It does not change the environment's execution mode.
Each file is identified by its path and processed once; discovery does not tail files
for appended records or re-read files changed in place.

For partitioned sources, Flink discovers the partition directories during planning and
monitors those paths. It does not discover new partition directories. If no partitions
exist initially, the source remains empty even when ``monitor_interval`` is set.

``path_regex_pattern`` uses Java regular expressions and matches the entire file path,
excluding the URI scheme and authority. It is not a substring search or a filename-only
match. For example, use ``r".*/events[.]json"`` to select files named ``events.json``.
Hidden files and directories are excluded.

Both readers support ``computed_columns`` and ``watermark``, using the same schema
declarations as ``read_generic``. A watermark needs a TIMESTAMP or TIMESTAMP_LTZ column
with precision from 0 to 3, for example ``pf.DataType.timestamp(3)``. For example::

    pf.config.set("execution.runtime-mode", "streaming")
    events = pf.read_json(
        "file:///tmp/incoming",
        schema={"id": pf.DataType.int64(), "ts_millis": pf.DataType.int64()},
        monitor_interval="10s",
        computed_columns={"event_time": "TO_TIMESTAMP_LTZ(ts_millis, 3)"},
        watermark=("event_time", "event_time - INTERVAL '5' SECOND"),
    )

Run this example in a fresh environment, or configure an existing streaming environment
before injecting it through ``set_table_environment``.

Options and defaults
~~~~~~~~~~~~~~~~~~~~

All four helpers accept ``connector_options`` for filesystem options and ``format_options``
for JSON or Parquet options. Both dictionaries require string keys and values. Format keys
may include or omit the current format prefix, such as ``compression`` or
``parquet.compression``. Specifying both forms of the same key is rejected. The
``connector``, ``path`` and ``format`` options are reserved; format options must be passed
through ``format_options`` rather than ``connector_options``.

Optional convenience parameters use ``None`` to mean unspecified. Dictionary values are
merged with explicitly supplied parameters before defaults are applied. Supplying both
forms is allowed if their string values agree; conflicting values raise ``ValueError``.
Values are compared as strings: ``"1min"`` and ``"60s"`` are different settings for this
check. The dictionaries supplied by the caller are not modified.

For example, this uses GZIP rather than the default SNAPPY::

    events.write_parquet(
        "file:///tmp/output",
        mode="append",
        format_options={"compression": "GZIP", "write.int64.timestamp": "true"},
        connector_options={"sink.rolling-policy.file-size": "64mb"},
    )

Parquet's effective default compression remains ``"SNAPPY"``. The default file rolling
threshold is ``"128mb"``, rollover and inactivity thresholds are ``"30min"``, and the
time-based rolling check interval is ``"1min"``. Partition commit defaults to
``"process-time"`` with a ``"0s"`` delay and no policy. JSON defaults to SQL timestamp
formatting, does not ignore parse errors, and does not fail on missing fields. Parquet
timestamp conversion uses the JVM default time zone unless ``utc_timezone=True`` is set.
This format option is independent of the session time zone and the partition commit time zone.

The filesystem connector and format factories validate option values. Options that do not
have a convenience parameter, including partition time extractors, custom commit policies,
compaction and Parquet tuning options, remain available through these dictionaries.

Streaming writes
~~~~~~~~~~~~~~~~

Rolling policy parameters apply to streaming writes, not batch writes. File size and time
settings are rolling thresholds, not hard limits on output file size or visibility latency.
Continuous writes need checkpointing to finish pending files. JSON also needs a rolling
condition to close the file; Parquet rolls on checkpoints as well as on size/time thresholds.
Configure ``execution.checkpointing.interval`` through the environment configuration before
execution; these helpers do not enable checkpointing automatically.

Streaming partition commit requires ``partition_by`` and a policy such as ``"success-file"``.
The ``"partition-time"`` trigger also needs upstream watermarks and partition time extraction
settings in ``connector_options``. If the watermark is declared on a TIMESTAMP_LTZ column,
set ``sink.partition-commit.watermark-time-zone`` to the session time zone. For example,
with ``table.local-time-zone="Asia/Shanghai"``, pass
``connector_options={"sink.partition-commit.watermark-time-zone": "Asia/Shanghai"}``.
Leaving this option at its UTC default with a non-UTC session time zone can shift partition
commit times by hours. For a TIMESTAMP watermark, keep the UTC default.

A success marker is a partition commit notification; it does not prevent later records from
being written to the partition. The ``"metastore"`` policy is specific to Hive tables and
cannot be used with these filesystem helpers.
