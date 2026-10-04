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

    read_catalog_table
    read_generic
    read_json
    read_parquet

Writers
-------

.. currentmodule:: pyflink.dataframe

.. autosummary::
    :toctree: api/

    DataFrame.write_catalog_table
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

The writers default to ``mode="append"`` in both batch and streaming execution, adding files
without replacing existing data. Explicit ``mode="overwrite"`` is supported only in batch
execution; the filesystem connector rejects it in streaming execution. For a non-partitioned
sink, overwrite replaces the existing output data. For a partitioned sink, only partitions
present in the input are overwritten; other partitions are retained. Configure
``execution.runtime-mode`` before creating the DataFrame environment. Writes wait
for completion with local or MiniCluster execution, just like ``write_generic``.
An unbounded local write therefore continues waiting until the job terminates.

Options and defaults
~~~~~~~~~~~~~~~~~~~~

All four helpers accept ``connector_options`` for filesystem options and ``format_options``
for JSON or Parquet options. Both dictionaries require string keys and values. Format keys
may include or omit the current format prefix, such as ``compression`` or
``parquet.compression``. Specifying both forms of the same key is rejected.
``connector_options`` cannot contain ``connector``, ``path`` or ``format``; use the dedicated
APIs and parameters to select these. Format options belong in ``format_options`` and are
prefixed with the format name. The bare ``connector`` key is rejected in either dictionary.

Optional connector and format parameters use ``None`` to mean unspecified. Dictionary values
are merged with explicitly supplied parameters. Options absent from both are omitted, so the
connector or format factory applies its defaults. Supplying both forms is allowed if their
string values agree; conflicting values raise ``ValueError``.
Values are compared as strings: ``"1min"`` and ``"60s"`` are different settings for this
check. The dictionaries supplied by the caller are not modified.

For example, this uses GZIP rather than the default SNAPPY::

    events.write_parquet(
        "file:///tmp/output",
        mode="append",
        format_options={"compression": "GZIP"},
        connector_options={"sink.parallelism": "2"},
    )

The current format default for Parquet compression is ``"SNAPPY"``. The default file rolling
threshold is ``"128mb"``, rollover and inactivity thresholds are ``"30min"``, and the
time-based rolling check interval is ``"1min"``. Partition commit defaults to
``"process-time"`` with a ``"0s"`` delay and no policy. JSON defaults to SQL timestamp
formatting, does not ignore parse errors, and does not fail on missing fields. Parquet's
``utc-timezone`` option defaults to ``False``, using the JVM default time zone for timestamp
conversion. This option is independent of the session time zone and the partition commit time zone.

``write_json`` also exposes ``ignore_null_fields`` and ``decimal_as_plain_number``. The first
omits fields with null values from JSON objects; it does not drop the entire record. The second
writes DECIMAL values as plain numbers instead of scientific notation, while retaining JSON
numeric values rather than strings. The format default for both options is currently ``False``.

The helpers check Python argument types and conflicting options. Connector- and format-specific
validation is delegated to the underlying implementations. Options without a convenience
parameter, including partition time extractors, custom commit policies and Parquet tuning
options, remain available through these dictionaries.

Format-specific options
~~~~~~~~~~~~~~~~~~~~~~~

Parquet writes timestamps as INT96 by default. To use INT64 with a timestamp logical type,
set ``write.int64.timestamp`` through ``format_options``. The ``timestamp.time.unit`` option
then selects ``millis``, ``micros`` (the default) or ``nanos``. Milliseconds discard precision
below a millisecond; choose the unit required by the consuming system. For example::

    timestamps = pf.sql("SELECT TIMESTAMP '2026-01-02 03:04:05.123456' AS ts")
    timestamps.write_parquet(
        "file:///tmp/timestamps",
        utc_timezone=True,
        format_options={
            "write.int64.timestamp": "true",
            "timestamp.time.unit": "micros",
        },
    )

JSON null fields and Map entries with null keys have separate settings. ``ignore_null_fields``
omits null fields from rows; it does not control null Map keys. The ``map-null-key.mode`` format
option defaults to ``"FAIL"``. ``"DROP"`` removes entries with null keys, while ``"LITERAL"``
replaces each null key with the string configured by ``map-null-key.literal``. For example::

    records = pf.sql(
        "SELECT CAST(NULL AS STRING) AS name, "
        "MAP[CAST(NULL AS STRING), 1, 'known', 2] AS attributes"
    )
    records.write_json(
        "file:///tmp/records",
        ignore_null_fields=True,
        format_options={
            "map-null-key.mode": "LITERAL",
            "map-null-key.literal": "missing",
        },
    )

The JSON record omits ``name`` and contains ``{"attributes": {"missing": 1, "known": 2}}``.

Continuous reads and event time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When ``source.monitor-interval`` is unset, readers scan the configured paths once and produce
a bounded source. Setting a positive interval, such as ``"60s"``, through ``monitor_interval``
or ``connector_options`` enables continuous discovery and requires streaming execution.
It does not change the environment's execution mode.
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
with precision from 0 to 3, such as ``pf.DataType.timestamp(3)``.

Run the following example in a fresh environment. When using ``set_table_environment``,
configure the injected environment for streaming first::

    import pyflink.dataframe as pf

    pf.config.set("execution.runtime-mode", "streaming")
    incoming_events = pf.read_json(
        "file:///tmp/incoming",
        schema={"id": pf.DataType.int64(), "ts_millis": pf.DataType.int64()},
        monitor_interval="10s",
        computed_columns={"event_time": "TO_TIMESTAMP_LTZ(ts_millis, 3)"},
        watermark=("event_time", "event_time - INTERVAL '5' SECOND"),
    )

Partition shuffle and compaction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both writers expose ``sink_shuffle_by_partition`` to redistribute records by dynamic partition
fields before writing. This can reduce the number of files but may cause data skew for heavily
populated partitions. The connector default is currently ``False``.

For streaming writes, ``auto_compaction=True`` enables automatic compaction after checkpoints
complete. Files remain invisible until compaction finishes. This reduces small files at the cost
of additional I/O. ``compaction_file_size`` sets the target size for compacted files; it is not a
hard size limit. If unspecified, the connector uses the rolling policy file size.
Automatic compaction is disabled by default.

Streaming writes
~~~~~~~~~~~~~~~~

Rolling policy parameters apply to streaming writes, not batch writes. File size and time
settings are rolling thresholds, not hard limits on output file size or visibility latency.
Continuous writes need checkpointing to finish pending files. Without automatic compaction,
JSON also needs a rolling condition to close the file. Parquet and writers with automatic
compaction enabled roll on checkpoints as well as on size/time thresholds.
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
