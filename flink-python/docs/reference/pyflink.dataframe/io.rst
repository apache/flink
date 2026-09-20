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
They read existing files once unless ``monitor_interval`` is set to discover new files
continuously. ``path_regex_pattern`` optionally filters the source file paths.
The connector and format JARs must be available to Flink. Parquet also requires
Hadoop libraries on the classpath, which can be provided through ``HADOOP_CLASSPATH``.

For example, convert JSON records to compressed Parquet files::

    import pyflink.dataframe as pf

    pf.config.set("execution.runtime-mode", "batch")
    events = pf.read_json(
        "file:///tmp/events.json",
        schema={"id": pf.DataType.int64(), "event": pf.DataType.string()},
        format_options={"ignore-parse-errors": "true"},
    )
    events.write_parquet("file:///tmp/events-parquet", compression="GZIP")

The writers default to ``mode="overwrite"`` and replace existing output data through
Flink's filesystem sink. Use ``mode="append"`` to retain existing files. Overwrite is
supported only in batch execution; streaming execution must use append mode. Configure
``execution.runtime-mode`` before creating the DataFrame environment. Writes wait
for completion with local or MiniCluster execution, just like ``write_generic``.

Rolling policy and partition commit parameters map to the corresponding
``sink.rolling-policy.*`` and ``sink.partition-commit.*`` connector options. Parquet
``compression`` maps to ``parquet.compression``. JSON ``format_options`` accepts string
keys and values, with or without the ``json.`` prefix; specifying the same option in
both forms is rejected. These options do not override the filesystem path or format.
