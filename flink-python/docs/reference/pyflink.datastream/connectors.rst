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


==========
Connectors
==========

File System
===========

File Source
-----------

.. currentmodule:: pyflink.datastream.connectors.file_system

.. autosummary::
    :toctree: api/

    FileEnumeratorProvider
    FileSplitAssignerProvider
    StreamFormat
    BulkFormat
    FileSourceBuilder
    FileSource


File Sink
---------

.. currentmodule:: pyflink.datastream.connectors.file_system

.. autosummary::
    :toctree: api/

    BucketAssigner
    RollingPolicy
    DefaultRollingPolicy
    OnCheckpointRollingPolicy
    OutputFileConfig
    FileCompactStrategy
    FileCompactor
    FileSink
    StreamingFileSink


Number Sequence
===============

.. currentmodule:: pyflink.datastream.connectors.number_seq

.. autosummary::
    :toctree: api/

    NumberSequenceSource


Kafka
=====

Kakfa Producer and Consumer
---------------------------

.. currentmodule:: pyflink.datastream.connectors.kafka

.. autosummary::
    :toctree: api/

    FlinkKafkaConsumer
    FlinkKafkaProducer
    Semantic


Kafka Source and Sink
---------------------------

.. currentmodule:: pyflink.datastream.connectors.kafka

.. autosummary::
    :toctree: api/

    KafkaSource
    KafkaSourceBuilder
    KafkaTopicPartition
    KafkaOffsetResetStrategy
    KafkaOffsetsInitializer
    KafkaSink
    KafkaSinkBuilder
    KafkaRecordSerializationSchema
    KafkaRecordSerializationSchemaBuilder
    KafkaTopicSelector


kinesis
=======

Kinesis Source
--------------

.. currentmodule:: pyflink.datastream.connectors.kinesis

.. autosummary::
    :toctree: api/

    KinesisShardAssigner
    KinesisDeserializationSchema
    WatermarkTracker
    FlinkKinesisConsumer


Kinesis Sink
------------

.. currentmodule:: pyflink.datastream.connectors.kinesis

.. autosummary::
    :toctree: api/

    PartitionKeyGenerator
    KinesisStreamsSink
    KinesisStreamsSinkBuilder
    KinesisFirehoseSink
    KinesisFirehoseSinkBuilder


Pulsar
======

Pulsar Source
-------------

.. currentmodule:: pyflink.datastream.connectors.pulsar

.. autosummary::
    :toctree: api/

    StartCursor
    StopCursor
    RangeGenerator
    PulsarSource
    PulsarSourceBuilder


Pulsar Sink
-----------

.. currentmodule:: pyflink.datastream.connectors.pulsar

.. autosummary::
    :toctree: api/

    TopicRoutingMode
    MessageDelayer
    PulsarSink
    PulsarSinkBuilder


Jdbc
====

.. currentmodule:: pyflink.datastream.connectors.jdbc

.. autosummary::
    :toctree: api/

    JdbcSink
    JdbcConnectionOptions
    JdbcExecutionOptions


RMQ
===

.. currentmodule:: pyflink.datastream.connectors.rabbitmq

.. autosummary::
    :toctree: api/

    RMQConnectionConfig
    RMQSource
    RMQSink


Elasticsearch
=============

.. currentmodule:: pyflink.datastream.connectors.elasticsearch

.. autosummary::
    :toctree: api/

    FlushBackoffType
    ElasticsearchEmitter
    Elasticsearch6SinkBuilder
    Elasticsearch7SinkBuilder
    ElasticsearchSink


Cassandra
=========

.. currentmodule:: pyflink.datastream.connectors.cassandra

.. autosummary::
    :toctree: api/

    CassandraSink
    ConsistencyLevel
    MapperOptions
    ClusterBuilder
    CassandraCommitter
    CassandraFailureHandler
