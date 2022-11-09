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

.. currentmodule:: pyflink.datastream.connectors

.. autosummary::
    :toctree: api/

    FileEnumeratorProvider
    FileSplitAssignerProvider
    StreamFormat
    FileSourceBuilder
    FileSource


File Sink
---------

.. currentmodule:: pyflink.datastream.connectors

.. autosummary::
    :toctree: api/

    BucketAssigner
    RollingPolicy
    OutputFileConfig
    FileSink
    StreamingFileSink


Number Sequence
===============

.. currentmodule:: pyflink.datastream.connectors

.. autosummary::
    :toctree: api/

    NumberSequenceSource


Kafka
=====

Kakfa Producer and Consumer
---------------------------

.. currentmodule:: pyflink.datastream.connectors

.. autosummary::
    :toctree: api/

    FlinkKafkaConsumer
    FlinkKafkaProducer


Pulsar
======

Pulsar Source
-------------

.. currentmodule:: pyflink.datastream.connectors

.. autosummary::
    :toctree: api/

    PulsarDeserializationSchema
    SubscriptionType
    StartCursor
    StopCursor
    PulsarSource
    PulsarSourceBuilder

Jdbc
====

.. currentmodule:: pyflink.datastream.connectors

.. autosummary::
    :toctree: api/

    JdbcSink
    JdbcConnectionOptions
    JdbcExecutionOptions


RMQ
===

.. currentmodule:: pyflink.datastream.connectors

.. autosummary::
    :toctree: api/

    RMQConnectionConfig
    RMQSource
    RMQSink

