---
title: "Reading from Kafka"
is_beta: true
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<a href="#top"></a>

Interact with [Apache Kafka](https://kafka.apache.org/) streams from Flink's APIs.

* This will be replaced by the TOC
{:toc}


Kafka Connector
-----------

### Background

Flink provides special Kafka Connectors for reading and writing data to Kafka topics.
The Flink Kafka Consumer integrates with Flink's checkpointing mechanisms to provide different 
processing guarantees (most importantly exactly-once guarantees).

For exactly-once processing Flink can not rely on the auto-commit capabilities of the Kafka consumers.
The Kafka consumer might commit offsets to Kafka which have not been processed successfully.

Flink provides different connector implementations for different use-cases and environments.




### How to read data from Kafka

#### Choose appropriate package and class

Please pick a package (maven artifact id) and class name for your use-case and environment. For most users, the `flink-connector-kafka-083` package and the `FlinkKafkaConsumer082` class are appropriate.

| Package                     | Supported Since | Class | Kafka Version | Allows exactly once processing | Notes |
| -------------               |-------------| -----| ------ | ------ |
| flink-connector-kafka       | 0.9, 0.10 | `KafkaSource` | 0.8.1, 0.8.2 | **No**, does not participate in checkpointing at all. | Uses the old, high level KafkaConsumer API, autocommits to ZK by Kafka |
| flink-connector-kafka       | 0.9, 0.10 | `PersistentKafkaSource` | 0.8.1, 0.8.2 | **No**, does not guarantee exactly-once processing, element order or strict partition assignment | Uses the old, high level KafkaConsumer API, offsets are committed into ZK manually |
| flink-connector-kafka-083   | 0.10      | `FlinkKafkaConsumer081` | 0.8.1  | **yes** | Uses the [SimpleConsumer](https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example) API of Kafka internally. Offsets are committed to ZK manually |
| flink-connector-kafka-083   | 0.10      | `FlinkKafkaConsumer082` | 0.8.2  | **yes** | Uses the new, unreleased consumer API of Kafka 0.9.3 internally. Offsets are committed to ZK manually |
| flink-connector-kafka-083   | 0.10      | `FlinkKafkaConsumer083` | 0.8.3  | **yes** | **EXPERIMENTAL** Uses the new, unreleased consumer of Kafka 0.9.3. Offsets are committed using the Consumer API |


