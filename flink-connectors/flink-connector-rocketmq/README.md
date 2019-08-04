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

# Flink Connector for RocketMQ

This module is a implementation for the message queue middleware 'RocketMQ'.The MQ now is belongs to ASF and born in Alibaba,You can refer to this module to make connection between Flink and RocketMQ cluster
## Quick Start

The `Example.java` can be used to learn how to use Flink to connect RocketMQ cluster.

There are the steps:
1. Create a `StreamExecutionEnvironment` obejct likes the common starting
2. Create a `ConsumerConfig` obejct and fill all the needed configuration
3. Create a `RocketMQSource` as the fink source base on the previous `ConsumerConfig`
4. Create a `ProducerConfig` and new a `RocketMQSink` as flink sink
5. Integrate the `RocketMQSource` and `RocketMQSink` into flink DataStreamSource
6. run `StreamExecutionEnvironment.execute()`


## Road Map

* For this first commit is the initialize version for this module,and just implement some entry level functions.and the version for rocketmq just only support the `version 3.2.6`
* The next step will ehance the connector to support options configuration,enhance the unit test
* Finally will support multiple rocketmq version and support ASF RocketMQ as well.

## Contact

Any suggestion and question are welcome.and we can improve this project together.
* GitHub `mikiaichiyu`
* Mail <13560102795@139.com>
