---
title: Kinesis
weight: 5
type: docs
aliases:
  - /zh/dev/table/connectors/kinesis.html
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

# Amazon Kinesis Data Streams SQL Connector

{{< label "Scan Source: Unbounded" >}}
{{< label "Sink: Streaming Append Mode" >}}

The Kinesis connector allows for reading data from and writing data into [Amazon Kinesis Data Streams (KDS)](https://aws.amazon.com/kinesis/data-streams/).

Dependencies
------------

{{< sql_download_table "kinesis" >}}

How to create a Kinesis data stream table
-----------------------------------------

Follow the instructions from the [Amazon KDS Developer Guide](https://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one-create-stream.html) to set up a Kinesis stream.
The following example shows how to create a table backed by a Kinesis data stream:

```sql
CREATE TABLE KinesisTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `category_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3)
)
PARTITIONED BY (user_id, item_id)
WITH (
  'connector' = 'kinesis',
  'stream' = 'user_behavior',
  'aws.region' = 'us-east-2',
  'scan.stream.initpos' = 'LATEST',
  'format' = 'csv'
);
```

Available Metadata
------------------

The following metadata can be exposed as read-only (`VIRTUAL`) columns in a table definition.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">Key</th>
      <th class="text-center" style="width: 45%">Data Type</th>
      <th class="text-center" style="width: 35%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code><a href="https://docs.aws.amazon.com/kinesis/latest/APIReference/API_Record.html#Streams-Type-Record-ApproximateArrivalTimestamp">timestamp</a></code></td>
      <td><code>TIMESTAMP_LTZ(3) NOT NULL</code></td>
      <td>The approximate time when the record was inserted into the stream.</td>
    </tr>
    <tr>
      <td><code><a href="https://docs.aws.amazon.com/kinesis/latest/APIReference/API_Shard.html#Streams-Type-Shard-ShardId">shard-id</a></code></td>
      <td><code>VARCHAR(128) NOT NULL</code></td>
      <td>The unique identifier of the shard within the stream from which the record was read.</td>
    </tr>
    <tr>
      <td><code><a href="https://docs.aws.amazon.com/kinesis/latest/APIReference/API_Record.html#Streams-Type-Record-SequenceNumber">sequence-number</a></code></td>
      <td><code>VARCHAR(128) NOT NULL</code></td>
      <td>The unique identifier of the record within its shard.</td>
    </tr>
    </tbody>
</table>

The extended `CREATE TABLE` example demonstrates the syntax for exposing these metadata fields:

```sql
CREATE TABLE KinesisTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `category_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3),
  `arrival_time` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
  `shard_id` VARCHAR(128) NOT NULL METADATA FROM 'shard-id' VIRTUAL,
  `sequence_number` VARCHAR(128) NOT NULL METADATA FROM 'sequence-number' VIRTUAL
)
PARTITIONED BY (user_id, item_id)
WITH (
  'connector' = 'kinesis',
  'stream' = 'user_behavior',
  'aws.region' = 'us-east-2',
  'scan.stream.initpos' = 'LATEST',
  'format' = 'csv'
);
```


Connector Options
-----------------

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">Option</th>
      <th class="text-center" style="width: 8%">Required</th>
      <th class="text-center" style="width: 7%">Default</th>
      <th class="text-center" style="width: 10%">Type</th>
      <th class="text-center" style="width: 50%">Description</th>
    </tr>
    <tr>
      <th colspan="5" class="text-left" style="width: 100%">Common Options</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what connector to use. For Kinesis use <code>'kinesis'</code>.</td>
    </tr>
    <tr>
      <td><h5>stream</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the Kinesis data stream backing this table.</td>
    </tr>
    <tr>
      <td><h5>format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to deserialize and serialize Kinesis data stream records. See <a href="#data-type-mapping">Data Type Mapping</a> for details.</td>
    </tr>
    <tr>
      <td><h5>aws.region</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The AWS region where the stream is defined. Either this or <code>aws.endpoint</code> are required.</td>
    </tr>
    <tr>
      <td><h5>aws.endpoint</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The AWS endpoint for Kinesis (derived from the AWS region setting if not set). Either this or <code>aws.region</code> are required.</td>
    </tr>
    </tbody>
    <thead>
    <tr>
      <th colspan="5" class="text-left" style="width: 100%">Authentication Options</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>aws.credentials.provider</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">AUTO</td>
      <td>String</td>
      <td>A credentials provider to use when authenticating against the Kinesis endpoint. See <a href="#authentication">Authentication</a> for details.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.basic.accesskeyid</h5></td>
	  <td>optional</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The AWS access key ID to use when setting credentials provider type to BASIC.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.basic.secretkey</h5></td>
	  <td>optional</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The AWS secret key to use when setting credentials provider type to BASIC.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.profile.path</h5></td>
	  <td>optional</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>Optional configuration for profile path if credential provider type is set to be PROFILE.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.profile.name</h5></td>
	  <td>optional</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>Optional configuration for profile name if credential provider type is set to be PROFILE.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.arn</h5></td>
	  <td>optional</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The role ARN to use when credential provider type is set to ASSUME_ROLE or WEB_IDENTITY_TOKEN.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.sessionName</h5></td>
	  <td>optional</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The role session name to use when credential provider type is set to ASSUME_ROLE or WEB_IDENTITY_TOKEN.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.externalId</h5></td>
	  <td>optional</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The external ID to use when credential provider type is set to ASSUME_ROLE.</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.role.provider</h5></td>
	  <td>optional</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The credentials provider that provides credentials for assuming the role when credential provider type is set to ASSUME_ROLE. Roles can be nested, so this value can again be set to ASSUME_ROLE</td>
    </tr>
    <tr>
	  <td><h5>aws.credentials.webIdentityToken.file</h5></td>
	  <td>optional</td>
	  <td style="word-wrap: break-word;">(none)</td>
	  <td>String</td>
	  <td>The absolute path to the web identity token file that should be used if provider type is set to WEB_IDENTITY_TOKEN.</td>
    </tr>
    </tbody>
    <thead>
    <tr>
      <th colspan="5" class="text-left" style="width: 100%">Source Options</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>scan.stream.initpos</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">LATEST</td>
      <td>String</td>
      <td>Initial position to be used when reading from the table. See <a href="#start-reading-position">Start Reading Position</a> for details.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.initpos-timestamp</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The initial timestamp to start reading Kinesis stream from (when <code>scan.stream.initpos</code> is AT_TIMESTAMP). See <a href="#start-reading-position">Start Reading Position</a> for details.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.initpos-timestamp-format</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">yyyy-MM-dd'T'HH:mm:ss.SSSXXX</td>
      <td>String</td>
      <td>The date format of initial timestamp to start reading Kinesis stream from (when <code>scan.stream.initpos</code> is AT_TIMESTAMP). See <a href="#start-reading-position">Start Reading Position</a> for details.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.recordpublisher</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">POLLING</td>
      <td>String</td>
      <td>The <code>RecordPublisher</code> type to use for sources. See <a href="#enhanced-fan-out">Enhanced Fan-Out</a> for details.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.efo.consumername</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The name of the EFO consumer to register with KDS. See <a href="#enhanced-fan-out">Enhanced Fan-Out</a> for details.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.efo.registration</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">LAZY</td>
      <td>String</td>
      <td>Determine how and when consumer de-/registration is performed (LAZY|EAGER|NONE). See <a href="#enhanced-fan-out">Enhanced Fan-Out</a> for details.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.efo.consumerarn</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The prefix of consumer ARN for a given stream. See <a href="#enhanced-fan-out">Enhanced Fan-Out</a> for details.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.efo.http-client.max-concurrency</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>Integer</td>
      <td>Maximum number of allowed concurrent requests for the EFO client. See <a href="#enhanced-fan-out">Enhanced Fan-Out</a> for details.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describe.maxretries</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50</td>
      <td>Integer</td>
      <td>The maximum number of <code>describeStream</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describe.backoff.base</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">2000</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between each <code>describeStream</code> attempt (for consuming from DynamoDB streams).</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describe.backoff.max</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds)  between each <code>describeStream</code> attempt (for consuming from DynamoDB streams).</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describe.backoff.expconst</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>describeStream</code> attempt (for consuming from DynamoDB streams).</td>
    </tr>
    <tr>
      <td><h5>scan.list.shards.maxretries</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>The maximum number of <code>listShards</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.list.shards.backoff.base</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between each <code>listShards</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.list.shards.backoff.max</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between each <code>listShards</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.list.shards.backoff.expconst</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>listShards</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describestreamconsumer.maxretries</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">50</td>
      <td>Integer</td>
      <td>The maximum number of <code>describeStreamConsumer</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describestreamconsumer.backoff.base</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">2000</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between each <code>describeStreamConsumer</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describestreamconsumer.backoff.max</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between each <code>describeStreamConsumer</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.describestreamconsumer.backoff.expconst</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>describeStreamConsumer</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.registerstreamconsumer.maxretries</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>The maximum number of <code>registerStream</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.registerstreamconsumer.timeout</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">60</td>
      <td>Integer</td>
      <td>The maximum time in seconds to wait for a stream consumer to become active before giving up.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.registerstreamconsumer.backoff.base</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">500</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between each <code>registerStream</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.registerstreamconsumer.backoff.max</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">2000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between each <code>registerStream</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.registerstreamconsumer.backoff.expconst</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>registerStream</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.deregisterstreamconsumer.maxretries</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>The maximum number of <code>deregisterStream</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.deregisterstreamconsumer.timeout</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">60</td>
      <td>Integer</td>
      <td>The maximum time in seconds to wait for a stream consumer to deregister before giving up.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.deregisterstreamconsumer.backoff.base</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">500</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between each <code>deregisterStream</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.deregisterstreamconsumer.backoff.max</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">2000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between each <code>deregisterStream</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.stream.deregisterstreamconsumer.backoff.expconst</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>deregisterStream</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.subscribetoshard.maxretries</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10</td>
      <td>Integer</td>
      <td>The maximum number of <code>subscribeToShard</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.subscribetoshard.backoff.base</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between each <code>subscribeToShard</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.subscribetoshard.backoff.max</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">2000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between each <code>subscribeToShard</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.subscribetoshard.backoff.expconst</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>subscribeToShard</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getrecords.maxrecordcount</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>Integer</td>
      <td>The maximum number of records to try to get each time we fetch records from a AWS Kinesis shard.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getrecords.maxretries</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The maximum number of <code>getRecords</code> attempts if we get a recoverable exception.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getrecords.backoff.base</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">300</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between <code>getRecords</code> attempts if we get a ProvisionedThroughputExceededException.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getrecords.backoff.max</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between <code>getRecords</code> attempts if we get a ProvisionedThroughputExceededException.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getrecords.backoff.expconst</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>getRecords</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getrecords.intervalmillis</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">200</td>
      <td>Long</td>
      <td>The interval (in milliseconds) between each <code>getRecords</code> request to a AWS Kinesis shard in milliseconds.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getiterator.maxretries</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The maximum number of <code>getShardIterator</code> attempts if we get ProvisionedThroughputExceededException.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getiterator.backoff.base</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">300</td>
      <td>Long</td>
      <td>The base backoff time (in milliseconds) between <code>getShardIterator</code> attempts if we get a ProvisionedThroughputExceededException.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getiterator.backoff.max</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Long</td>
      <td>The maximum backoff time (in milliseconds) between <code>getShardIterator</code> attempts if we get a ProvisionedThroughputExceededException.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.getiterator.backoff.expconst</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1.5</td>
      <td>Double</td>
      <td>The power constant for exponential backoff between each <code>getShardIterator</code> attempt.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.discovery.intervalmillis</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>Integer</td>
      <td>The interval between each attempt to discover new shards.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.adaptivereads</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>The config to turn on adaptive reads from a shard. See the <code>AdaptivePollingRecordPublisher</code> documentation for details.</td>
    </tr>
    <tr>
      <td><h5>scan.shard.idle.interval</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">-1</td>
      <td>Long</td>
      <td>The interval (in milliseconds) after which to consider a shard idle for purposes of watermark generation. A positive value will allow the watermark to progress even when some shards don't receive new records.</td>
    </tr>
    <tr>
      <td><h5>scan.watermark.sync.interval</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30000</td>
      <td>Long</td>
      <td>The interval (in milliseconds) for periodically synchronizing the shared watermark state.</td>
    </tr>
    <tr>
      <td><h5>scan.watermark.lookahead.millis</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">0</td>
      <td>Long</td>
      <td>The maximum delta (in milliseconds) allowed for the reader to advance ahead of the shared global watermark.</td>
    </tr>
    <tr>
      <td><h5>scan.watermark.sync.queue.capacity</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">100</td>
      <td>Integer</td>
      <td>The maximum number of records that will be buffered before suspending consumption of a shard.</td>
    </tr>
    </tbody>
    <thead>
    <tr>
      <th colspan="5" class="text-left" style="width: 100%">Sink Options</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>sink.partitioner</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">random or row-based</td>
      <td>String</td>
      <td>Optional output partitioning from Flink's partitions into Kinesis shards. See <a href="#sink-partitioning">Sink Partitioning</a> for details.</td>
    </tr>
    <tr>
      <td><h5>sink.partitioner-field-delimiter</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">|</td>
      <td>String</td>
      <td>Optional field delimiter for a fields-based partitioner derived from a PARTITION BY clause. See <a href="#sink-partitioning">Sink Partitioning</a> for details.</td>
    </tr>
    <tr>
      <td><h5>sink.producer.*</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td></td>
      <td>
        Sink options for the <code>KinesisProducer</code>.
        Suffix names must match the <a href="https://javadoc.io/static/com.amazonaws/amazon-kinesis-producer/0.14.0/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.html">KinesisProducerConfiguration</a> setters in lower-case hyphenated style (for example, <code>sink.producer.collection-max-count</code> or <code>sink.producer.aggregation-max-count</code>).
        The transformed action keys are passed to the <code>sink.producer.*</code> to <a href="https://javadoc.io/static/com.amazonaws/amazon-kinesis-producer/0.14.0/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.html#fromProperties-java.util.Properties-">KinesisProducerConfigurations#fromProperties</a>.
        Note that some of the defaults are overwritten by <code>KinesisConfigUtil</code>.
      </td>
    </tr>
    </tbody>
</table>

Features
--------

### Authorization

Make sure to [create an appropriate IAM policy](https://docs.aws.amazon.com/streams/latest/dev/controlling-access.html) to allow reading from / writing to the Kinesis data streams.

### Authentication

Depending on your deployment you would choose a different Credentials Provider to allow access to Kinesis.
By default, the `AUTO` Credentials Provider is used.
If the access key ID and secret key are set in the deployment configuration, this results in using the `BASIC` provider.

A specific [AWSCredentialsProvider](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html?com/amazonaws/auth/AWSCredentialsProvider.html) can be **optionally** set using the `aws.credentials.provider` setting.
Supported values are:

* `AUTO` - Use the default AWS Credentials Provider chain that searches for credentials in the following order: `ENV_VARS`, `SYS_PROPS`, `WEB_IDENTITY_TOKEN`, `PROFILE`, and EC2/ECS credentials provider.
* `BASIC` - Use access key ID and secret key supplied as configuration.
* `ENV_VAR` - Use `AWS_ACCESS_KEY_ID` & `AWS_SECRET_ACCESS_KEY` environment variables.
* `SYS_PROP` - Use Java system properties `aws.accessKeyId` and `aws.secretKey`.
* `PROFILE` - Use an AWS credentials profile to create the AWS credentials.
* `ASSUME_ROLE` - Create AWS credentials by assuming a role. The credentials for assuming the role must be supplied.
* `WEB_IDENTITY_TOKEN` - Create AWS credentials by assuming a role using Web Identity Token.

### Start Reading Position

You can configure table sources to start reading a table-backing Kinesis data stream from a specific position through the `scan.stream.initpos` option.
Available values are:

* `LATEST`: read shards starting from the latest record.
* `TRIM_HORIZON`: read shards starting from the earliest record possible (data may be trimmed by Kinesis depending on the current retention settings of the backing stream).
* `AT_TIMESTAMP`: read shards starting from a specified timestamp. The timestamp value should be specified through the `scan.stream.initpos-timestamp` in one of the following formats:
   * A non-negative double value representing the number of seconds that has elapsed since the Unix epoch (for example, `1459799926.480`).
   * A value conforming to a user-defined `SimpleDateFormat` specified at `scan.stream.initpos-timestamp-format`.
     If a user does not define a format, the default pattern will be `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`.
     For example, timestamp value is `2016-04-04` and user-defined format is `yyyy-MM-dd`, or timestamp value is `2016-04-04T19:58:46.480-00:00` and a user-defined format is not provided.

### Sink Partitioning

Kinesis data streams consist of one or more shards, and the `sink.partitioner` option allows you to control how records written into a multi-shard Kinesis-backed table will be partitioned between its shards.
Valid values are:

* `fixed`: Kinesis `PartitionKey` values derived from the Flink subtask index, so each Flink partition ends up in at most one Kinesis partition (assuming that no re-sharding takes place at runtime).
* `random`: Kinesis `PartitionKey` values are assigned randomly. This is the default value for tables not defined with a `PARTITION BY` clause.
* Custom `FixedKinesisPartitioner` subclass: e.g. `'org.mycompany.MyPartitioner'`.

{{< hint info >}}
Records written into tables defining a `PARTITION BY` clause will always be partitioned based on a concatenated projection of the `PARTITION BY` fields.
In this case, the `sink.partitioner` field cannot be used to modify this behavior (attempting to do this results in a configuration error).
You can, however, use the `sink.partitioner-field-delimiter` option to set the delimiter of field values in the concatenated [PartitionKey](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html#Streams-PutRecord-request-PartitionKey) string (an empty string is also a valid delimiter).
{{< /hint >}}

### Enhanced Fan-Out

[Enhanced Fan-Out (EFO)](https://aws.amazon.com/blogs/aws/kds-enhanced-fanout/) increases the maximum number of concurrent consumers per Kinesis data stream.
Without EFO, all concurrent Kinesis consumers share a single read quota per shard.
Using EFO, each consumer gets a distinct dedicated read quota per shard, allowing read throughput to scale with the number of consumers.

<span class="label label-info">Note</span> Using EFO will [incur additional cost](https://aws.amazon.com/kinesis/data-streams/pricing/).

You can enable and configure EFO with the following properties:

* `scan.stream.recordpublisher`: Determines whether to use `EFO` or `POLLING`.
* `scan.stream.efo.consumername`: A name to identify the consumer when the above value is `EFO`.
* `scan.stream.efo.registration`: Strategy for (de-)registration  of `EFO` consumers with the name given by the `scan.stream.efo.consumername` value. Valid strategies are:
  * `LAZY` (default): Stream consumers are registered when the Flink job starts running.
    If the stream consumer already exists, it will be reused.
    This is the preferred strategy for the majority of applications.
    However, jobs with parallelism greater than 1 will result in tasks competing to register and acquire the stream consumer ARN.
    For jobs with very large parallelism this can result in an increased start-up time.
    The describe operation has a limit of 20 [transactions per second](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamConsumer.html),
    this means application startup time will increase by roughly `parallelism/20 seconds`.
  * `EAGER`: Stream consumers are registered in the `FlinkKinesisConsumer` constructor.
    If the stream consumer already exists, it will be reused.
    This will result in registration occurring when the job is constructed,
    either on the Flink Job Manager or client environment submitting the job.
    Using this strategy results in a single thread registering and retrieving the stream consumer ARN,
    reducing startup time over `LAZY` (with large parallelism).
    However, consider that the client environment will require access to the AWS services.
  * `NONE`: Stream consumer registration is not performed by `FlinkKinesisConsumer`.
    Registration must be performed externally using the [AWS CLI or SDK](https://aws.amazon.com/tools/)
    to invoke [RegisterStreamConsumer](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_RegisterStreamConsumer.html).
    Stream consumer ARNs should be provided to the job via the consumer configuration.
* `scan.stream.efo.consumerarn.<stream-name>`: ARNs identifying externally registered ARN-consumers (substitute `<stream-name>` with the name of your stream in the parameter name).
   Use this if you choose to use `NONE` as a `scan.stream.efo.registration` strategy.

<span class="label label-info">Note</span> For a given Kinesis data stream, each EFO consumer must have a unique name.
However, consumer names do not have to be unique across data streams.
Reusing a consumer name will result in existing subscriptions being terminated.

<span class="label label-info">Note</span> With the `LAZY` and `EAGER` strategies, stream consumers are de-registered when the job is shutdown gracefully.
In the event that a job terminates within executing the shutdown hooks, stream consumers will remain active.
In this situation the stream consumers will be gracefully reused when the application restarts.
With the `NONE` strategy, stream consumer de-registration is not performed by `FlinkKinesisConsumer`.

Data Type Mapping
----------------

Kinesis stores records as Base64-encoded binary data objects, so it doesn't have a notion of internal record structure.
Instead, Kinesis records are deserialized and serialized by formats, e.g. 'avro', 'csv', or 'json'.
To determine the data type of the messages in your Kinesis-backed tables, pick a suitable Flink format with the `format` keyword.
Please refer to the [Formats]({{< ref "docs/connectors/table/formats/overview" >}}) pages for more details.

{{< top >}}
