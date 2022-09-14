---
title: Firehose
weight: 5
type: docs
aliases:
- /dev/table/connectors/firehose.html
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

# Amazon Kinesis Data Firehose SQL Connector

{{< label "Sink: Streaming Append Mode" >}}

The Kinesis Data Firehose connector allows for writing data into [Amazon Kinesis Data Firehose (KDF)](https://aws.amazon.com/kinesis/data-firehose/).

Dependencies
------------

{{< sql_download_table "aws-kinesis-firehose" >}}

How to create a Kinesis Data Firehose table
-----------------------------------------

Follow the instructions from the [Amazon Kinesis Data Firehose Developer Guide](https://docs.aws.amazon.com/ses/latest/dg/event-publishing-kinesis-analytics-firehose-stream.html) to set up a Kinesis Data Firehose delivery stream.
The following example shows how to create a table backed by a Kinesis Data Firehose delivery stream with minimum required options:

```sql
CREATE TABLE FirehoseTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `category_id` BIGINT,
  `behavior` STRING
)
WITH (
  'connector' = 'firehose',
  'delivery-stream' = 'user_behavior',
  'aws.region' = 'us-east-2',
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
      <td>Specify what connector to use. For Kinesis Data Firehose use <code>'firehose'</code>.</td>
    </tr>
    <tr>
      <td><h5>delivery-stream</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the Kinesis Data Firehose delivery stream backing this table.</td>
    </tr>
    <tr>
      <td><h5>format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to deserialize and serialize Kinesis Data Firehose records. See <a href="#data-type-mapping">Data Type Mapping</a> for details.</td>
    </tr>
    <tr>
      <td><h5>aws.region</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The AWS region where the delivery stream is defined. This option is required for <code>KinesisFirehoseSink</code> creation.</td>
    </tr>
    <tr>
      <td><h5>aws.endpoint</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The AWS endpoint for Amazon Kinesis Data Firehose.</td>
    </tr>
    <tr>
      <td><h5>aws.trust.all.certificates</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>If true accepts all SSL certificates.</td>
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
      <th colspan="5" class="text-left" style="width: 100%">Sink Options</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>sink.http-client.max-concurrency</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>Integer</td>
      <td>
      Maximum number of allowed concurrent requests by <code>FirehoseAsyncClient</code> to be delivered to delivery stream.
      </td>
    </tr>
    <tr>
      <td><h5>sink.http-client.read-timeout</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">360000</td>
      <td>Integer</td>
      <td>
        Maximum amount of time in ms for requests to be sent by <code>FirehoseAsyncClient</code> to delivery stream before failure.
      </td>
    </tr>
    <tr>
      <td><h5>sink.http-client.protocol.version</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">HTTP2</td>
      <td>String</td>
      <td>Http version used by <code>FirehoseAsyncClient</code>.</td>
    </tr>
    <tr>
      <td><h5>sink.batch.max-size</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">500</td>
      <td>Integer</td>
      <td>Maximum batch size of elements to be passed to <code>FirehoseAsyncClient</code> to be written downstream to delivery stream.</td>
    </tr>
    <tr>
      <td><h5>sink.requests.max-inflight</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">16</td>
      <td>Integer</td>
      <td>Request threshold for uncompleted requests by <code>FirehoseAsyncClient</code>before blocking new write requests.</td>
    </tr>
    <tr>
      <td><h5>sink.requests.max-buffered</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>String</td>
      <td>request buffer threshold by <code>FirehoseAsyncClient</code> before blocking new write requests.</td>
    </tr>
    <tr>
      <td><h5>sink.flush-buffer.size</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5242880</td>
      <td>Long</td>
      <td>Threshold value in bytes for writer buffer in <code>FirehoseAsyncClient</code> before flushing.</td>
    </tr>
    <tr>
      <td><h5>sink.flush-buffer.timeout</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">5000</td>
      <td>Long</td>
      <td>Threshold time in ms for an element to be in a buffer of <code>FirehoseAsyncClient</code> before flushing.</td>
    </tr>
    <tr>
      <td><h5>sink.fail-on-error</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Flag used for retrying failed requests. If set any request failure will not be retried and will fail the job.</td>
    </tr>
    </tbody>
</table>

## Authorization

Make sure to [create an appropriate IAM policy](https://docs.aws.amazon.com/firehose/latest/dev/controlling-access.html) to allow reading writing to the Kinesis Data Firehose delivery stream.

## Authentication

Depending on your deployment you would choose a different Credentials Provider to allow access to Kinesis Data Firehose.
By default, the `AUTO` Credentials Provider is used.
If the access key ID and secret key are set in the deployment configuration, this results in using the `BASIC` provider.

A specific [AWSCredentialsProvider](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/index.html?com/amazonaws/auth/AWSCredentialsProvider.html) can be **optionally** set using the `aws.credentials.provider` setting.
Supported values are:

- `AUTO` - Use the default AWS Credentials Provider chain that searches for credentials in the following order: `ENV_VARS`, `SYS_PROPS`, `WEB_IDENTITY_TOKEN`, `PROFILE`, and EC2/ECS credentials provider.
- `BASIC` - Use access key ID and secret key supplied as configuration.
- `ENV_VAR` - Use `AWS_ACCESS_KEY_ID` & `AWS_SECRET_ACCESS_KEY` environment variables.
- `SYS_PROP` - Use Java system properties `aws.accessKeyId` and `aws.secretKey`.
- `PROFILE` - Use an AWS credentials profile to create the AWS credentials.
- `ASSUME_ROLE` - Create AWS credentials by assuming a role. The credentials for assuming the role must be supplied.
- `WEB_IDENTITY_TOKEN` - Create AWS credentials by assuming a role using Web Identity Token.

## Data Type Mapping

Kinesis Data Firehose stores records as Base64-encoded binary data objects, so it doesn't have a notion of internal record structure.
Instead, Kinesis Data Firehose records are deserialized and serialized by formats, e.g. 'avro', 'csv', or 'json'.
To determine the data type of the messages in your Kinesis Data Firehose backed tables, pick a suitable Flink format with the `format` keyword.
Please refer to the [Formats]({{< ref "docs/connectors/table/formats/overview" >}}) pages for more details.

## Notice

The current implementation for the Kinesis Data Firehose SQL connector only supports Kinesis Data Firehose backed sinks and doesn't provide an implementation for source queries.
Queries similar to:
```sql
SELECT * FROM FirehoseTable;
```
should result in an error similar to
```
Connector firehose can only be used as a sink. It cannot be used as a source.
```
{{< top >}}
