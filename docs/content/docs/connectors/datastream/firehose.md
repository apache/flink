---
title: Firehose
weight: 5
type: docs
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

# Amazon Kinesis Data Firehose Sink

The Firehose sink writes to [Amazon Kinesis Data Firehose](https://aws.amazon.com/kinesis/data-firehose/).

Follow the instructions from the [Amazon Kinesis Data Firehose Developer Guide](https://docs.aws.amazon.com/firehose/latest/dev/basic-create.html)
to setup a Kinesis Data Firehose delivery stream.

To use the connector, add the following Maven dependency to your project:

{{< artifact flink-connector-aws-kinesis-firehose >}}

{{< py_download_link "aws-kinesis-firehose" >}}

The `KinesisFirehoseSink` uses [AWS v2 SDK for Java](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html) to write data from a Flink stream into a Firehose delivery stream.

{{< tabs "42vs28vdth5-nm76-6dz1-5m7s-5y345bu56se5u66je" >}}
{{< tab "Java" >}}
```java
Properties sinkProperties = new Properties();
// Required
sinkProperties.put(AWSConfigConstants.AWS_REGION, "eu-west-1");
// Optional, provide via alternative routes e.g. environment variables
sinkProperties.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
sinkProperties.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");

KinesisFirehoseSink<String> kdfSink =
    KinesisFirehoseSink.<String>builder()
        .setFirehoseClientProperties(sinkProperties)      // Required
        .setSerializationSchema(new SimpleStringSchema()) // Required
        .setDeliveryStreamName("your-stream-name")        // Required
        .setFailOnError(false)                            // Optional
        .setMaxBatchSize(500)                             // Optional
        .setMaxInFlightRequests(50)                       // Optional
        .setMaxBufferedRequests(10_000)                   // Optional
        .setMaxBatchSizeInBytes(4 * 1024 * 1024)          // Optional
        .setMaxTimeInBufferMS(5000)                       // Optional
        .setMaxRecordSizeInBytes(1000 * 1024)             // Optional
        .build();

flinkStream.sinkTo(kdfSink);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
Properties sinkProperties = new Properties()
// Required
sinkProperties.put(AWSConfigConstants.AWS_REGION, "eu-west-1")
// Optional, provide via alternative routes e.g. environment variables
sinkProperties.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
sinkProperties.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")

val kdfSink =
    KinesisFirehoseSink.<String>builder()
        .setFirehoseClientProperties(sinkProperties)      // Required
        .setSerializationSchema(new SimpleStringSchema()) // Required
        .setDeliveryStreamName("your-stream-name")        // Required
        .setFailOnError(false)                            // Optional
        .setMaxBatchSize(500)                             // Optional
        .setMaxInFlightRequests(50)                       // Optional
        .setMaxBufferedRequests(10_000)                   // Optional
        .setMaxBatchSizeInBytes(4 * 1024 * 1024)          // Optional
        .setMaxTimeInBufferMS(5000)                       // Optional
        .setMaxRecordSizeInBytes(1000 * 1024)             // Optional
        .build()

flinkStream.sinkTo(kdfSink)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
sink_properties = {
    # Required
    'aws.region': 'eu-west-1',
    # Optional, provide via alternative routes e.g. environment variables
    'aws.credentials.provider.basic.accesskeyid': 'aws_access_key_id',
    'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key'
}

kdf_sink = KinesisFirehoseSink.builder() \
    .set_firehose_client_properties(sink_properties) \     # Required
    .set_serialization_schema(SimpleStringSchema()) \      # Required
    .set_delivery_stream_name('your-stream-name') \        # Required
    .set_fail_on_error(False) \                            # Optional
    .set_max_batch_size(500) \                             # Optional
    .set_max_in_flight_requests(50) \                      # Optional
    .set_max_buffered_requests(10000) \                    # Optional
    .set_max_batch_size_in_bytes(5 * 1024 * 1024) \        # Optional
    .set_max_time_in_buffer_ms(5000) \                     # Optional
    .set_max_record_size_in_bytes(1 * 1024 * 1024) \       # Optional
    .build()
```
{{< /tab >}}
{{< /tabs >}}

## Configurations

Flink's Firehose sink is created by using the static builder `KinesisFirehoseSink.<InputType>builder()`.

1. __setFirehoseClientProperties(Properties sinkProperties)__
    * Required.
    * Supplies credentials, region and other parameters to the Firehose client.
2. __setSerializationSchema(SerializationSchema<InputType> serializationSchema)__
    * Required.
    * Supplies a serialization schema to the Sink. This schema is used to serialize elements before sending to Firehose.
3. __setDeliveryStreamName(String deliveryStreamName)__
    * Required.
    * Name of the delivery stream to sink to.
4. _setFailOnError(boolean failOnError)_
    * Optional. Default: `false`.
    * Whether failed requests to write records to Firehose are treated as fatal exceptions in the sink.
5. _setMaxBatchSize(int maxBatchSize)_
    * Optional. Default: `500`.
    * Maximum size of a batch to write to Firehose.
6. _setMaxInFlightRequests(int maxInFlightRequests)_
    * Optional. Default: `50`.
    * The maximum number of in flight requests allowed before the sink applies backpressure.
7. _setMaxBufferedRequests(int maxBufferedRequests)_
    * Optional. Default: `10_000`.
    * The maximum number of records that may be buffered in the sink before backpressure is applied. 
8. _setMaxBatchSizeInBytes(int maxBatchSizeInBytes)_
    * Optional. Default: `4 * 1024 * 1024`.
    * The maximum size (in bytes) a batch may become. All batches sent will be smaller than or equal to this size.
9. _setMaxTimeInBufferMS(int maxTimeInBufferMS)_
    * Optional. Default: `5000`.
    * The maximum time a record may stay in the sink before being flushed.
10. _setMaxRecordSizeInBytes(int maxRecordSizeInBytes)_
    * Optional. Default: `1000 * 1024`.
    * The maximum record size that the sink will accept, records larger than this will be automatically rejected.
11. _build()_
    * Constructs and returns the Firehose sink.


## Using Custom Firehose Endpoints

It is sometimes desirable to have Flink operate as a consumer or producer against a Firehose VPC endpoint or a non-AWS
Firehose endpoint such as [Localstack](https://localstack.cloud/); this is especially useful when performing
functional testing of a Flink application. The AWS endpoint that would normally be inferred by the AWS region set in the
Flink configuration must be overridden via a configuration property.

To override the AWS endpoint, set the `AWSConfigConstants.AWS_ENDPOINT` and `AWSConfigConstants.AWS_REGION` properties. The region will be used to sign the endpoint URL.

{{< tabs "bcadd466-8416-4d3c-a6a7-c46eee0cbd4a" >}}
{{< tab "Java" >}}
```java
Properties producerConfig = new Properties();
producerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1");
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
producerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4566");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val producerConfig = new Properties()
producerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1")
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
producerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4566")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
producer_config = {
    'aws.region': 'us-east-1',
    'aws.credentials.provider.basic.accesskeyid': 'aws_access_key_id',
    'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key',
    'aws.endpoint': 'http://localhost:4566'
}
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
