---
title: DynamoDB
weight: 5
type: docs
aliases:
- /dev/connectors/dynamodb.html
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

# Amazon DynamoDB Sink

The DynamoDB sink writes to [Amazon DynamoDB](https://aws.amazon.com/dynamodb) using the [AWS v2 SDK for Java](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html). Follow the instructions from the [Amazon DynamoDB Developer Guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/getting-started-step-1.html)
to setup a table.

To use the connector, add the following Maven dependency to your project:

{{< connector_artifact flink-connector-dynamodb 3.0.0 >}}

{{< tabs "ec24a4ae-6a47-11ed-a1eb-0242ac120002" >}}
{{< tab "Java" >}}
```java
Properties sinkProperties = new Properties();
// Required
sinkProperties.put(AWSConfigConstants.AWS_REGION, "eu-west-1");
// Optional, provide via alternative routes e.g. environment variables
sinkProperties.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
sinkProperties.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");

ElementConverter<InputType, DynamoDbWriteRequest> elementConverter = new CustomElementConverter();

DynamoDbSink<String> dynamoDbSink = 
    DynamoDbSink.<InputType>builder()
        .setDynamoDbProperties(sinkProperties)              // Required
        .setTableName("my-dynamodb-table")                  // Required
        .setElementConverter(elementConverter)              // Required
        .setOverwriteByPartitionKeys(singletonList("key"))  // Optional  
        .setFailOnError(false)                              // Optional
        .setMaxBatchSize(25)                                // Optional
        .setMaxInFlightRequests(50)                         // Optional
        .setMaxBufferedRequests(10_000)                     // Optional
        .setMaxTimeInBufferMS(5000)                         // Optional
        .build();

flinkStream.sinkTo(dynamoDbSink);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val sinkProperties = new Properties()
// Required
sinkProperties.put(AWSConfigConstants.AWS_REGION, "eu-west-1")
// Optional, provide via alternative routes e.g. environment variables
sinkProperties.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
sinkProperties.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")

val elementConverter = new CustomElementConverter();

val dynamoDbSink =
    DynamoDbSink.<InputType>builder()
        .setDynamoDbProperties(sinkProperties)              // Required
        .setTableName("my-dynamodb-table")                  // Required
        .setElementConverter(elementConverter)              // Required
        .setOverwriteByPartitionKeys(singletonList("key"))  // Optional    
        .setFailOnError(false)                              // Optional
        .setMaxBatchSize(25)                                // Optional
        .setMaxInFlightRequests(50)                         // Optional
        .setMaxBufferedRequests(10_000)                     // Optional
        .setMaxTimeInBufferMS(5000)                         // Optional
        .build()

flinkStream.sinkTo(dynamoDbSink)
```
{{< /tab >}}
{{< /tabs >}}

## Configurations

Flink's DynamoDB sink is created by using the static builder `DynamoDBSink.<InputType>builder()`.

1. __setDynamoDbProperties(Properties sinkProperties)__
    * Required.
    * Supplies credentials, region and other parameters to the DynamoDB client.
2. __setTableName(String tableName)__
    * Required.
    * Name of the table to sink to.
3. __setElementConverter(ElementConverter<InputType, DynamoDbWriteRequest> elementConverter)__
    * Required.
    * Converts generic records of type `InputType` to `DynamoDbWriteRequest`.
4. _setOverwriteByPartitionKeys(List<String> partitionKeys)_
    * Optional. Default: [].
    * Used to deduplicate write requests within each batch pushed to DynamoDB.
5. _setFailOnError(boolean failOnError)_
    * Optional. Default: `false`.
    * Whether failed requests to write records are treated as fatal exceptions in the sink.
6. _setMaxBatchSize(int maxBatchSize)_
    * Optional. Default: `25`.
    * Maximum size of a batch to write.
7. _setMaxInFlightRequests(int maxInFlightRequests)_
    * Optional. Default: `50`.
    * The maximum number of in flight requests allowed before the sink applies backpressure.
8. _setMaxBufferedRequests(int maxBufferedRequests)_
    * Optional. Default: `10_000`.
    * The maximum number of records that may be buffered in the sink before backpressure is applied.
9. _setMaxBatchSizeInBytes(int maxBatchSizeInBytes)_
    * N/A.
    * This configuration is not supported, see [FLINK-29854](https://issues.apache.org/jira/browse/FLINK-29854).
10. _setMaxTimeInBufferMS(int maxTimeInBufferMS)_
    * Optional. Default: `5000`.
    * The maximum time a record may stay in the sink before being flushed.
11. _setMaxRecordSizeInBytes(int maxRecordSizeInBytes)_
    * N/A.
    * This configuration is not supported, see [FLINK-29854](https://issues.apache.org/jira/browse/FLINK-29854).
12. _build()_
    * Constructs and returns the DynamoDB sink.

## Element Converter

An element converter is used to convert from a record in the DataStream to a DynamoDbWriteRequest which the sink will write to the destination DynamoDB table. The DynamoDB sink allows the user to supply a custom element converter, or use the provided
`DynamoDbBeanElementConverter` when you are working with `@DynamoDbBean` objects. For more information on supported
annotations see [here](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/examples-dynamodb-enhanced.html#dynamodb-enhanced-mapper-tableschema).

A sample application using a custom `ElementConverter` can be found [here](https://github.com/apache/flink-connector-aws/blob/main/flink-connector-dynamodb/src/test/java/org/apache/flink/connector/dynamodb/sink/examples/SinkIntoDynamoDb.java). A sample application using the `DynamoDbBeanElementConverter` can be found [here](https://github.com/apache/flink-connector-aws/blob/main/flink-connector-dynamodb/src/test/java/org/apache/flink/connector/dynamodb/sink/examples/SinkDynamoDbBeanIntoDynamoDb.java).

## Using Custom DynamoDB Endpoints

It is sometimes desirable to have Flink operate as a consumer or producer against a DynamoDB VPC endpoint or a non-AWS
DynamoDB endpoint such as [Localstack](https://localstack.cloud/); this is especially useful when performing
functional testing of a Flink application. The AWS endpoint that would normally be inferred by the AWS region set in the
Flink configuration must be overridden via a configuration property.

To override the AWS endpoint, set the `AWSConfigConstants.AWS_ENDPOINT` and `AWSConfigConstants.AWS_REGION` properties. The region will be used to sign the endpoint URL.

{{< tabs "bcadd466-8416-4d3c-a6a7-c46eee0cbd4a" >}}
{{< tab "Java" >}}
```java
Properties producerConfig = new Properties();
producerConfig.put(AWSConfigConstants.AWS_REGION, "eu-west-1");
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
producerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4566");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val producerConfig = new Properties()
producerConfig.put(AWSConfigConstants.AWS_REGION, "eu-west-1")
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
producerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4566")
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
