/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.glue.schema.registry.test;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.GetRecordsResult;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.Record;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.GetShardListResult;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput;
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.deserializers.AWSDeserializer;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSAvroSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Simple client to publish and retrieve messages, using the AWS Kinesis SDK, Flink Kinesis
 * Connectors and Glue Schema Registry classes.
 */
public class GSRKinesisPubsubClient {
    private static final Logger LOG = LoggerFactory.getLogger(GSRKinesisPubsubClient.class);

    private final AmazonKinesis kinesisClient;
    private final Properties properties;

    public GSRKinesisPubsubClient(Properties properties) {
        this.kinesisClient = createClientWithCredentials(properties);
        this.properties = properties;
    }

    public void createStream(String stream, int shards, Properties props) throws Exception {
        try {
            kinesisClient.describeStream(stream);
            kinesisClient.deleteStream(stream);
        } catch (ResourceNotFoundException rnfe) {
            // Exception can be ignored
        }

        kinesisClient.createStream(stream, shards);
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(5));
        while (deadline.hasTimeLeft()) {
            try {
                Thread.sleep(250);
                if (kinesisClient.describeStream(stream).getStreamDescription().getShards().size()
                        != shards) {
                    continue;
                }
                break;
            } catch (ResourceNotFoundException rnfe) {
                // Exception can be ignored
            }
        }
    }

    public void sendMessage(String schema, String streamName, GenericRecord msg) {
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName(streamName);
        putRecordRequest.setPartitionKey("fakePartitionKey");
        UUID schemaVersionId =
                createSerializer()
                        .registerSchema(
                                AWSSerializerInput.builder()
                                        .schemaDefinition(schema)
                                        .schemaName(streamName)
                                        .transportName(streamName)
                                        .build());

        byte[] serializedData = createSerializer().serialize(msg, schemaVersionId);
        putRecordRequest.withData(ByteBuffer.wrap(serializedData));
        PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);

        LOG.info("added record: {}", putRecordResult.getSequenceNumber());
    }

    public List<Object> readAllMessages(String streamName) throws Exception {
        KinesisProxyInterface kinesisProxy = KinesisProxy.create(properties);
        Map<String, String> streamNamesWithLastSeenShardIds = new HashMap<>();
        streamNamesWithLastSeenShardIds.put(streamName, null);

        GetShardListResult shardListResult =
                kinesisProxy.getShardList(streamNamesWithLastSeenShardIds);
        AWSDeserializer awsDeserializer = createDeserializer();
        int maxRecordsToFetch = 10;

        List<Object> messages = new ArrayList<>();
        // retrieve records from all shards
        for (StreamShardHandle ssh : shardListResult.getRetrievedShardListOfStream(streamName)) {
            String shardIterator = kinesisProxy.getShardIterator(ssh, "TRIM_HORIZON", null);
            GetRecordsResult getRecordsResult =
                    kinesisProxy.getRecords(shardIterator, maxRecordsToFetch);
            List<org.apache.flink.kinesis.shaded.com.amazonaws.services.kinesis.model.Record>
                    aggregatedRecords = getRecordsResult.getRecords();
            for (Record record : aggregatedRecords) {
                Object obj =
                        awsDeserializer.deserialize(
                                AWSDeserializerInput.builder()
                                        .buffer(ByteBuffer.wrap(record.getData().array()))
                                        .transportName(streamName)
                                        .build());
                messages.add(obj);
            }
        }
        return messages;
    }

    private Map<String, Object> getSerDeConfigs() {
        Map<String, Object> configs = new HashMap();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "ca-central-1");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(
                AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
                AvroRecordType.GENERIC_RECORD.getName());

        return configs;
    }

    private AWSAvroSerializer createSerializer() {
        return AWSAvroSerializer.builder()
                .configs(getSerDeConfigs())
                .credentialProvider(DefaultCredentialsProvider.builder().build())
                .build();
    }

    private AWSDeserializer createDeserializer() {
        return AWSDeserializer.builder()
                .configs(getSerDeConfigs())
                .credentialProvider(DefaultCredentialsProvider.builder().build())
                .build();
    }

    private static AmazonKinesis createClientWithCredentials(Properties props)
            throws AmazonClientException {
        AWSCredentialsProvider credentialsProvider = new EnvironmentVariableCredentialsProvider();
        return AmazonKinesisClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                props.getProperty(ConsumerConfigConstants.AWS_ENDPOINT),
                                "ca-central-1"))
                .build();
    }
}
