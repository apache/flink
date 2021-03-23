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

import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisPubsubClient;

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput;
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.deserializers.AWSDeserializer;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSAvroSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.generic.GenericRecord;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.nio.ByteBuffer;
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
    private final KinesisPubsubClient client;

    public GSRKinesisPubsubClient(Properties properties) {
        this.client = new KinesisPubsubClient(properties);
    }

    public void sendMessage(String schema, String streamName, GenericRecord msg) {
        UUID schemaVersionId =
                createSerializer()
                        .registerSchema(
                                AWSSerializerInput.builder()
                                        .schemaDefinition(schema)
                                        .schemaName(streamName)
                                        .transportName(streamName)
                                        .build());

        client.sendMessage(streamName, createSerializer().serialize(msg, schemaVersionId));
    }

    public List<Object> readAllMessages(String streamName) throws Exception {
        AWSDeserializer awsDeserializer = createDeserializer();

        return client.readAllMessages(
                streamName,
                bytes ->
                        awsDeserializer.deserialize(
                                AWSDeserializerInput.builder()
                                        .buffer(ByteBuffer.wrap(bytes))
                                        .transportName(streamName)
                                        .build()));
    }

    public void createStream(String stream, int shards, Properties props) throws Exception {
        client.createTopic(stream, shards, props);
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
}
