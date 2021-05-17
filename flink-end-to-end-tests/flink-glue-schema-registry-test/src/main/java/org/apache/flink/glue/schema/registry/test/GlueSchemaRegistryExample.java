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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.kafka.test.base.KafkaExampleUtil;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A simple example that shows how to read from and write to Kinesis. This will read Avro messages
 * from the input stream, and finally write back to another stream.
 *
 * <p>Example usage: --input-stream test-input --output-stream test-output --aws.endpoint
 * https://localhost:4567 --flink.stream.initpos TRIM_HORIZON
 */
public class GlueSchemaRegistryExample {
    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = KafkaExampleUtil.prepareExecutionEnv(parameterTool);

        String inputStream = parameterTool.getRequired("input-stream");
        String outputStream = parameterTool.getRequired("output-stream");

        FlinkKinesisConsumer<GenericRecord> consumer =
                new FlinkKinesisConsumer<>(
                        inputStream,
                        GlueSchemaRegistryAvroDeserializationSchema.forGeneric(
                                getSchema(), getConfigs()),
                        parameterTool.getProperties());

        Properties producerProperties = new Properties(parameterTool.getProperties());
        // producer needs region even when URL is specified
        producerProperties.put(ConsumerConfigConstants.AWS_REGION, "ca-central-1");
        // test driver does not deaggregate
        producerProperties.put("AggregationEnabled", String.valueOf(false));

        // KPL does not recognize endpoint URL..
        String kinesisUrl = producerProperties.getProperty(ConsumerConfigConstants.AWS_ENDPOINT);
        if (kinesisUrl != null) {
            URL url = new URL(kinesisUrl);
            producerProperties.put("KinesisEndpoint", url.getHost());
            producerProperties.put("KinesisPort", Integer.toString(url.getPort()));
            producerProperties.put("VerifyCertificate", "false");
        }

        FlinkKinesisProducer<GenericRecord> producer =
                new FlinkKinesisProducer<>(
                        GlueSchemaRegistryAvroSerializationSchema.forGeneric(
                                getSchema(), outputStream, getConfigs()),
                        producerProperties);
        producer.setDefaultStream(outputStream);
        producer.setDefaultPartition("fakePartition");

        DataStream<GenericRecord> input = env.addSource(consumer);

        input.addSink(producer);
        env.execute();
    }

    public static Schema getSchema() throws IOException {
        Schema.Parser parser = new Schema.Parser();

        return parser.parse(
                GlueSchemaRegistryExample.class
                        .getClassLoader()
                        .getResourceAsStream("avro/user.avsc"));
    }

    private static Map<String, Object> getConfigs() {
        Map<String, Object> configs = new HashMap();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "ca-central-1");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(
                AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
                AvroRecordType.GENERIC_RECORD.getName());

        return configs;
    }
}
