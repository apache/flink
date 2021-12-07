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
import org.apache.flink.connectors.kinesis.testutils.KinesaliteContainer;
import org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TestLogger;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_POSITION;

/** End-to-end test for Glue Schema Registry AVRO format using Kinesalite. */
public class GlueSchemaRegistryAvroKinesisITCase extends TestLogger {
    private static final String INPUT_STREAM = "gsr_avro_input_stream";
    private static final String OUTPUT_STREAM = "gsr_avro_output_stream";
    private static final String INTER_CONTAINER_KINESALITE_ALIAS = "kinesalite";
    private static final String ACCESS_KEY = System.getenv("IT_CASE_GLUE_SCHEMA_ACCESS_KEY");
    private static final String SECRET_KEY = System.getenv("IT_CASE_GLUE_SCHEMA_SECRET_KEY");

    private static final Network network = Network.newNetwork();

    @ClassRule public static final Timeout TIMEOUT = new Timeout(10, TimeUnit.MINUTES);

    @ClassRule
    public static final KinesaliteContainer KINESALITE =
            new KinesaliteContainer(
                            DockerImageName.parse("instructure/kinesalite").withTag("latest"))
                    .withNetwork(network)
                    .withNetworkAliases(INTER_CONTAINER_KINESALITE_ALIAS);

    private GSRKinesisPubsubClient kinesisClient;

    @Before
    public void setUp() throws Exception {
        Assume.assumeTrue(
                "Access key not configured, skipping test...",
                !StringUtils.isNullOrWhitespaceOnly(ACCESS_KEY));
        Assume.assumeTrue(
                "Secret key not configured, skipping test...",
                !StringUtils.isNullOrWhitespaceOnly(SECRET_KEY));

        Properties properties = KINESALITE.getContainerProperties();

        kinesisClient = new GSRKinesisPubsubClient(properties);
        kinesisClient.createStream(INPUT_STREAM, 2, properties);
        kinesisClient.createStream(OUTPUT_STREAM, 2, properties);

        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");
    }

    @After
    public void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
    }

    @Test
    public void testGSRAvroGenericFormatWithFlink() throws Exception {
        List<GenericRecord> messages = getRecords();
        for (GenericRecord msg : messages) {
            kinesisClient.sendMessage(getSchema().toString(), INPUT_STREAM, msg);
        }
        log.info("generated records");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<GenericRecord> input = env.addSource(createSource());
        input.addSink(createSink());
        env.executeAsync();

        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(60));
        List<Object> results = kinesisClient.readAllMessages(OUTPUT_STREAM);
        while (deadline.hasTimeLeft() && results.size() < messages.size()) {
            log.info("waiting for results..");
            Thread.sleep(1000);
            results = kinesisClient.readAllMessages(OUTPUT_STREAM);
        }
        log.info("results: {}", results);

        Assert.assertEquals(
                "Results received from '" + OUTPUT_STREAM + "': " + results,
                messages.size(),
                results.size());
        Assert.assertTrue(messages.containsAll(results));
    }

    private FlinkKinesisConsumer<GenericRecord> createSource() throws Exception {
        Properties properties = KINESALITE.getContainerProperties();
        properties.setProperty(
                STREAM_INITIAL_POSITION,
                ConsumerConfigConstants.InitialPosition.TRIM_HORIZON.name());
        FlinkKinesisConsumer<GenericRecord> consumer =
                new FlinkKinesisConsumer<>(
                        INPUT_STREAM,
                        GlueSchemaRegistryAvroDeserializationSchema.forGeneric(
                                getSchema(), getConfigs()),
                        properties);
        return consumer;
    }

    private FlinkKinesisProducer<GenericRecord> createSink() throws Exception {
        FlinkKinesisProducer<GenericRecord> producer =
                new FlinkKinesisProducer<>(
                        GlueSchemaRegistryAvroSerializationSchema.forGeneric(
                                getSchema(), OUTPUT_STREAM, getConfigs()),
                        getProducerProperties());
        producer.setDefaultStream(OUTPUT_STREAM);
        producer.setDefaultPartition("fakePartition");
        return producer;
    }

    private Properties getProducerProperties() throws Exception {
        Properties producerProperties = new Properties(KINESALITE.getContainerProperties());
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
        return producerProperties;
    }

    private Schema getSchema() throws IOException {
        Schema.Parser parser = new Schema.Parser();

        return parser.parse(
                GlueSchemaRegistryAvroKinesisITCase.class
                        .getClassLoader()
                        .getResourceAsStream("avro/user.avsc"));
    }

    private Map<String, Object> getConfigs() {
        Map<String, Object> configs = new HashMap();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "ca-central-1");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(
                AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
                AvroRecordType.GENERIC_RECORD.getName());

        return configs;
    }

    private List<GenericRecord> getRecords() throws IOException {
        Schema userSchema = getSchema();

        GenericRecord sansa = new GenericData.Record(userSchema);
        sansa.put("name", "Sansa");
        sansa.put("favorite_number", 99);
        sansa.put("favorite_color", "white");

        GenericRecord harry = new GenericData.Record(userSchema);
        harry.put("name", "Harry");
        harry.put("favorite_number", 10);
        harry.put("favorite_color", "black");

        GenericRecord hermione = new GenericData.Record(userSchema);
        hermione.put("name", "Hermione");
        hermione.put("favorite_number", 1);
        hermione.put("favorite_color", "red");

        GenericRecord ron = new GenericData.Record(userSchema);
        ron.put("name", "Ron");
        ron.put("favorite_number", 18);
        ron.put("favorite_color", "green");

        GenericRecord jay = new GenericData.Record(userSchema);
        jay.put("name", "Jay");
        jay.put("favorite_number", 0);
        jay.put("favorite_color", "blue");

        List<GenericRecord> records = new ArrayList<>();
        records.add(sansa);
        records.add(harry);
        records.add(hermione);
        records.add(ron);
        records.add(jay);
        return records;
    }
}
