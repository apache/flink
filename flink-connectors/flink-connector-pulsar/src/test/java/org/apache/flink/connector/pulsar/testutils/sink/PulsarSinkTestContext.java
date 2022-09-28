/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.testutils.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.testutils.PulsarTestContext;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.external.sink.DataStreamSinkV2ExternalContext;
import org.apache.flink.connector.testframe.external.sink.TestingSinkSettings;

import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import org.apache.pulsar.client.api.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_BATCHING_MAX_MESSAGES;
import static org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema.pulsarSchema;
import static org.apache.flink.connector.pulsar.testutils.PulsarTestCommonUtils.toDeliveryGuarantee;

/** Common sink test context for pulsar based test. */
public class PulsarSinkTestContext extends PulsarTestContext<String>
        implements DataStreamSinkV2ExternalContext<String> {

    private static final String TOPIC_NAME_PREFIX = "flink-sink-topic-";
    private static final int RECORD_SIZE_UPPER_BOUND = 300;
    private static final int RECORD_SIZE_LOWER_BOUND = 100;
    private static final int RECORD_STRING_SIZE = 20;

    private String topicName = topicName();
    private final Closer closer = Closer.create();

    public PulsarSinkTestContext(PulsarTestEnvironment environment) {
        super(environment, Schema.STRING);
    }

    @Override
    protected String displayName() {
        return "write messages into one topic in Pulsar";
    }

    @Override
    public Sink<String> createSink(TestingSinkSettings sinkSettings) {
        operator.createTopic(topicName, 4);
        DeliveryGuarantee guarantee = toDeliveryGuarantee(sinkSettings.getCheckpointingMode());

        return PulsarSink.builder()
                .setServiceUrl(operator.serviceUrl())
                .setAdminUrl(operator.adminUrl())
                .setTopics(topicName)
                .setDeliveryGuarantee(guarantee)
                .setSerializationSchema(pulsarSchema(schema))
                .enableSchemaEvolution()
                .setConfig(PULSAR_BATCHING_MAX_MESSAGES, 4)
                .build();
    }

    @Override
    public ExternalSystemDataReader<String> createSinkDataReader(TestingSinkSettings sinkSettings) {
        PulsarPartitionDataReader<String> reader =
                sneakyClient(
                        () -> new PulsarPartitionDataReader<>(operator, topicName, Schema.STRING));
        closer.register(reader);

        return reader;
    }

    @Override
    public List<String> generateTestData(TestingSinkSettings sinkSettings, long seed) {
        Random random = new Random(seed);
        int recordSize =
                random.nextInt(RECORD_SIZE_UPPER_BOUND - RECORD_SIZE_LOWER_BOUND)
                        + RECORD_SIZE_LOWER_BOUND;
        List<String> records = new ArrayList<>(recordSize);
        for (int i = 0; i < recordSize; i++) {
            int size = random.nextInt(RECORD_STRING_SIZE) + RECORD_STRING_SIZE;
            String record = "index:" + i + "-data:" + randomAlphanumeric(size);
            records.add(record);
        }

        return records;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }

    @Override
    public void close() throws Exception {
        // Change the topic name after finishing a test case.
        closer.register(() -> topicName = topicName());
        closer.close();
    }

    private String topicName() {
        return TOPIC_NAME_PREFIX + randomAlphanumeric(8);
    }
}
