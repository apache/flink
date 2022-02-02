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

package org.apache.flink.connector.pulsar.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.connector.pulsar.testutils.source.ControlSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.testutils.junit.SharedObjectsExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema.flinkSchema;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for using PulsarSink writing to a Pulsar cluster. */
class PulsarSinkITCase extends PulsarTestSuiteBase {

    // Using this extension for creating shared reference which would be used in source function.
    @RegisterExtension final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    @Test
    void writeRecordsToPulsarWithAtLeastOnceGuarantee() throws Exception {
        writeRecordsToPulsar(DeliveryGuarantee.AT_LEAST_ONCE);
    }

    @Test
    void writeRecordsToPulsarWithNoneGuarantee() throws Exception {
        writeRecordsToPulsar(DeliveryGuarantee.NONE);
    }

    @Test
    void writeRecordsToPulsarWithExactlyOnceGuarantee() throws Exception {
        writeRecordsToPulsar(DeliveryGuarantee.EXACTLY_ONCE);
    }

    private void writeRecordsToPulsar(DeliveryGuarantee guarantee) throws Exception {
        // A random topic with partition 1.
        String topic = randomAlphabetic(8);
        operator().createTopic(topic, 4);
        int counts = ThreadLocalRandom.current().nextInt(100, 200);

        ControlSource source =
                new ControlSource(
                        sharedObjects, operator(), topic, guarantee, counts, Duration.ofMinutes(5));
        PulsarSink<String> sink =
                PulsarSink.builder()
                        .setServiceUrl(operator().serviceUrl())
                        .setAdminUrl(operator().adminUrl())
                        .setDeliveryGuarantee(guarantee)
                        .setTopics(topic)
                        .setSerializationSchema(flinkSchema(new SimpleStringSchema()))
                        .build();

        StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(100L);
        env.addSource(source).sinkTo(sink);
        env.execute();

        List<String> expectedRecords = source.getExpectedRecords();
        List<String> consumedRecords = source.getConsumedRecords();

        assertThat(consumedRecords)
                .hasSameSizeAs(expectedRecords)
                .containsExactlyInAnyOrderElementsOf(expectedRecords);
    }
}
