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

package org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout;

import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEREGISTER_STREAM_TIMEOUT_SECONDS;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.NONE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_CONSUMER_ARN_PREFIX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_CONSUMER_NAME;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_REGISTRATION_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.REGISTER_STREAM_TIMEOUT_SECONDS;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.EFO;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_TIMEOUT_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FanOutRecordPublisherConfiguration}. */
public class FanOutRecordPublisherConfigurationTest extends TestLogger {

    @Test
    public void testPollingRecordPublisher() {
        assertThatThrownBy(
                        () -> {
                            Properties testConfig = TestUtils.getStandardProperties();
                            testConfig.setProperty(
                                    RECORD_PUBLISHER_TYPE, RecordPublisherType.POLLING.toString());

                            new FanOutRecordPublisherConfiguration(testConfig, new ArrayList<>());
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Only efo record publisher can register a FanOutProperties.");
    }

    @Test
    public void testEagerStrategyWithConsumerName() {
        String fakedConsumerName = "fakedconsumername";
        Properties testConfig = TestUtils.getStandardProperties();
        testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
        testConfig.setProperty(EFO_CONSUMER_NAME, fakedConsumerName);
        FanOutRecordPublisherConfiguration fanOutRecordPublisherConfiguration =
                new FanOutRecordPublisherConfiguration(testConfig, new ArrayList<>());
        assertThat(Optional.of(fakedConsumerName))
                .isEqualTo(fanOutRecordPublisherConfiguration.getConsumerName());
    }

    @Test
    public void testEagerStrategyWithNoConsumerName() {
        String msg = "No valid enhanced fan-out consumer name is set through " + EFO_CONSUMER_NAME;

        assertThatThrownBy(
                        () -> {
                            Properties testConfig = TestUtils.getStandardProperties();
                            testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
                            new FanOutRecordPublisherConfiguration(testConfig, new ArrayList<>());
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(msg);
    }

    @Test
    public void testNoneStrategyWithStreams() {
        List<String> streams = Arrays.asList("fakedstream1", "fakedstream2");
        Properties testConfig = TestUtils.getStandardProperties();
        testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
        testConfig.setProperty(EFO_REGISTRATION_TYPE, NONE.toString());
        streams.forEach(
                stream -> testConfig.setProperty(EFO_CONSUMER_ARN_PREFIX + "." + stream, stream));
        FanOutRecordPublisherConfiguration fanOutRecordPublisherConfiguration =
                new FanOutRecordPublisherConfiguration(testConfig, streams);
        Map<String, String> expectedStreamArns = new HashMap<>();
        expectedStreamArns.put("fakedstream1", "fakedstream1");
        expectedStreamArns.put("fakedstream2", "fakedstream2");

        assertThat(Optional.of("fakedstream1"))
                .isEqualTo(fanOutRecordPublisherConfiguration.getStreamConsumerArn("fakedstream1"));
    }

    @Test
    public void testNoneStrategyWithNoStreams() {
        List<String> streams = Arrays.asList("fakedstream1", "fakedstream2");

        String msg =
                "Invalid efo consumer arn settings for not providing consumer arns: flink.stream.efo.consumerarn.fakedstream1, flink.stream.efo.consumerarn.fakedstream2";
        assertThatThrownBy(
                        () -> {
                            Properties testConfig = TestUtils.getStandardProperties();
                            testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
                            testConfig.setProperty(EFO_REGISTRATION_TYPE, NONE.toString());

                            new FanOutRecordPublisherConfiguration(testConfig, streams);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(msg);
    }

    @Test
    public void testNoneStrategyWithNotEnoughStreams() {
        List<String> streams = Arrays.asList("fakedstream1", "fakedstream2");

        String msg =
                "Invalid efo consumer arn settings for not providing consumer arns: flink.stream.efo.consumerarn.fakedstream2";
        assertThatThrownBy(
                        () -> {
                            Properties testConfig = TestUtils.getStandardProperties();
                            testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
                            testConfig.setProperty(EFO_REGISTRATION_TYPE, NONE.toString());
                            testConfig.setProperty(
                                    EFO_CONSUMER_ARN_PREFIX + "." + "fakedstream1", "fakedstream1");

                            new FanOutRecordPublisherConfiguration(testConfig, streams);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(msg);
    }

    @Test
    public void testParseRegisterStreamConsumerTimeout() {
        Properties testConfig = TestUtils.getStandardProperties();
        testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
        testConfig.setProperty(EFO_CONSUMER_NAME, "name");
        testConfig.setProperty(REGISTER_STREAM_TIMEOUT_SECONDS, "120");

        FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(testConfig, Collections.emptyList());

        assertThat(configuration.getRegisterStreamConsumerTimeout())
                .isEqualTo(Duration.ofSeconds(120));
        assertThat(configuration.getDeregisterStreamConsumerTimeout())
                .isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    public void testParseDeregisterStreamConsumerTimeout() {
        Properties testConfig = TestUtils.getStandardProperties();
        testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
        testConfig.setProperty(EFO_CONSUMER_NAME, "name");
        testConfig.setProperty(DEREGISTER_STREAM_TIMEOUT_SECONDS, "240");

        FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(testConfig, Collections.emptyList());

        assertThat(configuration.getRegisterStreamConsumerTimeout())
                .isEqualTo(Duration.ofSeconds(60));
        assertThat(configuration.getDeregisterStreamConsumerTimeout())
                .isEqualTo(Duration.ofSeconds(240));
    }

    @Test
    public void testParseSubscribeToShardTimeout() {
        Properties testConfig = TestUtils.getStandardProperties();
        testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
        testConfig.setProperty(EFO_CONSUMER_NAME, "name");
        testConfig.setProperty(SUBSCRIBE_TO_SHARD_TIMEOUT_SECONDS, "123");

        FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(testConfig, Collections.emptyList());

        assertThat(configuration.getSubscribeToShardTimeout()).isEqualTo(Duration.ofSeconds(123));
    }

    @Test
    public void testDefaultSubscribeToShardTimeout() {
        Properties testConfig = TestUtils.getStandardProperties();
        testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
        testConfig.setProperty(EFO_CONSUMER_NAME, "name");

        FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(testConfig, Collections.emptyList());

        assertThat(configuration.getSubscribeToShardTimeout()).isEqualTo(Duration.ofSeconds(60));
    }
}
