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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

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
import static org.junit.Assert.assertEquals;

/** Tests for {@link FanOutRecordPublisherConfiguration}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FanOutRecordPublisherConfiguration.class)
public class FanOutRecordPublisherConfigurationTest extends TestLogger {
    @Rule private ExpectedException exception = ExpectedException.none();

    @Test
    public void testPollingRecordPublisher() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Only efo record publisher can register a FanOutProperties.");

        Properties testConfig = TestUtils.getStandardProperties();
        testConfig.setProperty(RECORD_PUBLISHER_TYPE, RecordPublisherType.POLLING.toString());

        new FanOutRecordPublisherConfiguration(testConfig, new ArrayList<>());
    }

    @Test
    public void testEagerStrategyWithConsumerName() {
        String fakedConsumerName = "fakedconsumername";
        Properties testConfig = TestUtils.getStandardProperties();
        testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
        testConfig.setProperty(EFO_CONSUMER_NAME, fakedConsumerName);
        FanOutRecordPublisherConfiguration fanOutRecordPublisherConfiguration =
                new FanOutRecordPublisherConfiguration(testConfig, new ArrayList<>());
        assertEquals(
                fanOutRecordPublisherConfiguration.getConsumerName(),
                Optional.of(fakedConsumerName));
    }

    @Test
    public void testEagerStrategyWithNoConsumerName() {
        String msg = "No valid enhanced fan-out consumer name is set through " + EFO_CONSUMER_NAME;

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(msg);

        Properties testConfig = TestUtils.getStandardProperties();
        testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
        new FanOutRecordPublisherConfiguration(testConfig, new ArrayList<>());
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

        assertEquals(
                fanOutRecordPublisherConfiguration.getStreamConsumerArn("fakedstream1"),
                Optional.of("fakedstream1"));
    }

    @Test
    public void testNoneStrategyWithNoStreams() {
        List<String> streams = Arrays.asList("fakedstream1", "fakedstream2");

        String msg =
                "Invalid efo consumer arn settings for not providing consumer arns: flink.stream.efo.consumerarn.fakedstream1, flink.stream.efo.consumerarn.fakedstream2";
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(msg);

        Properties testConfig = TestUtils.getStandardProperties();
        testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
        testConfig.setProperty(EFO_REGISTRATION_TYPE, NONE.toString());

        new FanOutRecordPublisherConfiguration(testConfig, streams);
    }

    @Test
    public void testNoneStrategyWithNotEnoughStreams() {
        List<String> streams = Arrays.asList("fakedstream1", "fakedstream2");

        String msg =
                "Invalid efo consumer arn settings for not providing consumer arns: flink.stream.efo.consumerarn.fakedstream2";
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(msg);

        Properties testConfig = TestUtils.getStandardProperties();
        testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
        testConfig.setProperty(EFO_REGISTRATION_TYPE, NONE.toString());
        testConfig.setProperty(EFO_CONSUMER_ARN_PREFIX + "." + "fakedstream1", "fakedstream1");

        new FanOutRecordPublisherConfiguration(testConfig, streams);
    }

    @Test
    public void testParseRegisterStreamConsumerTimeout() {
        Properties testConfig = TestUtils.getStandardProperties();
        testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
        testConfig.setProperty(EFO_CONSUMER_NAME, "name");
        testConfig.setProperty(REGISTER_STREAM_TIMEOUT_SECONDS, "120");

        FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(testConfig, Collections.emptyList());

        assertEquals(Duration.ofSeconds(120), configuration.getRegisterStreamConsumerTimeout());
        assertEquals(Duration.ofSeconds(60), configuration.getDeregisterStreamConsumerTimeout());
    }

    @Test
    public void testParseDeregisterStreamConsumerTimeout() {
        Properties testConfig = TestUtils.getStandardProperties();
        testConfig.setProperty(RECORD_PUBLISHER_TYPE, EFO.toString());
        testConfig.setProperty(EFO_CONSUMER_NAME, "name");
        testConfig.setProperty(DEREGISTER_STREAM_TIMEOUT_SECONDS, "240");

        FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(testConfig, Collections.emptyList());

        assertEquals(Duration.ofSeconds(60), configuration.getRegisterStreamConsumerTimeout());
        assertEquals(Duration.ofSeconds(240), configuration.getDeregisterStreamConsumerTimeout());
    }
}
