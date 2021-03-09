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

import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisException.FlinkKinesisTimeoutException;
import org.apache.flink.streaming.connectors.kinesis.proxy.FullJitterBackoff;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory.StreamConsumerFakeKinesis;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;

import java.util.Properties;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_MAX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEREGISTER_STREAM_TIMEOUT_SECONDS;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.LAZY;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_CONSUMER_NAME;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_REGISTRATION_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_MAX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.REGISTER_STREAM_TIMEOUT_SECONDS;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.EFO;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.efoConsumerArn;
import static org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory.STREAM_CONSUMER_ARN_EXISTING;
import static org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory.STREAM_CONSUMER_ARN_NEW;
import static org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory.StreamConsumerFakeKinesis.NUMBER_OF_DESCRIBE_REQUESTS_TO_ACTIVATE;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link StreamConsumerRegistrar}. */
public class StreamConsumerRegistrarTest {

    private static final String STREAM = "stream";

    private static final long EXPECTED_REGISTRATION_MAX = 1;
    private static final long EXPECTED_REGISTRATION_BASE = 2;
    private static final double EXPECTED_REGISTRATION_POW = 0.5;

    private static final long EXPECTED_DEREGISTRATION_MAX = 2;
    private static final long EXPECTED_DEREGISTRATION_BASE = 4;
    private static final double EXPECTED_DEREGISTRATION_POW = 1;

    @Rule public final ExpectedException thrown = ExpectedException.none();

    @Test
    public void testStreamNotFoundWhenRegisteringThrowsException() throws Exception {
        thrown.expect(ResourceNotFoundException.class);

        KinesisProxyV2Interface kinesis = FakeKinesisFanOutBehavioursFactory.streamNotFound();
        StreamConsumerRegistrar registrar = createRegistrar(kinesis, mock(FullJitterBackoff.class));

        registrar.registerStreamConsumer(STREAM, "name");
    }

    @Test
    public void testRegisterStreamConsumerRegistersNewStreamConsumer() throws Exception {
        FullJitterBackoff backoff = mock(FullJitterBackoff.class);

        KinesisProxyV2Interface kinesis =
                FakeKinesisFanOutBehavioursFactory.streamConsumerNotFound();
        StreamConsumerRegistrar registrar = createRegistrar(kinesis, backoff);

        String result = registrar.registerStreamConsumer(STREAM, "name");

        assertEquals(STREAM_CONSUMER_ARN_NEW, result);
    }

    @Test
    public void testRegisterStreamConsumerThatAlreadyExistsAndActive() throws Exception {
        FullJitterBackoff backoff = mock(FullJitterBackoff.class);

        KinesisProxyV2Interface kinesis =
                FakeKinesisFanOutBehavioursFactory.existingActiveConsumer();
        StreamConsumerRegistrar registrar = createRegistrar(kinesis, backoff);

        String result = registrar.registerStreamConsumer(STREAM, "name");

        verify(backoff, never()).sleep(anyLong());
        assertEquals(STREAM_CONSUMER_ARN_EXISTING, result);
    }

    @Test
    public void testRegisterStreamConsumerWaitsForConsumerToBecomeActive() throws Exception {
        FullJitterBackoff backoff = mock(FullJitterBackoff.class);

        StreamConsumerFakeKinesis kinesis =
                FakeKinesisFanOutBehavioursFactory.registerExistingConsumerAndWaitToBecomeActive();
        StreamConsumerRegistrar registrar = createRegistrar(kinesis, backoff);

        String result = registrar.registerStreamConsumer(STREAM, "name");

        // we backoff on each retry
        verify(backoff, times(NUMBER_OF_DESCRIBE_REQUESTS_TO_ACTIVATE - 1)).sleep(anyLong());
        assertEquals(STREAM_CONSUMER_ARN_EXISTING, result);

        // We will invoke describe stream until the stream consumer is activated
        assertEquals(
                NUMBER_OF_DESCRIBE_REQUESTS_TO_ACTIVATE,
                kinesis.getNumberOfDescribeStreamConsumerInvocations());

        for (int i = 1; i < NUMBER_OF_DESCRIBE_REQUESTS_TO_ACTIVATE; i++) {
            verify(backoff).calculateFullJitterBackoff(anyLong(), anyLong(), anyDouble(), eq(i));
        }
    }

    @Test
    public void testRegisterStreamConsumerTimeoutWaitingForConsumerToBecomeActive()
            throws Exception {
        thrown.expect(FlinkKinesisTimeoutException.class);
        thrown.expectMessage(
                "Timeout waiting for stream consumer to become active: name on stream-arn");

        StreamConsumerFakeKinesis kinesis =
                FakeKinesisFanOutBehavioursFactory.registerExistingConsumerAndWaitToBecomeActive();

        Properties configProps = createEfoProperties();
        configProps.setProperty(REGISTER_STREAM_TIMEOUT_SECONDS, "1");

        FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(configProps, singletonList(STREAM));
        StreamConsumerRegistrar registrar =
                new StreamConsumerRegistrar(kinesis, configuration, backoffFor(1001));

        registrar.registerStreamConsumer(STREAM, "name");
    }

    @Test
    public void testRegistrationBackoffForLazy() throws Exception {
        FullJitterBackoff backoff = mock(FullJitterBackoff.class);

        KinesisProxyV2Interface kinesis =
                FakeKinesisFanOutBehavioursFactory.existingActiveConsumer();

        Properties efoProperties = createEfoProperties();
        efoProperties.setProperty(EFO_REGISTRATION_TYPE, LAZY.name());

        FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(efoProperties, emptyList());
        StreamConsumerRegistrar registrar =
                new StreamConsumerRegistrar(kinesis, configuration, backoff);

        String result = registrar.registerStreamConsumer(STREAM, "name");

        verify(backoff).sleep(anyLong());
        assertEquals(STREAM_CONSUMER_ARN_EXISTING, result);
    }

    @Test
    public void testDeregisterStreamConsumerAndWaitForDeletingStatus() throws Exception {
        FullJitterBackoff backoff = mock(FullJitterBackoff.class);

        StreamConsumerFakeKinesis kinesis =
                FakeKinesisFanOutBehavioursFactory.existingActiveConsumer();
        StreamConsumerRegistrar registrar = createRegistrar(kinesis, backoff);

        registrar.deregisterStreamConsumer(STREAM);

        // We will invoke describe stream until the stream consumer is in the DELETING state
        assertEquals(2, kinesis.getNumberOfDescribeStreamConsumerInvocations());

        for (int i = 1; i < 2; i++) {
            verify(backoff).calculateFullJitterBackoff(anyLong(), anyLong(), anyDouble(), eq(i));
        }
    }

    @Test
    public void testDeregisterStreamConsumerTimeoutWaitingForConsumerToDeregister()
            throws Exception {
        thrown.expect(FlinkKinesisTimeoutException.class);
        thrown.expectMessage(
                "Timeout waiting for stream consumer to deregister: stream-consumer-arn");

        StreamConsumerFakeKinesis kinesis =
                FakeKinesisFanOutBehavioursFactory.existingActiveConsumer();

        Properties configProps = createEfoProperties();
        configProps.setProperty(DEREGISTER_STREAM_TIMEOUT_SECONDS, "1");

        FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(configProps, singletonList(STREAM));
        StreamConsumerRegistrar registrar =
                new StreamConsumerRegistrar(kinesis, configuration, backoffFor(1001));

        registrar.deregisterStreamConsumer(STREAM);
    }

    @Test
    public void testDeregisterStreamConsumerNotFound() throws Exception {
        FullJitterBackoff backoff = mock(FullJitterBackoff.class);

        StreamConsumerFakeKinesis kinesis =
                FakeKinesisFanOutBehavioursFactory.streamConsumerNotFound();
        StreamConsumerRegistrar registrar = createRegistrar(kinesis, backoff);

        registrar.deregisterStreamConsumer(STREAM);

        assertEquals(1, kinesis.getNumberOfDescribeStreamConsumerInvocations());
    }

    @Test
    public void testDeregisterStreamConsumerArnNotFound() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Stream consumer ARN not found for stream: not-found");

        FullJitterBackoff backoff = mock(FullJitterBackoff.class);

        StreamConsumerFakeKinesis kinesis =
                FakeKinesisFanOutBehavioursFactory.streamConsumerNotFound();
        StreamConsumerRegistrar registrar = createRegistrar(kinesis, backoff);

        registrar.deregisterStreamConsumer("not-found");
    }

    @Test
    public void testRegistrationBackoff() throws Exception {
        FanOutRecordPublisherConfiguration configuration = createConfiguration();

        FullJitterBackoff backoff = mock(FullJitterBackoff.class);
        when(backoff.calculateFullJitterBackoff(anyLong(), anyLong(), anyDouble(), anyInt()))
                .thenReturn(5L);

        StreamConsumerRegistrar registrar =
                new StreamConsumerRegistrar(
                        mock(KinesisProxyV2Interface.class), configuration, backoff);

        registrar.registrationBackoff(configuration, backoff, 10);

        verify(backoff).sleep(5);
        verify(backoff)
                .calculateFullJitterBackoff(
                        EXPECTED_REGISTRATION_BASE,
                        EXPECTED_REGISTRATION_MAX,
                        EXPECTED_REGISTRATION_POW,
                        10);
    }

    @Test
    public void testDeregistrationBackoff() throws Exception {
        FanOutRecordPublisherConfiguration configuration = createConfiguration();

        FullJitterBackoff backoff = mock(FullJitterBackoff.class);
        when(backoff.calculateFullJitterBackoff(anyLong(), anyLong(), anyDouble(), anyInt()))
                .thenReturn(5L);

        StreamConsumerRegistrar registrar =
                new StreamConsumerRegistrar(
                        mock(KinesisProxyV2Interface.class), configuration, backoff);

        registrar.deregistrationBackoff(configuration, backoff, 11);

        verify(backoff).sleep(5);
        verify(backoff)
                .calculateFullJitterBackoff(
                        EXPECTED_DEREGISTRATION_BASE,
                        EXPECTED_DEREGISTRATION_MAX,
                        EXPECTED_DEREGISTRATION_POW,
                        11);
    }

    @Test
    public void testCloseClosesProxy() {
        KinesisProxyV2Interface kinesis = mock(KinesisProxyV2Interface.class);
        StreamConsumerRegistrar registrar = createRegistrar(kinesis, mock(FullJitterBackoff.class));

        registrar.close();

        verify(kinesis).close();
    }

    private StreamConsumerRegistrar createRegistrar(
            final KinesisProxyV2Interface kinesis, final FullJitterBackoff backoff) {
        FanOutRecordPublisherConfiguration configuration = createConfiguration();
        return new StreamConsumerRegistrar(kinesis, configuration, backoff);
    }

    private FanOutRecordPublisherConfiguration createConfiguration() {
        return new FanOutRecordPublisherConfiguration(createEfoProperties(), singletonList(STREAM));
    }

    private Properties createEfoProperties() {
        Properties config = new Properties();
        config.setProperty(RECORD_PUBLISHER_TYPE, EFO.name());
        config.setProperty(EFO_CONSUMER_NAME, "dummy-efo-consumer");
        config.setProperty(
                REGISTER_STREAM_BACKOFF_BASE, String.valueOf(EXPECTED_REGISTRATION_BASE));
        config.setProperty(REGISTER_STREAM_BACKOFF_MAX, String.valueOf(EXPECTED_REGISTRATION_MAX));
        config.setProperty(
                REGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT,
                String.valueOf(EXPECTED_REGISTRATION_POW));
        config.setProperty(
                DEREGISTER_STREAM_BACKOFF_BASE, String.valueOf(EXPECTED_DEREGISTRATION_BASE));
        config.setProperty(
                DEREGISTER_STREAM_BACKOFF_MAX, String.valueOf(EXPECTED_DEREGISTRATION_MAX));
        config.setProperty(
                DEREGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT,
                String.valueOf(EXPECTED_DEREGISTRATION_POW));
        config.setProperty(efoConsumerArn(STREAM), "stream-consumer-arn");
        return config;
    }

    private FullJitterBackoff backoffFor(final long millisToBackoffFor) {
        FullJitterBackoff backoff = spy(new FullJitterBackoff());
        when(backoff.calculateFullJitterBackoff(anyLong(), anyLong(), anyDouble(), anyInt()))
                .thenReturn(millisToBackoffFor);
        return backoff;
    }
}
