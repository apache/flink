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

package org.apache.flink.streaming.connectors.kinesis.proxy;

import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutRecordPublisherConfiguration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyList;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_MAX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_MAX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_CONSUMER_NAME;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_MAX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.EFO;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_MAX;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_DESCRIBE_RETRIES;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_BASE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_EXPONENTIAL_CONSTANT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_MAX;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for methods in the {@link KinesisProxyV2} class. */
public class KinesisProxyV2Test {

    private static final long EXPECTED_SUBSCRIBE_TO_SHARD_MAX = 1;
    private static final long EXPECTED_SUBSCRIBE_TO_SHARD_BASE = 2;
    private static final double EXPECTED_SUBSCRIBE_TO_SHARD_POW = 0.1;

    private static final long EXPECTED_REGISTRATION_MAX = 2;
    private static final long EXPECTED_REGISTRATION_BASE = 3;
    private static final double EXPECTED_REGISTRATION_POW = 0.2;

    private static final long EXPECTED_DEREGISTRATION_MAX = 3;
    private static final long EXPECTED_DEREGISTRATION_BASE = 4;
    private static final double EXPECTED_DEREGISTRATION_POW = 0.3;

    private static final long EXPECTED_DESCRIBE_CONSUMER_MAX = 4;
    private static final long EXPECTED_DESCRIBE_CONSUMER_BASE = 5;
    private static final double EXPECTED_DESCRIBE_CONSUMER_POW = 0.4;

    private static final long EXPECTED_DESCRIBE_STREAM_MAX = 5;
    private static final long EXPECTED_DESCRIBE_STREAM_BASE = 6;
    private static final double EXPECTED_DESCRIBE_STREAM_POW = 0.5;
    private static final int EXPECTED_DESCRIBE_STREAM_RETRIES = 10;

    @Rule public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testSubscribeToShard() {
        KinesisAsyncClient kinesis = mock(KinesisAsyncClient.class);
        KinesisProxyV2 proxy =
                new KinesisProxyV2(
                        kinesis,
                        mock(SdkAsyncHttpClient.class),
                        createConfiguration(),
                        mock(FullJitterBackoff.class));

        SubscribeToShardRequest request = SubscribeToShardRequest.builder().build();
        SubscribeToShardResponseHandler responseHandler =
                SubscribeToShardResponseHandler.builder().subscriber(event -> {}).build();

        proxy.subscribeToShard(request, responseHandler);

        verify(kinesis).subscribeToShard(eq(request), eq(responseHandler));
    }

    @Test
    public void testCloseInvokesClientClose() {
        SdkAsyncHttpClient httpClient = mock(SdkAsyncHttpClient.class);
        KinesisAsyncClient kinesis = mock(KinesisAsyncClient.class);
        KinesisProxyV2 proxy =
                new KinesisProxyV2(
                        kinesis, httpClient, createConfiguration(), mock(FullJitterBackoff.class));

        proxy.close();

        verify(kinesis).close();
        verify(httpClient).close();
    }

    @Test
    public void testRegisterStreamConsumer() throws Exception {
        KinesisAsyncClient client = mock(KinesisAsyncClient.class);
        KinesisProxyV2 proxy =
                new KinesisProxyV2(
                        client,
                        mock(SdkAsyncHttpClient.class),
                        createConfiguration(),
                        mock(FullJitterBackoff.class));

        RegisterStreamConsumerResponse expected = RegisterStreamConsumerResponse.builder().build();

        ArgumentCaptor<RegisterStreamConsumerRequest> requestCaptor =
                ArgumentCaptor.forClass(RegisterStreamConsumerRequest.class);
        when(client.registerStreamConsumer(requestCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(expected));

        RegisterStreamConsumerResponse actual = proxy.registerStreamConsumer("arn", "name");

        assertEquals(expected, actual);

        RegisterStreamConsumerRequest request = requestCaptor.getValue();
        assertEquals("arn", request.streamARN());
        assertEquals("name", request.consumerName());
    }

    @Test
    public void testRegisterStreamConsumerBackoffJitter() throws Exception {
        FullJitterBackoff backoff = mock(FullJitterBackoff.class);
        KinesisAsyncClient client = mock(KinesisAsyncClient.class);
        KinesisProxyV2 proxy =
                new KinesisProxyV2(
                        client, mock(SdkAsyncHttpClient.class), createConfiguration(), backoff);

        when(client.registerStreamConsumer(any(RegisterStreamConsumerRequest.class)))
                .thenThrow(new RuntimeException(LimitExceededException.builder().build()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                RegisterStreamConsumerResponse.builder().build()));

        proxy.registerStreamConsumer("arn", "name");

        verify(backoff).sleep(anyLong());
        verify(backoff)
                .calculateFullJitterBackoff(
                        EXPECTED_REGISTRATION_BASE,
                        EXPECTED_REGISTRATION_MAX,
                        EXPECTED_REGISTRATION_POW,
                        1);
    }

    @Test
    public void testDeregisterStreamConsumer() throws Exception {
        KinesisAsyncClient client = mock(KinesisAsyncClient.class);
        KinesisProxyV2 proxy =
                new KinesisProxyV2(
                        client,
                        mock(SdkAsyncHttpClient.class),
                        createConfiguration(),
                        mock(FullJitterBackoff.class));

        DeregisterStreamConsumerResponse expected =
                DeregisterStreamConsumerResponse.builder().build();

        ArgumentCaptor<DeregisterStreamConsumerRequest> requestCaptor =
                ArgumentCaptor.forClass(DeregisterStreamConsumerRequest.class);
        when(client.deregisterStreamConsumer(requestCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(expected));

        DeregisterStreamConsumerResponse actual = proxy.deregisterStreamConsumer("arn");

        assertEquals(expected, actual);

        DeregisterStreamConsumerRequest request = requestCaptor.getValue();
        assertEquals("arn", request.consumerARN());
    }

    @Test
    public void testDeregisterStreamConsumerBackoffJitter() throws Exception {
        FullJitterBackoff backoff = mock(FullJitterBackoff.class);
        KinesisAsyncClient client = mock(KinesisAsyncClient.class);
        KinesisProxyV2 proxy =
                new KinesisProxyV2(
                        client, mock(SdkAsyncHttpClient.class), createConfiguration(), backoff);

        when(client.deregisterStreamConsumer(any(DeregisterStreamConsumerRequest.class)))
                .thenThrow(new RuntimeException(LimitExceededException.builder().build()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                DeregisterStreamConsumerResponse.builder().build()));

        proxy.deregisterStreamConsumer("arn");

        verify(backoff).sleep(anyLong());
        verify(backoff)
                .calculateFullJitterBackoff(
                        EXPECTED_DEREGISTRATION_BASE,
                        EXPECTED_DEREGISTRATION_MAX,
                        EXPECTED_DEREGISTRATION_POW,
                        1);
    }

    @Test
    public void testDescribeStreamConsumerWithStreamConsumerArn() throws Exception {
        KinesisAsyncClient client = mock(KinesisAsyncClient.class);
        KinesisProxyV2 proxy =
                new KinesisProxyV2(
                        client,
                        mock(SdkAsyncHttpClient.class),
                        createConfiguration(),
                        mock(FullJitterBackoff.class));

        DescribeStreamConsumerResponse expected = DescribeStreamConsumerResponse.builder().build();

        ArgumentCaptor<DescribeStreamConsumerRequest> requestCaptor =
                ArgumentCaptor.forClass(DescribeStreamConsumerRequest.class);
        when(client.describeStreamConsumer(requestCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(expected));

        DescribeStreamConsumerResponse actual = proxy.describeStreamConsumer("arn");

        assertEquals(expected, actual);

        DescribeStreamConsumerRequest request = requestCaptor.getValue();
        assertEquals("arn", request.consumerARN());
    }

    @Test
    public void testDescribeStreamConsumerWithStreamArnAndConsumerName() throws Exception {
        KinesisAsyncClient client = mock(KinesisAsyncClient.class);
        KinesisProxyV2 proxy =
                new KinesisProxyV2(
                        client,
                        mock(SdkAsyncHttpClient.class),
                        createConfiguration(),
                        mock(FullJitterBackoff.class));

        DescribeStreamConsumerResponse expected = DescribeStreamConsumerResponse.builder().build();

        ArgumentCaptor<DescribeStreamConsumerRequest> requestCaptor =
                ArgumentCaptor.forClass(DescribeStreamConsumerRequest.class);
        when(client.describeStreamConsumer(requestCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(expected));

        DescribeStreamConsumerResponse actual = proxy.describeStreamConsumer("arn", "name");

        assertEquals(expected, actual);

        DescribeStreamConsumerRequest request = requestCaptor.getValue();
        assertEquals("arn", request.streamARN());
        assertEquals("name", request.consumerName());
    }

    @Test
    public void testDescribeStreamConsumerBackoffJitter() throws Exception {
        FullJitterBackoff backoff = mock(FullJitterBackoff.class);
        KinesisAsyncClient client = mock(KinesisAsyncClient.class);
        KinesisProxyV2 proxy =
                new KinesisProxyV2(
                        client, mock(SdkAsyncHttpClient.class), createConfiguration(), backoff);

        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class)))
                .thenThrow(new RuntimeException(LimitExceededException.builder().build()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                DescribeStreamConsumerResponse.builder().build()));

        proxy.describeStreamConsumer("arn");

        verify(backoff).sleep(anyLong());
        verify(backoff)
                .calculateFullJitterBackoff(
                        EXPECTED_DESCRIBE_CONSUMER_BASE,
                        EXPECTED_DESCRIBE_CONSUMER_MAX,
                        EXPECTED_DESCRIBE_CONSUMER_POW,
                        1);
    }

    @Test
    public void testDescribeStreamSummary() throws Exception {
        KinesisAsyncClient client = mock(KinesisAsyncClient.class);
        KinesisProxyV2 proxy =
                new KinesisProxyV2(
                        client,
                        mock(SdkAsyncHttpClient.class),
                        createConfiguration(),
                        mock(FullJitterBackoff.class));

        DescribeStreamSummaryResponse expected = DescribeStreamSummaryResponse.builder().build();

        ArgumentCaptor<DescribeStreamSummaryRequest> requestCaptor =
                ArgumentCaptor.forClass(DescribeStreamSummaryRequest.class);
        when(client.describeStreamSummary(requestCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(expected));

        DescribeStreamSummaryResponse actual = proxy.describeStreamSummary("stream");

        assertEquals(expected, actual);

        DescribeStreamSummaryRequest request = requestCaptor.getValue();
        assertEquals("stream", request.streamName());
    }

    @Test
    public void testDescribeStreamSummaryBackoffJitter() throws Exception {
        FullJitterBackoff backoff = mock(FullJitterBackoff.class);
        KinesisAsyncClient client = mock(KinesisAsyncClient.class);
        KinesisProxyV2 proxy =
                new KinesisProxyV2(
                        client, mock(SdkAsyncHttpClient.class), createConfiguration(), backoff);

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenThrow(new RuntimeException(LimitExceededException.builder().build()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                DescribeStreamSummaryResponse.builder().build()));

        proxy.describeStreamSummary("arn");

        verify(backoff).sleep(anyLong());
        verify(backoff)
                .calculateFullJitterBackoff(
                        EXPECTED_DESCRIBE_STREAM_BASE,
                        EXPECTED_DESCRIBE_STREAM_MAX,
                        EXPECTED_DESCRIBE_STREAM_POW,
                        1);
    }

    @Test
    public void testDescribeStreamSummaryFailsAfterMaxRetries() throws Exception {
        exception.expect(RuntimeException.class);
        exception.expectMessage("Retries exceeded - all 10 retry attempts failed.");

        FullJitterBackoff backoff = mock(FullJitterBackoff.class);
        KinesisAsyncClient client = mock(KinesisAsyncClient.class);
        KinesisProxyV2 proxy =
                new KinesisProxyV2(
                        client, mock(SdkAsyncHttpClient.class), createConfiguration(), backoff);

        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class)))
                .thenThrow(new RuntimeException(LimitExceededException.builder().build()));

        proxy.describeStreamSummary("arn");
    }

    private FanOutRecordPublisherConfiguration createConfiguration() {
        return new FanOutRecordPublisherConfiguration(createEfoProperties(), emptyList());
    }

    private Properties createEfoProperties() {
        Properties config = new Properties();
        config.setProperty(RECORD_PUBLISHER_TYPE, EFO.name());
        config.setProperty(EFO_CONSUMER_NAME, "dummy-efo-consumer");
        config.setProperty(
                SUBSCRIBE_TO_SHARD_BACKOFF_BASE, String.valueOf(EXPECTED_SUBSCRIBE_TO_SHARD_BASE));
        config.setProperty(
                SUBSCRIBE_TO_SHARD_BACKOFF_MAX, String.valueOf(EXPECTED_SUBSCRIBE_TO_SHARD_MAX));
        config.setProperty(
                SUBSCRIBE_TO_SHARD_BACKOFF_EXPONENTIAL_CONSTANT,
                String.valueOf(EXPECTED_SUBSCRIBE_TO_SHARD_POW));
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
        config.setProperty(
                DESCRIBE_STREAM_CONSUMER_BACKOFF_BASE,
                String.valueOf(EXPECTED_DESCRIBE_CONSUMER_BASE));
        config.setProperty(
                DESCRIBE_STREAM_CONSUMER_BACKOFF_MAX,
                String.valueOf(EXPECTED_DESCRIBE_CONSUMER_MAX));
        config.setProperty(
                DESCRIBE_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT,
                String.valueOf(EXPECTED_DESCRIBE_CONSUMER_POW));
        config.setProperty(
                STREAM_DESCRIBE_BACKOFF_BASE, String.valueOf(EXPECTED_DESCRIBE_STREAM_BASE));
        config.setProperty(
                STREAM_DESCRIBE_BACKOFF_MAX, String.valueOf(EXPECTED_DESCRIBE_STREAM_MAX));
        config.setProperty(
                STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT,
                String.valueOf(EXPECTED_DESCRIBE_STREAM_POW));
        config.setProperty(
                STREAM_DESCRIBE_RETRIES, String.valueOf(EXPECTED_DESCRIBE_STREAM_RETRIES));
        return config;
    }
}
