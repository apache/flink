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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisException.FlinkKinesisTimeoutException;
import org.apache.flink.streaming.connectors.kinesis.proxy.FullJitterBackoff;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType.LAZY;
import static software.amazon.awssdk.services.kinesis.model.ConsumerStatus.ACTIVE;
import static software.amazon.awssdk.services.kinesis.model.ConsumerStatus.DELETING;

/**
 * Responsible for registering and deregistering EFO stream consumers. Will block until consumers
 * are ready.
 */
@Internal
public class StreamConsumerRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(StreamConsumerRegistrar.class);

    private final KinesisProxyV2Interface kinesisProxyV2Interface;

    private final FanOutRecordPublisherConfiguration configuration;

    private final FullJitterBackoff backoff;

    public StreamConsumerRegistrar(
            final KinesisProxyV2Interface kinesisProxyV2Interface,
            final FanOutRecordPublisherConfiguration configuration,
            final FullJitterBackoff backoff) {
        this.kinesisProxyV2Interface = Preconditions.checkNotNull(kinesisProxyV2Interface);
        this.configuration = Preconditions.checkNotNull(configuration);
        this.backoff = Preconditions.checkNotNull(backoff);
    }

    /**
     * Register a stream consumer with the given name against the given stream. Blocks until the
     * consumer becomes active. If the stream consumer already exists, the ARN is returned.
     *
     * @param stream the stream to register the stream consumer against
     * @param streamConsumerName the name of the new stream consumer
     * @return the stream consumer ARN
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public String registerStreamConsumer(final String stream, final String streamConsumerName)
            throws ExecutionException, InterruptedException {
        LOG.debug("Registering stream consumer - {}::{}", stream, streamConsumerName);

        int attempt = 1;

        if (configuration.getEfoRegistrationType() == LAZY) {
            registrationBackoff(configuration, backoff, attempt++);
        }

        DescribeStreamSummaryResponse describeStreamSummaryResponse =
                kinesisProxyV2Interface.describeStreamSummary(stream);
        String streamArn = describeStreamSummaryResponse.streamDescriptionSummary().streamARN();

        LOG.debug("Found stream ARN - {}", streamArn);

        Optional<DescribeStreamConsumerResponse> describeStreamConsumerResponse =
                describeStreamConsumer(streamArn, streamConsumerName);

        if (!describeStreamConsumerResponse.isPresent()) {
            invokeIgnoringResourceInUse(
                    () ->
                            kinesisProxyV2Interface.registerStreamConsumer(
                                    streamArn, streamConsumerName));
        }

        String streamConsumerArn =
                waitForConsumerToBecomeActive(
                        describeStreamConsumerResponse.orElse(null),
                        streamArn,
                        streamConsumerName,
                        attempt);

        LOG.debug("Using stream consumer - {}", streamConsumerArn);

        return streamConsumerArn;
    }

    /**
     * Deregister the stream consumer with the given ARN. Blocks until the consumer is deleted.
     *
     * @param stream the stream in which to deregister the consumer
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void deregisterStreamConsumer(final String stream)
            throws InterruptedException, ExecutionException {
        LOG.debug("Deregistering stream consumer - {}", stream);

        int attempt = 1;
        String streamConsumerArn = getStreamConsumerArn(stream);

        deregistrationBackoff(configuration, backoff, attempt++);

        Optional<DescribeStreamConsumerResponse> response =
                describeStreamConsumer(streamConsumerArn);
        if (response.isPresent()
                && response.get().consumerDescription().consumerStatus() != DELETING) {
            invokeIgnoringResourceInUse(
                    () -> kinesisProxyV2Interface.deregisterStreamConsumer(streamConsumerArn));
        }

        waitForConsumerToDeregister(response.orElse(null), streamConsumerArn, attempt);

        LOG.debug("Deregistered stream consumer - {}", streamConsumerArn);
    }

    /** Destroy any open resources used by the factory. */
    public void close() {
        kinesisProxyV2Interface.close();
    }

    @VisibleForTesting
    void registrationBackoff(
            final FanOutRecordPublisherConfiguration configuration,
            final FullJitterBackoff backoff,
            int attempt)
            throws InterruptedException {
        long backoffMillis =
                backoff.calculateFullJitterBackoff(
                        configuration.getRegisterStreamBaseBackoffMillis(),
                        configuration.getRegisterStreamMaxBackoffMillis(),
                        configuration.getRegisterStreamExpConstant(),
                        attempt);

        backoff.sleep(backoffMillis);
    }

    @VisibleForTesting
    void deregistrationBackoff(
            final FanOutRecordPublisherConfiguration configuration,
            final FullJitterBackoff backoff,
            int attempt)
            throws InterruptedException {
        long backoffMillis =
                backoff.calculateFullJitterBackoff(
                        configuration.getDeregisterStreamBaseBackoffMillis(),
                        configuration.getDeregisterStreamMaxBackoffMillis(),
                        configuration.getDeregisterStreamExpConstant(),
                        attempt);

        backoff.sleep(backoffMillis);
    }

    private String waitForConsumerToBecomeActive(
            @Nullable final DescribeStreamConsumerResponse describeStreamConsumerResponse,
            final String streamArn,
            final String streamConsumerName,
            final int initialAttempt)
            throws InterruptedException, ExecutionException {
        int attempt = initialAttempt;

        Instant start = Instant.now();
        Duration timeout = configuration.getRegisterStreamConsumerTimeout();

        DescribeStreamConsumerResponse response = describeStreamConsumerResponse;
        while (response == null || response.consumerDescription().consumerStatus() != ACTIVE) {
            LOG.debug(
                    "Waiting for stream consumer to become active, attempt {} - {} on {}",
                    attempt,
                    streamConsumerName,
                    streamArn);
            registrationBackoff(configuration, backoff, attempt++);
            response =
                    kinesisProxyV2Interface.describeStreamConsumer(streamArn, streamConsumerName);

            if (Duration.between(start, Instant.now()).compareTo(timeout) > 0) {
                throw new FlinkKinesisTimeoutException(
                        "Timeout waiting for stream consumer to become active: "
                                + streamConsumerName
                                + " on "
                                + streamArn);
            }
        }

        return response.consumerDescription().consumerARN();
    }

    private void waitForConsumerToDeregister(
            @Nullable final DescribeStreamConsumerResponse describeStreamConsumerResponse,
            final String streamConsumerArn,
            final int initialAttempt)
            throws InterruptedException, ExecutionException {
        int attempt = initialAttempt;

        Instant start = Instant.now();
        Duration timeout = configuration.getDeregisterStreamConsumerTimeout();

        Optional<DescribeStreamConsumerResponse> response =
                Optional.ofNullable(describeStreamConsumerResponse);
        while (response.isPresent()
                && response.get().consumerDescription().consumerStatus() != DELETING) {
            LOG.debug(
                    "Waiting for stream consumer to deregister, attempt {} - {}",
                    attempt,
                    streamConsumerArn);
            deregistrationBackoff(configuration, backoff, attempt++);
            response = describeStreamConsumer(streamConsumerArn);

            if (Duration.between(start, Instant.now()).compareTo(timeout) > 0) {
                throw new FlinkKinesisTimeoutException(
                        "Timeout waiting for stream consumer to deregister: " + streamConsumerArn);
            }
        }
    }

    private Optional<DescribeStreamConsumerResponse> describeStreamConsumer(
            final String streamArn, final String streamConsumerName)
            throws InterruptedException, ExecutionException {
        return describeStreamConsumer(
                () ->
                        kinesisProxyV2Interface.describeStreamConsumer(
                                streamArn, streamConsumerName));
    }

    private Optional<DescribeStreamConsumerResponse> describeStreamConsumer(
            final String streamConsumerArn) throws InterruptedException, ExecutionException {
        return describeStreamConsumer(
                () -> kinesisProxyV2Interface.describeStreamConsumer(streamConsumerArn));
    }

    private Optional<DescribeStreamConsumerResponse> describeStreamConsumer(
            final ResponseSupplier<DescribeStreamConsumerResponse> responseSupplier)
            throws InterruptedException, ExecutionException {
        DescribeStreamConsumerResponse response;

        try {
            response = responseSupplier.get();
        } catch (ExecutionException ex) {
            if (isResourceNotFound(ex)) {
                return Optional.empty();
            }

            throw ex;
        }

        return Optional.ofNullable(response);
    }

    private <T> void invokeIgnoringResourceInUse(final ResponseSupplier<T> responseSupplier)
            throws InterruptedException, ExecutionException {
        try {
            responseSupplier.get();
        } catch (ExecutionException ex) {
            if (isResourceInUse(ex)) {
                // The stream consumer may have been created since we performed the describe
                return;
            }

            throw ex;
        }
    }

    private boolean isResourceNotFound(final ExecutionException ex) {
        return ex.getCause() instanceof ResourceNotFoundException;
    }

    private boolean isResourceInUse(final ExecutionException ex) {
        return ex.getCause() instanceof ResourceInUseException;
    }

    private String getStreamConsumerArn(final String stream) {
        Optional<String> streamConsumerArn = configuration.getStreamConsumerArn(stream);
        if (!streamConsumerArn.isPresent()) {
            throw new IllegalArgumentException(
                    "Stream consumer ARN not found for stream: " + stream);
        }

        return streamConsumerArn.get();
    }

    private interface ResponseSupplier<T> {
        T get() throws ExecutionException, InterruptedException;
    }
}
