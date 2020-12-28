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

import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.efoConsumerArn;

/** This is a configuration class for enhanced fan-out components. */
public class FanOutRecordPublisherConfiguration {

    /** The efo registration type for de-/registration of streams. */
    private final EFORegistrationType efoRegistrationType;

    /**
     * The efo stream consumer name. Should not be Null if the efoRegistrationType is either LAZY or
     * EAGER.
     */
    @Nullable private String consumerName;

    /** A map of stream to stream consumer ARN for EFO subscriptions. */
    private final Map<String, String> streamConsumerArns = new HashMap<>();

    /** Base backoff millis for the deregister stream operation. */
    private final int subscribeToShardMaxRetries;

    /** Maximum backoff millis for the subscribe to shard operation. */
    private final long subscribeToShardMaxBackoffMillis;

    /** Base backoff millis for the subscribe to shard operation. */
    private final long subscribeToShardBaseBackoffMillis;

    /** Exponential backoff power constant for the subscribe to shard operation. */
    private final double subscribeToShardExpConstant;

    /** Base backoff millis for the register stream operation. */
    private final long registerStreamBaseBackoffMillis;

    /** Maximum backoff millis for the register stream operation. */
    private final long registerStreamMaxBackoffMillis;

    /** Exponential backoff power constant for the register stream operation. */
    private final double registerStreamExpConstant;

    /** Maximum retry attempts for the register stream operation. */
    private final int registerStreamMaxRetries;

    /** Maximum time to wait for a stream consumer to become active before giving up. */
    private final Duration registerStreamConsumerTimeout;

    /** Base backoff millis for the deregister stream operation. */
    private final long deregisterStreamBaseBackoffMillis;

    /** Maximum backoff millis for the deregister stream operation. */
    private final long deregisterStreamMaxBackoffMillis;

    /** Exponential backoff power constant for the deregister stream operation. */
    private final double deregisterStreamExpConstant;

    /** Maximum retry attempts for the deregister stream operation. */
    private final int deregisterStreamMaxRetries;

    /** Maximum time to wait for a stream consumer to deregister before giving up. */
    private final Duration deregisterStreamConsumerTimeout;

    /** Max retries for the describe stream operation. */
    private final int describeStreamMaxRetries;

    /** Backoff millis for the describe stream operation. */
    private final long describeStreamBaseBackoffMillis;

    /** Maximum backoff millis for the describe stream operation. */
    private final long describeStreamMaxBackoffMillis;

    /** Exponential backoff power constant for the describe stream operation. */
    private final double describeStreamExpConstant;

    /** Max retries for the describe stream consumer operation. */
    private final int describeStreamConsumerMaxRetries;

    /** Backoff millis for the describe stream consumer operation. */
    private final long describeStreamConsumerBaseBackoffMillis;

    /** Maximum backoff millis for the describe stream consumer operation. */
    private final long describeStreamConsumerMaxBackoffMillis;

    /** Exponential backoff power constant for the describe stream consumer operation. */
    private final double describeStreamConsumerExpConstant;

    /**
     * Creates a FanOutRecordPublisherConfiguration.
     *
     * @param configProps the configuration properties from config file.
     * @param streams the streams which is sent to match the EFO consumer arn if the EFO
     *     registration mode is set to `NONE`.
     */
    public FanOutRecordPublisherConfiguration(
            final Properties configProps, final List<String> streams) {
        Preconditions.checkArgument(
                configProps
                        .getProperty(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE)
                        .equals(RecordPublisherType.EFO.toString()),
                "Only efo record publisher can register a FanOutProperties.");
        KinesisConfigUtil.validateEfoConfiguration(configProps, streams);

        efoRegistrationType =
                EFORegistrationType.valueOf(
                        configProps.getProperty(
                                ConsumerConfigConstants.EFO_REGISTRATION_TYPE,
                                EFORegistrationType.EAGER.toString()));
        // if efo registration type is EAGER|LAZY, then user should explicitly provide a consumer
        // name for each stream.
        if (efoRegistrationType == EFORegistrationType.EAGER
                || efoRegistrationType == EFORegistrationType.LAZY) {
            consumerName = configProps.getProperty(ConsumerConfigConstants.EFO_CONSUMER_NAME);
        }

        for (String stream : streams) {
            String key = efoConsumerArn(stream);
            if (configProps.containsKey(key)) {
                streamConsumerArns.put(stream, configProps.getProperty(key));
            }
        }

        this.subscribeToShardMaxRetries =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_RETRIES))
                        .map(Integer::parseInt)
                        .orElse(ConsumerConfigConstants.DEFAULT_SUBSCRIBE_TO_SHARD_RETRIES);
        this.subscribeToShardBaseBackoffMillis =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_BASE))
                        .map(Long::parseLong)
                        .orElse(ConsumerConfigConstants.DEFAULT_SUBSCRIBE_TO_SHARD_BACKOFF_BASE);
        this.subscribeToShardMaxBackoffMillis =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_MAX))
                        .map(Long::parseLong)
                        .orElse(ConsumerConfigConstants.DEFAULT_SUBSCRIBE_TO_SHARD_BACKOFF_MAX);
        this.subscribeToShardExpConstant =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants
                                                .SUBSCRIBE_TO_SHARD_BACKOFF_EXPONENTIAL_CONSTANT))
                        .map(Double::parseDouble)
                        .orElse(
                                ConsumerConfigConstants
                                        .DEFAULT_SUBSCRIBE_TO_SHARD_BACKOFF_EXPONENTIAL_CONSTANT);

        this.registerStreamBaseBackoffMillis =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_BASE))
                        .map(Long::parseLong)
                        .orElse(ConsumerConfigConstants.DEFAULT_REGISTER_STREAM_BACKOFF_BASE);
        this.registerStreamMaxBackoffMillis =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_MAX))
                        .map(Long::parseLong)
                        .orElse(ConsumerConfigConstants.DEFAULT_REGISTER_STREAM_BACKOFF_MAX);
        this.registerStreamExpConstant =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants
                                                .REGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT))
                        .map(Double::parseDouble)
                        .orElse(
                                ConsumerConfigConstants
                                        .DEFAULT_REGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT);
        this.registerStreamMaxRetries =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.REGISTER_STREAM_RETRIES))
                        .map(Integer::parseInt)
                        .orElse(ConsumerConfigConstants.DEFAULT_REGISTER_STREAM_RETRIES);
        this.registerStreamConsumerTimeout =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.REGISTER_STREAM_TIMEOUT_SECONDS))
                        .map(Integer::parseInt)
                        .map(Duration::ofSeconds)
                        .orElse(ConsumerConfigConstants.DEFAULT_REGISTER_STREAM_TIMEOUT);

        this.deregisterStreamBaseBackoffMillis =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_BASE))
                        .map(Long::parseLong)
                        .orElse(ConsumerConfigConstants.DEFAULT_DEREGISTER_STREAM_BACKOFF_BASE);
        this.deregisterStreamMaxBackoffMillis =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_MAX))
                        .map(Long::parseLong)
                        .orElse(ConsumerConfigConstants.DEFAULT_DEREGISTER_STREAM_BACKOFF_MAX);
        this.deregisterStreamExpConstant =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants
                                                .DEREGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT))
                        .map(Double::parseDouble)
                        .orElse(
                                ConsumerConfigConstants
                                        .DEFAULT_DEREGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT);
        this.deregisterStreamMaxRetries =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.DEREGISTER_STREAM_RETRIES))
                        .map(Integer::parseInt)
                        .orElse(ConsumerConfigConstants.DEFAULT_DEREGISTER_STREAM_RETRIES);
        this.deregisterStreamConsumerTimeout =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.DEREGISTER_STREAM_TIMEOUT_SECONDS))
                        .map(Integer::parseInt)
                        .map(Duration::ofSeconds)
                        .orElse(ConsumerConfigConstants.DEFAULT_DEREGISTER_STREAM_TIMEOUT);

        this.describeStreamMaxRetries =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.STREAM_DESCRIBE_RETRIES))
                        .map(Integer::parseInt)
                        .orElse(ConsumerConfigConstants.DEFAULT_STREAM_DESCRIBE_RETRIES);
        this.describeStreamBaseBackoffMillis =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_BASE))
                        .map(Long::parseLong)
                        .orElse(ConsumerConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE);
        this.describeStreamMaxBackoffMillis =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_MAX))
                        .map(Long::parseLong)
                        .orElse(ConsumerConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX);
        this.describeStreamExpConstant =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants
                                                .STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT))
                        .map(Double::parseDouble)
                        .orElse(
                                ConsumerConfigConstants
                                        .DEFAULT_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT);
        this.describeStreamConsumerMaxRetries =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_RETRIES))
                        .map(Integer::parseInt)
                        .orElse(ConsumerConfigConstants.DEFAULT_DESCRIBE_STREAM_CONSUMER_RETRIES);
        this.describeStreamConsumerBaseBackoffMillis =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants
                                                .DESCRIBE_STREAM_CONSUMER_BACKOFF_BASE))
                        .map(Long::parseLong)
                        .orElse(
                                ConsumerConfigConstants
                                        .DEFAULT_DESCRIBE_STREAM_CONSUMER_BACKOFF_BASE);
        this.describeStreamConsumerMaxBackoffMillis =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants
                                                .DESCRIBE_STREAM_CONSUMER_BACKOFF_MAX))
                        .map(Long::parseLong)
                        .orElse(
                                ConsumerConfigConstants
                                        .DEFAULT_DESCRIBE_STREAM_CONSUMER_BACKOFF_MAX);
        this.describeStreamConsumerExpConstant =
                Optional.ofNullable(
                                configProps.getProperty(
                                        ConsumerConfigConstants
                                                .DESCRIBE_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT))
                        .map(Double::parseDouble)
                        .orElse(
                                ConsumerConfigConstants
                                        .DEFAULT_DESCRIBE_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT);
    }

    // ------------------------------------------------------------------------
    //  subscribeToShard() related performance settings
    // ------------------------------------------------------------------------

    /** Get maximum retry attempts for the subscribe to shard operation. */
    public int getSubscribeToShardMaxRetries() {
        return subscribeToShardMaxRetries;
    }

    /** Get maximum backoff millis for the subscribe to shard operation. */
    public long getSubscribeToShardMaxBackoffMillis() {
        return subscribeToShardMaxBackoffMillis;
    }

    /** Get base backoff millis for the subscribe to shard operation. */
    public long getSubscribeToShardBaseBackoffMillis() {
        return subscribeToShardBaseBackoffMillis;
    }

    /** Get exponential backoff power constant for the subscribe to shard operation. */
    public double getSubscribeToShardExpConstant() {
        return subscribeToShardExpConstant;
    }

    // ------------------------------------------------------------------------
    //  registerStream() related performance settings
    // ------------------------------------------------------------------------

    /** Get base backoff millis for the register stream operation. */
    public long getRegisterStreamBaseBackoffMillis() {
        return registerStreamBaseBackoffMillis;
    }

    /** Get maximum backoff millis for the register stream operation. */
    public long getRegisterStreamMaxBackoffMillis() {
        return registerStreamMaxBackoffMillis;
    }

    /** Get exponential backoff power constant for the register stream operation. */
    public double getRegisterStreamExpConstant() {
        return registerStreamExpConstant;
    }

    /** Get maximum retry attempts for the register stream operation. */
    public int getRegisterStreamMaxRetries() {
        return registerStreamMaxRetries;
    }

    /** Get maximum duration to wait for a stream consumer to become active before giving up. */
    public Duration getRegisterStreamConsumerTimeout() {
        return registerStreamConsumerTimeout;
    }

    // ------------------------------------------------------------------------
    //  deregisterStream() related performance settings
    // ------------------------------------------------------------------------

    /** Get base backoff millis for the deregister stream operation. */
    public long getDeregisterStreamBaseBackoffMillis() {
        return deregisterStreamBaseBackoffMillis;
    }

    /** Get maximum backoff millis for the deregister stream operation. */
    public long getDeregisterStreamMaxBackoffMillis() {
        return deregisterStreamMaxBackoffMillis;
    }

    /** Get exponential backoff power constant for the deregister stream operation. */
    public double getDeregisterStreamExpConstant() {
        return deregisterStreamExpConstant;
    }

    /** Get maximum retry attempts for the register stream operation. */
    public int getDeregisterStreamMaxRetries() {
        return deregisterStreamMaxRetries;
    }

    /** Get maximum duration to wait for a stream consumer to deregister before giving up. */
    public Duration getDeregisterStreamConsumerTimeout() {
        return deregisterStreamConsumerTimeout;
    }

    // ------------------------------------------------------------------------
    //  describeStream() related performance settings
    // ------------------------------------------------------------------------

    /** Get maximum retry attempts for the describe stream operation. */
    public int getDescribeStreamMaxRetries() {
        return describeStreamMaxRetries;
    }

    /** Get base backoff millis for the describe stream operation. */
    public long getDescribeStreamBaseBackoffMillis() {
        return describeStreamBaseBackoffMillis;
    }

    /** Get maximum backoff millis for the describe stream operation. */
    public long getDescribeStreamMaxBackoffMillis() {
        return describeStreamMaxBackoffMillis;
    }

    /** Get exponential backoff power constant for the describe stream operation. */
    public double getDescribeStreamExpConstant() {
        return describeStreamExpConstant;
    }

    // ------------------------------------------------------------------------
    //  describeStreamConsumer() related performance settings
    // ------------------------------------------------------------------------

    /** Get maximum retry attempts for the describe stream operation. */
    public int getDescribeStreamConsumerMaxRetries() {
        return describeStreamConsumerMaxRetries;
    }

    /** Get base backoff millis for the describe stream operation. */
    public long getDescribeStreamConsumerBaseBackoffMillis() {
        return describeStreamConsumerBaseBackoffMillis;
    }

    /** Get maximum backoff millis for the describe stream operation. */
    public long getDescribeStreamConsumerMaxBackoffMillis() {
        return describeStreamConsumerMaxBackoffMillis;
    }

    /** Get exponential backoff power constant for the describe stream operation. */
    public double getDescribeStreamConsumerExpConstant() {
        return describeStreamConsumerExpConstant;
    }

    /** Get efo registration type. */
    public EFORegistrationType getEfoRegistrationType() {
        return efoRegistrationType;
    }

    /** Get consumer name, will be null if efo registration type is 'NONE'. */
    public Optional<String> getConsumerName() {
        return Optional.ofNullable(consumerName);
    }

    /**
     * Get the according consumer arn to the stream, will be null if efo registration type is 'LAZY'
     * or 'EAGER'.
     */
    public Optional<String> getStreamConsumerArn(String stream) {
        return Optional.ofNullable(streamConsumerArns.get(stream));
    }
}
