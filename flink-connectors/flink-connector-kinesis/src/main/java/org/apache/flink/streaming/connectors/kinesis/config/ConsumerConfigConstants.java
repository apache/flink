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

package org.apache.flink.streaming.connectors.kinesis.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.internals.ShardConsumer;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;

import com.amazonaws.services.kinesis.model.ShardIteratorType;

import java.time.Duration;

/**
 * Optional consumer specific configuration keys and default values for {@link
 * FlinkKinesisConsumer}.
 */
@PublicEvolving
public class ConsumerConfigConstants extends AWSConfigConstants {

    /**
     * The initial position to start reading shards from. This will affect the {@link
     * ShardIteratorType} used when the consumer tasks retrieve the first shard iterator for each
     * Kinesis shard.
     */
    public enum InitialPosition {

        /**
         * Start reading from the earliest possible record in the stream (excluding expired data
         * records).
         */
        TRIM_HORIZON(SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM),

        /** Start reading from the latest incoming record. */
        LATEST(SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM),

        /** Start reading from the record at the specified timestamp. */
        AT_TIMESTAMP(SentinelSequenceNumber.SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM);

        private SentinelSequenceNumber sentinelSequenceNumber;

        InitialPosition(SentinelSequenceNumber sentinelSequenceNumber) {
            this.sentinelSequenceNumber = sentinelSequenceNumber;
        }

        public SentinelSequenceNumber toSentinelSequenceNumber() {
            return this.sentinelSequenceNumber;
        }
    }

    /** The record publisher type represents the record-consume style. */
    public enum RecordPublisherType {

        /** Consume the Kinesis records using AWS SDK v2 with the enhanced fan-out consumer. */
        EFO,
        /** Consume the Kinesis records using AWS SDK v1 with the get-records method. */
        POLLING
    }

    /** The EFO registration type reprsents how we are going to de-/register efo consumer. */
    public enum EFORegistrationType {

        /**
         * Delay the registration of efo consumer for taskmanager to execute. De-register the efo
         * consumer for taskmanager to execute when task is shut down.
         */
        LAZY,
        /**
         * Register the efo consumer eagerly for jobmanager to execute. De-register the efo consumer
         * the same way as lazy does.
         */
        EAGER,
        /** Do not register efo consumer programmatically. Do not de-register either. */
        NONE
    }

    /** The RecordPublisher type (EFO|POLLING, default is POLLING). */
    public static final String RECORD_PUBLISHER_TYPE = "flink.stream.recordpublisher";

    /** The name of the EFO consumer to register with KDS. */
    public static final String EFO_CONSUMER_NAME = "flink.stream.efo.consumername";

    /**
     * Determine how and when consumer de-/registration is performed (LAZY|EAGER|NONE, default is
     * LAZY).
     */
    public static final String EFO_REGISTRATION_TYPE = "flink.stream.efo.registration";

    /** The prefix of consumer ARN for a given stream. */
    public static final String EFO_CONSUMER_ARN_PREFIX = "flink.stream.efo.consumerarn";

    /** The initial position to start reading Kinesis streams from (LATEST is used if not set). */
    public static final String STREAM_INITIAL_POSITION = "flink.stream.initpos";

    /**
     * The initial timestamp to start reading Kinesis stream from (when AT_TIMESTAMP is set for
     * STREAM_INITIAL_POSITION).
     */
    public static final String STREAM_INITIAL_TIMESTAMP = "flink.stream.initpos.timestamp";

    /**
     * The date format of initial timestamp to start reading Kinesis stream from (when AT_TIMESTAMP
     * is set for STREAM_INITIAL_POSITION).
     */
    public static final String STREAM_TIMESTAMP_DATE_FORMAT =
            "flink.stream.initpos.timestamp.format";

    /** The maximum number of describeStream attempts if we get a recoverable exception. */
    public static final String STREAM_DESCRIBE_RETRIES = "flink.stream.describe.maxretries";

    /**
     * The base backoff time between each describeStream attempt (for consuming from DynamoDB
     * streams).
     */
    public static final String STREAM_DESCRIBE_BACKOFF_BASE = "flink.stream.describe.backoff.base";

    /**
     * The maximum backoff time between each describeStream attempt (for consuming from DynamoDB
     * streams).
     */
    public static final String STREAM_DESCRIBE_BACKOFF_MAX = "flink.stream.describe.backoff.max";

    /**
     * The power constant for exponential backoff between each describeStream attempt (for consuming
     * from DynamoDB streams).
     */
    public static final String STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT =
            "flink.stream.describe.backoff.expconst";

    /** The maximum number of listShards attempts if we get a recoverable exception. */
    public static final String LIST_SHARDS_RETRIES = "flink.list.shards.maxretries";

    /** The base backoff time between each listShards attempt. */
    public static final String LIST_SHARDS_BACKOFF_BASE = "flink.list.shards.backoff.base";

    /** The maximum backoff time between each listShards attempt. */
    public static final String LIST_SHARDS_BACKOFF_MAX = "flink.list.shards.backoff.max";

    /** The power constant for exponential backoff between each listShards attempt. */
    public static final String LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT =
            "flink.list.shards.backoff.expconst";

    /** The maximum number of describeStreamConsumer attempts if we get a recoverable exception. */
    public static final String DESCRIBE_STREAM_CONSUMER_RETRIES =
            "flink.stream.describestreamconsumer.maxretries";

    /** The base backoff time between each describeStreamConsumer attempt. */
    public static final String DESCRIBE_STREAM_CONSUMER_BACKOFF_BASE =
            "flink.stream.describestreamconsumer.backoff.base";

    /** The maximum backoff time between each describeStreamConsumer attempt. */
    public static final String DESCRIBE_STREAM_CONSUMER_BACKOFF_MAX =
            "flink.stream.describestreamconsumer.backoff.max";

    /** The power constant for exponential backoff between each describeStreamConsumer attempt. */
    public static final String DESCRIBE_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT =
            "flink.stream.describestreamconsumer.backoff.expconst";

    /** The maximum number of registerStream attempts if we get a recoverable exception. */
    public static final String REGISTER_STREAM_RETRIES =
            "flink.stream.registerstreamconsumer.maxretries";

    /**
     * The maximum time in seconds to wait for a stream consumer to become active before giving up.
     */
    public static final String REGISTER_STREAM_TIMEOUT_SECONDS =
            "flink.stream.registerstreamconsumer.timeout";

    /** The base backoff time between each registerStream attempt. */
    public static final String REGISTER_STREAM_BACKOFF_BASE =
            "flink.stream.registerstreamconsumer.backoff.base";

    /** The maximum backoff time between each registerStream attempt. */
    public static final String REGISTER_STREAM_BACKOFF_MAX =
            "flink.stream.registerstreamconsumer.backoff.max";

    /** The power constant for exponential backoff between each registerStream attempt. */
    public static final String REGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT =
            "flink.stream.registerstreamconsumer.backoff.expconst";

    /** The maximum number of deregisterStream attempts if we get a recoverable exception. */
    public static final String DEREGISTER_STREAM_RETRIES =
            "flink.stream.deregisterstreamconsumer.maxretries";

    /** The maximum time in seconds to wait for a stream consumer to deregister before giving up. */
    public static final String DEREGISTER_STREAM_TIMEOUT_SECONDS =
            "flink.stream.deregisterstreamconsumer.timeout";

    /** The base backoff time between each deregisterStream attempt. */
    public static final String DEREGISTER_STREAM_BACKOFF_BASE =
            "flink.stream.deregisterstreamconsumer.backoff.base";

    /** The maximum backoff time between each deregisterStream attempt. */
    public static final String DEREGISTER_STREAM_BACKOFF_MAX =
            "flink.stream.deregisterstreamconsumer.backoff.max";

    /** The power constant for exponential backoff between each deregisterStream attempt. */
    public static final String DEREGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT =
            "flink.stream.deregisterstreamconsumer.backoff.expconst";

    /** The maximum number of subscribeToShard attempts if we get a recoverable exception. */
    public static final String SUBSCRIBE_TO_SHARD_RETRIES =
            "flink.shard.subscribetoshard.maxretries";

    /** A timeout when waiting for a shard subscription to be established. */
    public static final String SUBSCRIBE_TO_SHARD_TIMEOUT_SECONDS =
            "flink.shard.subscribetoshard.timeout";

    /** The base backoff time between each subscribeToShard attempt. */
    public static final String SUBSCRIBE_TO_SHARD_BACKOFF_BASE =
            "flink.shard.subscribetoshard.backoff.base";

    /** The maximum backoff time between each subscribeToShard attempt. */
    public static final String SUBSCRIBE_TO_SHARD_BACKOFF_MAX =
            "flink.shard.subscribetoshard.backoff.max";

    /** The power constant for exponential backoff between each subscribeToShard attempt. */
    public static final String SUBSCRIBE_TO_SHARD_BACKOFF_EXPONENTIAL_CONSTANT =
            "flink.shard.subscribetoshard.backoff.expconst";

    /**
     * The maximum number of records to try to get each time we fetch records from a AWS Kinesis
     * shard.
     */
    public static final String SHARD_GETRECORDS_MAX = "flink.shard.getrecords.maxrecordcount";

    /** The maximum number of getRecords attempts if we get a recoverable exception. */
    public static final String SHARD_GETRECORDS_RETRIES = "flink.shard.getrecords.maxretries";

    /**
     * The base backoff time between getRecords attempts if we get a
     * ProvisionedThroughputExceededException.
     */
    public static final String SHARD_GETRECORDS_BACKOFF_BASE =
            "flink.shard.getrecords.backoff.base";

    /**
     * The maximum backoff time between getRecords attempts if we get a
     * ProvisionedThroughputExceededException.
     */
    public static final String SHARD_GETRECORDS_BACKOFF_MAX = "flink.shard.getrecords.backoff.max";

    /** The power constant for exponential backoff between each getRecords attempt. */
    public static final String SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT =
            "flink.shard.getrecords.backoff.expconst";

    /** The interval between each getRecords request to a AWS Kinesis shard in milliseconds. */
    public static final String SHARD_GETRECORDS_INTERVAL_MILLIS =
            "flink.shard.getrecords.intervalmillis";

    /**
     * The maximum number of getShardIterator attempts if we get
     * ProvisionedThroughputExceededException.
     */
    public static final String SHARD_GETITERATOR_RETRIES = "flink.shard.getiterator.maxretries";

    /**
     * The base backoff time between getShardIterator attempts if we get a
     * ProvisionedThroughputExceededException.
     */
    public static final String SHARD_GETITERATOR_BACKOFF_BASE =
            "flink.shard.getiterator.backoff.base";

    /**
     * The maximum backoff time between getShardIterator attempts if we get a
     * ProvisionedThroughputExceededException.
     */
    public static final String SHARD_GETITERATOR_BACKOFF_MAX =
            "flink.shard.getiterator.backoff.max";

    /** The power constant for exponential backoff between each getShardIterator attempt. */
    public static final String SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT =
            "flink.shard.getiterator.backoff.expconst";

    /** The interval between each attempt to discover new shards. */
    public static final String SHARD_DISCOVERY_INTERVAL_MILLIS =
            "flink.shard.discovery.intervalmillis";

    /** The config to turn on adaptive reads from a shard. */
    public static final String SHARD_USE_ADAPTIVE_READS = "flink.shard.adaptivereads";

    /** The interval after which to consider a shard idle for purposes of watermark generation. */
    public static final String SHARD_IDLE_INTERVAL_MILLIS = "flink.shard.idle.interval";

    /** The interval for periodically synchronizing the shared watermark state. */
    public static final String WATERMARK_SYNC_MILLIS = "flink.watermark.sync.interval";

    /** The maximum delta allowed for the reader to advance ahead of the shared global watermark. */
    public static final String WATERMARK_LOOKAHEAD_MILLIS = "flink.watermark.lookahead.millis";

    /**
     * The maximum number of records that will be buffered before suspending consumption of a shard.
     */
    public static final String WATERMARK_SYNC_QUEUE_CAPACITY =
            "flink.watermark.sync.queue.capacity";

    public static final String EFO_HTTP_CLIENT_MAX_CONCURRENCY =
            "flink.stream.efo.http-client.max-concurrency";

    public static final String EFO_HTTP_CLIENT_READ_TIMEOUT_MILLIS =
            "flink.stream.efo.http-client.read-timeout";

    // ------------------------------------------------------------------------
    //  Default values for consumer configuration
    // ------------------------------------------------------------------------

    public static final String DEFAULT_STREAM_INITIAL_POSITION = InitialPosition.LATEST.toString();

    public static final String DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT =
            "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    public static final int DEFAULT_STREAM_DESCRIBE_RETRIES = 50;

    public static final long DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE = 2000L;

    public static final long DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX = 5000L;

    public static final double DEFAULT_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

    public static final long DEFAULT_LIST_SHARDS_BACKOFF_BASE = 1000L;

    public static final long DEFAULT_LIST_SHARDS_BACKOFF_MAX = 5000L;

    public static final double DEFAULT_LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

    public static final int DEFAULT_LIST_SHARDS_RETRIES = 10;

    public static final int DEFAULT_DESCRIBE_STREAM_CONSUMER_RETRIES = 50;

    public static final long DEFAULT_DESCRIBE_STREAM_CONSUMER_BACKOFF_BASE = 2000L;

    public static final long DEFAULT_DESCRIBE_STREAM_CONSUMER_BACKOFF_MAX = 5000L;

    public static final double DEFAULT_DESCRIBE_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

    public static final int DEFAULT_REGISTER_STREAM_RETRIES = 10;

    public static final Duration DEFAULT_REGISTER_STREAM_TIMEOUT = Duration.ofSeconds(60);

    public static final long DEFAULT_REGISTER_STREAM_BACKOFF_BASE = 500L;

    public static final long DEFAULT_REGISTER_STREAM_BACKOFF_MAX = 2000L;

    public static final double DEFAULT_REGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

    public static final int DEFAULT_DEREGISTER_STREAM_RETRIES = 10;

    public static final Duration DEFAULT_DEREGISTER_STREAM_TIMEOUT = Duration.ofSeconds(60);

    public static final long DEFAULT_DEREGISTER_STREAM_BACKOFF_BASE = 500L;

    public static final long DEFAULT_DEREGISTER_STREAM_BACKOFF_MAX = 2000L;

    public static final double DEFAULT_DEREGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

    public static final int DEFAULT_SUBSCRIBE_TO_SHARD_RETRIES = 10;

    public static final Duration DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT = Duration.ofSeconds(60);

    public static final long DEFAULT_SUBSCRIBE_TO_SHARD_BACKOFF_BASE = 1000L;

    public static final long DEFAULT_SUBSCRIBE_TO_SHARD_BACKOFF_MAX = 2000L;

    public static final double DEFAULT_SUBSCRIBE_TO_SHARD_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

    public static final int DEFAULT_SHARD_GETRECORDS_MAX = 10000;

    public static final int DEFAULT_SHARD_GETRECORDS_RETRIES = 3;

    public static final long DEFAULT_SHARD_GETRECORDS_BACKOFF_BASE = 300L;

    public static final long DEFAULT_SHARD_GETRECORDS_BACKOFF_MAX = 1000L;

    public static final double DEFAULT_SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

    public static final long DEFAULT_SHARD_GETRECORDS_INTERVAL_MILLIS = 200L;

    public static final int DEFAULT_SHARD_GETITERATOR_RETRIES = 3;

    public static final long DEFAULT_SHARD_GETITERATOR_BACKOFF_BASE = 300L;

    public static final long DEFAULT_SHARD_GETITERATOR_BACKOFF_MAX = 1000L;

    public static final double DEFAULT_SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT = 1.5;

    public static final long DEFAULT_SHARD_DISCOVERY_INTERVAL_MILLIS = 10000L;

    public static final boolean DEFAULT_SHARD_USE_ADAPTIVE_READS = false;

    public static final long DEFAULT_SHARD_IDLE_INTERVAL_MILLIS = -1;

    public static final long DEFAULT_WATERMARK_SYNC_MILLIS = 30_000;

    public static final int DEFAULT_EFO_HTTP_CLIENT_MAX_CONCURRENCY = 10_000;

    public static final Duration DEFAULT_EFO_HTTP_CLIENT_READ_TIMEOUT = Duration.ofMinutes(6);

    /**
     * To avoid shard iterator expires in {@link ShardConsumer}s, the value for the configured
     * getRecords interval can not exceed 5 minutes, which is the expire time for retrieved
     * iterators.
     */
    public static final long MAX_SHARD_GETRECORDS_INTERVAL_MILLIS = 300000L;

    /**
     * Build the key of an EFO consumer ARN according to a stream name.
     *
     * @param streamName the stream name the key is built upon.
     * @return a key of EFO consumer ARN.
     */
    public static String efoConsumerArn(final String streamName) {
        return EFO_CONSUMER_ARN_PREFIX + "." + streamName;
    }
}
