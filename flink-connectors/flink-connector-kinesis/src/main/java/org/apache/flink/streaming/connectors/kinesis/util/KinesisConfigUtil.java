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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFORegistrationType;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.InitialPosition;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType;
import org.apache.flink.streaming.connectors.kinesis.config.ProducerConfigConstants;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utilities for Flink Kinesis connector configuration. */
@Internal
public class KinesisConfigUtil {

    /** Maximum number of items to pack into an PutRecords request. */
    protected static final String COLLECTION_MAX_COUNT = "CollectionMaxCount";

    /** Maximum number of items to pack into an aggregated record. */
    protected static final String AGGREGATION_MAX_COUNT = "AggregationMaxCount";

    /**
     * Limits the maximum allowed put rate for a shard, as a percentage of the backend limits. The
     * default value is set as 100% in Flink. KPL's default value is 150% but it makes KPL throw
     * RateLimitExceededException too frequently and breaks Flink sink as a result.
     */
    protected static final String RATE_LIMIT = "RateLimit";

    /** The threading model that KinesisProducer will use. */
    protected static final String THREADING_MODEL = "ThreadingModel";

    /**
     * The maximum number of threads that the native process' thread pool will be configured with.
     */
    protected static final String THREAD_POOL_SIZE = "ThreadPoolSize";

    /** Default values for RateLimit. */
    protected static final long DEFAULT_RATE_LIMIT = 100L;

    /** Default value for ThreadingModel. */
    protected static final KinesisProducerConfiguration.ThreadingModel DEFAULT_THREADING_MODEL =
            KinesisProducerConfiguration.ThreadingModel.POOLED;

    /** Default values for ThreadPoolSize. */
    protected static final int DEFAULT_THREAD_POOL_SIZE = 10;

    /** Validate configuration properties for {@link FlinkKinesisConsumer}. */
    public static void validateConsumerConfiguration(Properties config) {
        validateConsumerConfiguration(config, Collections.emptyList());
    }

    /** Validate configuration properties for {@link FlinkKinesisConsumer}. */
    public static void validateConsumerConfiguration(Properties config, List<String> streams) {
        checkNotNull(config, "config can not be null");

        validateAwsConfiguration(config);

        RecordPublisherType recordPublisherType = validateRecordPublisherType(config);

        if (recordPublisherType == RecordPublisherType.EFO) {
            validateEfoConfiguration(config, streams);
        }

        if (!(config.containsKey(AWSConfigConstants.AWS_REGION)
                || config.containsKey(ConsumerConfigConstants.AWS_ENDPOINT))) {
            // per validation in AwsClientBuilder
            throw new IllegalArgumentException(
                    String.format(
                            "For FlinkKinesisConsumer AWS region ('%s') and/or AWS endpoint ('%s') must be set in the config.",
                            AWSConfigConstants.AWS_REGION, AWSConfigConstants.AWS_ENDPOINT));
        }

        if (config.containsKey(ConsumerConfigConstants.STREAM_INITIAL_POSITION)) {
            String initPosType =
                    config.getProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION);

            // specified initial position in stream must be either LATEST, TRIM_HORIZON or
            // AT_TIMESTAMP
            try {
                InitialPosition.valueOf(initPosType);
            } catch (IllegalArgumentException e) {
                String errorMessage =
                        Arrays.stream(InitialPosition.values())
                                .map(Enum::name)
                                .collect(Collectors.joining(", "));
                throw new IllegalArgumentException(
                        "Invalid initial position in stream set in config. Valid values are: "
                                + errorMessage);
            }

            // specified initial timestamp in stream when using AT_TIMESTAMP
            if (InitialPosition.valueOf(initPosType) == InitialPosition.AT_TIMESTAMP) {
                if (!config.containsKey(STREAM_INITIAL_TIMESTAMP)) {
                    throw new IllegalArgumentException(
                            "Please set value for initial timestamp ('"
                                    + STREAM_INITIAL_TIMESTAMP
                                    + "') when using AT_TIMESTAMP initial position.");
                }
                validateOptionalDateProperty(
                        config,
                        STREAM_INITIAL_TIMESTAMP,
                        config.getProperty(
                                STREAM_TIMESTAMP_DATE_FORMAT, DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT),
                        "Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream. "
                                + "Must be a valid format: yyyy-MM-dd'T'HH:mm:ss.SSSXXX or non-negative double value. For example, 2016-04-04T19:58:46.480-00:00 or 1459799926.480 .");
            }
        }
        validateOptionalPositiveIntProperty(
                config,
                ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
                "Invalid value given for maximum records per getRecords shard operation. Must be a valid non-negative integer value.");

        validateOptionalPositiveIntProperty(
                config,
                ConsumerConfigConstants.SHARD_GETRECORDS_RETRIES,
                "Invalid value given for maximum retry attempts for getRecords shard operation. Must be a valid non-negative integer value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE,
                "Invalid value given for get records operation base backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_MAX,
                "Invalid value given for get records operation max backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveDoubleProperty(
                config,
                ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT,
                "Invalid value given for get records operation backoff exponential constant. Must be a valid non-negative double value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
                "Invalid value given for getRecords sleep interval in milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveIntProperty(
                config,
                ConsumerConfigConstants.SHARD_GETITERATOR_RETRIES,
                "Invalid value given for maximum retry attempts for getShardIterator shard operation. Must be a valid non-negative integer value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_BASE,
                "Invalid value given for get shard iterator operation base backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_MAX,
                "Invalid value given for get shard iterator operation max backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveDoubleProperty(
                config,
                ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT,
                "Invalid value given for get shard iterator operation backoff exponential constant. Must be a valid non-negative double value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS,
                "Invalid value given for shard discovery sleep interval in milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.LIST_SHARDS_BACKOFF_BASE,
                "Invalid value given for list shards operation base backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.LIST_SHARDS_BACKOFF_MAX,
                "Invalid value given for list shards operation max backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveDoubleProperty(
                config,
                ConsumerConfigConstants.LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT,
                "Invalid value given for list shards operation backoff exponential constant. Must be a valid non-negative double value.");

        validateOptionalPositiveIntProperty(
                config,
                ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_RETRIES,
                "Invalid value given for maximum retry attempts for describe stream consumer operation. Must be a valid non-negative int value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_MAX,
                "Invalid value given for describe stream consumer operation max backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveDoubleProperty(
                config,
                ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_EXPONENTIAL_CONSTANT,
                "Invalid value given for describe stream consumer operation backoff exponential constant. Must be a valid non-negative double value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_BACKOFF_BASE,
                "Invalid value given for describe stream consumer operation base backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveIntProperty(
                config,
                ConsumerConfigConstants.REGISTER_STREAM_RETRIES,
                "Invalid value given for maximum retry attempts for register stream operation. Must be a valid non-negative integer value.");

        validateOptionalPositiveIntProperty(
                config,
                ConsumerConfigConstants.REGISTER_STREAM_TIMEOUT_SECONDS,
                "Invalid value given for maximum timeout for register stream consumer. Must be a valid non-negative integer value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_MAX,
                "Invalid value given for register stream operation max backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_BASE,
                "Invalid value given for register stream operation base backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveDoubleProperty(
                config,
                ConsumerConfigConstants.REGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT,
                "Invalid value given for register stream operation backoff exponential constant. Must be a valid non-negative double value.");

        validateOptionalPositiveIntProperty(
                config,
                ConsumerConfigConstants.DEREGISTER_STREAM_RETRIES,
                "Invalid value given for maximum retry attempts for deregister stream operation. Must be a valid non-negative integer value.");

        validateOptionalPositiveIntProperty(
                config,
                ConsumerConfigConstants.DEREGISTER_STREAM_TIMEOUT_SECONDS,
                "Invalid value given for maximum timeout for deregister stream consumer. Must be a valid non-negative integer value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_BASE,
                "Invalid value given for deregister stream operation base backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_MAX,
                "Invalid value given for deregister stream operation max backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveDoubleProperty(
                config,
                ConsumerConfigConstants.DEREGISTER_STREAM_BACKOFF_EXPONENTIAL_CONSTANT,
                "Invalid value given for deregister stream operation backoff exponential constant. Must be a valid non-negative double value.");

        validateOptionalPositiveIntProperty(
                config,
                ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_RETRIES,
                "Invalid value given for maximum retry attempts for subscribe to shard operation. Must be a valid non-negative integer value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_BASE,
                "Invalid value given for subscribe to shard operation base backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveLongProperty(
                config,
                ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_MAX,
                "Invalid value given for subscribe to shard operation max backoff milliseconds. Must be a valid non-negative long value.");

        validateOptionalPositiveDoubleProperty(
                config,
                ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_BACKOFF_EXPONENTIAL_CONSTANT,
                "Invalid value given for subscribe to shard operation backoff exponential constant. Must be a valid non-negative double value.");

        if (config.containsKey(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS)) {
            checkArgument(
                    Long.parseLong(
                                    config.getProperty(
                                            ConsumerConfigConstants
                                                    .SHARD_GETRECORDS_INTERVAL_MILLIS))
                            < ConsumerConfigConstants.MAX_SHARD_GETRECORDS_INTERVAL_MILLIS,
                    "Invalid value given for getRecords sleep interval in milliseconds. Must be lower than "
                            + ConsumerConfigConstants.MAX_SHARD_GETRECORDS_INTERVAL_MILLIS
                            + " milliseconds.");
        }

        validateOptionalPositiveIntProperty(
                config,
                ConsumerConfigConstants.EFO_HTTP_CLIENT_MAX_CONCURRENCY,
                "Invalid value given for EFO HTTP client max concurrency. Must be positive.");
    }

    /**
     * Validate the record publisher type.
     *
     * @param config config properties
     * @return if {@code ConsumerConfigConstants.RECORD_PUBLISHER_TYPE} is set, return the parsed
     *     record publisher type. Else return polling record publisher type.
     */
    public static RecordPublisherType validateRecordPublisherType(Properties config) {
        if (config.containsKey(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE)) {
            String recordPublisherType =
                    config.getProperty(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE);

            // specified record publisher type in stream must be either EFO or POLLING
            try {
                return RecordPublisherType.valueOf(recordPublisherType);
            } catch (IllegalArgumentException e) {
                String errorMessage =
                        Arrays.stream(RecordPublisherType.values())
                                .map(Enum::name)
                                .collect(Collectors.joining(", "));
                throw new IllegalArgumentException(
                        "Invalid record publisher type in stream set in config. Valid values are: "
                                + errorMessage);
            }
        } else {
            return RecordPublisherType.POLLING;
        }
    }

    /**
     * Validate if the given config is a valid EFO configuration.
     *
     * @param config config properties.
     * @param streams the streams which is sent to match the EFO consumer arn if the EFO
     *     registration mode is set to `NONE`.
     */
    public static void validateEfoConfiguration(Properties config, List<String> streams) {
        EFORegistrationType efoRegistrationType;
        if (config.containsKey(ConsumerConfigConstants.EFO_REGISTRATION_TYPE)) {
            String typeInString = config.getProperty(ConsumerConfigConstants.EFO_REGISTRATION_TYPE);
            // specified efo registration type in stream must be either LAZY, EAGER or NONE.
            try {
                efoRegistrationType = EFORegistrationType.valueOf(typeInString);
            } catch (IllegalArgumentException e) {
                String errorMessage =
                        Arrays.stream(EFORegistrationType.values())
                                .map(Enum::name)
                                .collect(Collectors.joining(", "));
                throw new IllegalArgumentException(
                        "Invalid efo registration type in stream set in config. Valid values are: "
                                + errorMessage);
            }
        } else {
            efoRegistrationType = EFORegistrationType.LAZY;
        }
        if (efoRegistrationType == EFORegistrationType.NONE) {
            // if the registration type is NONE, then for each stream there must be an according
            // consumer ARN
            List<String> missingConsumerArnKeys = new ArrayList<>();
            for (String stream : streams) {
                String efoConsumerARNKey =
                        ConsumerConfigConstants.EFO_CONSUMER_ARN_PREFIX + "." + stream;
                if (!config.containsKey(efoConsumerARNKey)) {
                    missingConsumerArnKeys.add(efoConsumerARNKey);
                }
            }
            if (!missingConsumerArnKeys.isEmpty()) {
                String errorMessage =
                        Arrays.stream(missingConsumerArnKeys.toArray())
                                .map(Object::toString)
                                .collect(Collectors.joining(", "));
                throw new IllegalArgumentException(
                        "Invalid efo consumer arn settings for not providing consumer arns: "
                                + errorMessage);
            }
        } else {
            // if the registration type is LAZY or EAGER, then user must provide a self-defined
            // consumer name.
            if (!config.containsKey(ConsumerConfigConstants.EFO_CONSUMER_NAME)) {
                throw new IllegalArgumentException(
                        "No valid enhanced fan-out consumer name is set through "
                                + ConsumerConfigConstants.EFO_CONSUMER_NAME);
            }
        }
    }

    /**
     * Replace deprecated configuration properties for {@link FlinkKinesisProducer}. This should be
     * remove along with deprecated keys
     */
    @SuppressWarnings("deprecation")
    public static Properties replaceDeprecatedProducerKeys(Properties configProps) {
        // Replace deprecated key
        if (configProps.containsKey(ProducerConfigConstants.COLLECTION_MAX_COUNT)) {
            configProps.setProperty(
                    COLLECTION_MAX_COUNT,
                    configProps.getProperty(ProducerConfigConstants.COLLECTION_MAX_COUNT));
            configProps.remove(ProducerConfigConstants.COLLECTION_MAX_COUNT);
        }
        // Replace deprecated key
        if (configProps.containsKey(ProducerConfigConstants.AGGREGATION_MAX_COUNT)) {
            configProps.setProperty(
                    AGGREGATION_MAX_COUNT,
                    configProps.getProperty(ProducerConfigConstants.AGGREGATION_MAX_COUNT));
            configProps.remove(ProducerConfigConstants.AGGREGATION_MAX_COUNT);
        }
        return configProps;
    }

    /**
     * A set of configuration paremeters associated with the describeStreams API may be used if: 1)
     * an legacy client wants to consume from Kinesis 2) a current client wants to consumer from
     * DynamoDB streams
     *
     * <p>In the context of 1), the set of configurations needs to be translated to the
     * corresponding configurations in the Kinesis listShards API. In the mean time, keep these
     * configs since they are applicable in the context of 2), i.e., polling data from a DynamoDB
     * stream.
     *
     * @param configProps original config properties.
     * @return backfilled config properties.
     */
    @SuppressWarnings("UnusedReturnValue")
    public static Properties backfillConsumerKeys(Properties configProps) {
        HashMap<String, String> oldKeyToNewKeys = new HashMap<>();
        oldKeyToNewKeys.put(
                ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_BASE,
                ConsumerConfigConstants.LIST_SHARDS_BACKOFF_BASE);
        oldKeyToNewKeys.put(
                ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_MAX,
                ConsumerConfigConstants.LIST_SHARDS_BACKOFF_MAX);
        oldKeyToNewKeys.put(
                ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT,
                ConsumerConfigConstants.LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT);
        for (Map.Entry<String, String> entry : oldKeyToNewKeys.entrySet()) {
            String oldKey = entry.getKey();
            String newKey = entry.getValue();
            if (configProps.containsKey(oldKey)) {
                configProps.setProperty(newKey, configProps.getProperty(oldKey));
                // Do not remove the oldKey since they may be used in the context of talking to
                // DynamoDB streams
            }
        }
        return configProps;
    }

    /**
     * Validate configuration properties for {@link FlinkKinesisProducer}, and return a constructed
     * KinesisProducerConfiguration.
     */
    public static KinesisProducerConfiguration getValidatedProducerConfiguration(
            Properties config) {
        checkNotNull(config, "config can not be null");

        validateAwsConfiguration(config);

        if (!config.containsKey(AWSConfigConstants.AWS_REGION)) {
            // per requirement in Amazon Kinesis Producer Library
            throw new IllegalArgumentException(
                    String.format(
                            "For FlinkKinesisProducer AWS region ('%s') must be set in the config.",
                            AWSConfigConstants.AWS_REGION));
        }

        KinesisProducerConfiguration kpc = KinesisProducerConfiguration.fromProperties(config);
        kpc.setRegion(config.getProperty(AWSConfigConstants.AWS_REGION));

        kpc.setCredentialsProvider(AWSUtil.getCredentialsProvider(config));

        // we explicitly lower the credential refresh delay (default is 5 seconds)
        // to avoid an ignorable interruption warning that occurs when shutting down the
        // KPL client. See https://github.com/awslabs/amazon-kinesis-producer/issues/10.
        kpc.setCredentialsRefreshDelay(100);

        // Override default values if they aren't specified by users
        if (!config.containsKey(RATE_LIMIT)) {
            kpc.setRateLimit(DEFAULT_RATE_LIMIT);
        }
        if (!config.containsKey(THREADING_MODEL)) {
            kpc.setThreadingModel(DEFAULT_THREADING_MODEL);
        }
        if (!config.containsKey(THREAD_POOL_SIZE)) {
            kpc.setThreadPoolSize(DEFAULT_THREAD_POOL_SIZE);
        }

        return kpc;
    }

    /** Validate configuration properties related to Amazon AWS service. */
    public static void validateAwsConfiguration(Properties config) {
        if (config.containsKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER)) {
            String credentialsProviderType =
                    config.getProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER);

            // value specified for AWSConfigConstants.AWS_CREDENTIALS_PROVIDER needs to be
            // recognizable
            CredentialProvider providerType;
            try {
                providerType = CredentialProvider.valueOf(credentialsProviderType);
            } catch (IllegalArgumentException e) {
                StringBuilder sb = new StringBuilder();
                for (CredentialProvider type : CredentialProvider.values()) {
                    sb.append(type.toString()).append(", ");
                }
                throw new IllegalArgumentException(
                        "Invalid AWS Credential Provider Type set in config. Valid values are: "
                                + sb.toString());
            }

            // if BASIC type is used, also check that the Access Key ID and Secret Key is supplied
            if (providerType == CredentialProvider.BASIC) {
                if (!config.containsKey(AWSConfigConstants.AWS_ACCESS_KEY_ID)
                        || !config.containsKey(AWSConfigConstants.AWS_SECRET_ACCESS_KEY)) {
                    throw new IllegalArgumentException(
                            "Please set values for AWS Access Key ID ('"
                                    + AWSConfigConstants.AWS_ACCESS_KEY_ID
                                    + "') "
                                    + "and Secret Key ('"
                                    + AWSConfigConstants.AWS_SECRET_ACCESS_KEY
                                    + "') when using the BASIC AWS credential provider type.");
                }
            }
        }

        if (config.containsKey(AWSConfigConstants.AWS_REGION)) {
            // specified AWS Region name must be recognizable
            if (!AWSUtil.isValidRegion(config.getProperty(AWSConfigConstants.AWS_REGION))) {
                StringBuilder sb = new StringBuilder();
                for (Regions region : Regions.values()) {
                    sb.append(region.getName()).append(", ");
                }
                throw new IllegalArgumentException(
                        "Invalid AWS region set in config. Valid values are: " + sb.toString());
            }
        }
    }

    /**
     * Parses the timestamp in which to start consuming from the stream, from the given properties.
     *
     * @param consumerConfig the properties to parse timestamp from
     * @return the timestamp
     */
    public static Date parseStreamTimestampStartingPosition(final Properties consumerConfig) {
        String timestamp = consumerConfig.getProperty(STREAM_INITIAL_TIMESTAMP);

        try {
            String format =
                    consumerConfig.getProperty(
                            STREAM_TIMESTAMP_DATE_FORMAT, DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT);
            SimpleDateFormat customDateFormat = new SimpleDateFormat(format);
            return customDateFormat.parse(timestamp);
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw new IllegalArgumentException(exception);
        } catch (ParseException exception) {
            return new Date((long) (Double.parseDouble(timestamp) * 1000));
        }
    }

    private static void validateOptionalPositiveLongProperty(
            Properties config, String key, String message) {
        if (config.containsKey(key)) {
            try {
                long value = Long.parseLong(config.getProperty(key));
                if (value < 0) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(message);
            }
        }
    }

    private static void validateOptionalPositiveIntProperty(
            Properties config, String key, String message) {
        if (config.containsKey(key)) {
            try {
                int value = Integer.parseInt(config.getProperty(key));
                if (value < 0) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(message);
            }
        }
    }

    private static void validateOptionalPositiveDoubleProperty(
            Properties config, String key, String message) {
        if (config.containsKey(key)) {
            try {
                double value = Double.parseDouble(config.getProperty(key));
                if (value < 0) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(message);
            }
        }
    }

    private static void validateOptionalDateProperty(
            Properties config, String timestampKey, String format, String message) {
        if (config.containsKey(timestampKey)) {
            try {
                SimpleDateFormat customDateFormat = new SimpleDateFormat(format);
                customDateFormat.parse(config.getProperty(timestampKey));
            } catch (IllegalArgumentException | NullPointerException exception) {
                throw new IllegalArgumentException(message);
            } catch (ParseException exception) {
                try {
                    double value = Double.parseDouble(config.getProperty(timestampKey));
                    if (value < 0) {
                        throw new IllegalArgumentException(message);
                    }
                } catch (NumberFormatException numberFormatException) {
                    throw new IllegalArgumentException(message);
                }
            }
        }
    }
}
