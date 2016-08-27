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

import com.amazonaws.regions.Regions;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.InitialPosition;
import org.apache.flink.streaming.connectors.kinesis.config.ProducerConfigConstants;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utilities for Flink Kinesis connector configuration.
 */
public class KinesisConfigUtil {

	/**
	 * Validate configuration properties for {@link FlinkKinesisConsumer}.
	 */
	public static void validateConsumerConfiguration(Properties config) {
		checkNotNull(config, "config can not be null");

		validateAwsConfiguration(config);

		if (config.containsKey(ConsumerConfigConstants.STREAM_INITIAL_POSITION)) {
			String initPosType = config.getProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION);

			// specified initial position in stream must be either LATEST or TRIM_HORIZON
			try {
				InitialPosition.valueOf(initPosType);
			} catch (IllegalArgumentException e) {
				StringBuilder sb = new StringBuilder();
				for (InitialPosition pos : InitialPosition.values()) {
					sb.append(pos.toString()).append(", ");
				}
				throw new IllegalArgumentException("Invalid initial position in stream set in config. Valid values are: " + sb.toString());
			}
		}

		validateOptionalPositiveIntProperty(config, ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
			"Invalid value given for maximum records per getRecords shard operation. Must be a valid non-negative integer value.");

		validateOptionalPositiveIntProperty(config, ConsumerConfigConstants.SHARD_GETRECORDS_RETRIES,
			"Invalid value given for maximum retry attempts for getRecords shard operation. Must be a valid non-negative integer value.");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE,
			"Invalid value given for get records operation base backoff milliseconds. Must be a valid non-negative long value");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_MAX,
			"Invalid value given for get records operation max backoff milliseconds. Must be a valid non-negative long value");

		validateOptionalPositiveDoubleProperty(config, ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT,
			"Invalid value given for get records operation backoff exponential constant. Must be a valid non-negative double value");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
			"Invalid value given for getRecords sleep interval in milliseconds. Must be a valid non-negative long value.");

		validateOptionalPositiveIntProperty(config, ConsumerConfigConstants.SHARD_GETITERATOR_RETRIES,
			"Invalid value given for maximum retry attempts for getShardIterator shard operation. Must be a valid non-negative integer value.");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_BASE,
			"Invalid value given for get shard iterator operation base backoff milliseconds. Must be a valid non-negative long value");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_MAX,
			"Invalid value given for get shard iterator operation max backoff milliseconds. Must be a valid non-negative long value");

		validateOptionalPositiveDoubleProperty(config, ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT,
			"Invalid value given for get shard iterator operation backoff exponential constant. Must be a valid non-negative double value");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS,
			"Invalid value given for shard discovery sleep interval in milliseconds. Must be a valid non-negative long value");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_BASE,
			"Invalid value given for describe stream operation base backoff milliseconds. Must be a valid non-negative long value");

		validateOptionalPositiveLongProperty(config, ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_MAX,
			"Invalid value given for describe stream operation max backoff milliseconds. Must be a valid non-negative long value");

		validateOptionalPositiveDoubleProperty(config, ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT,
			"Invalid value given for describe stream operation backoff exponential constant. Must be a valid non-negative double value");
	}

	/**
	 * Validate configuration properties for {@link FlinkKinesisProducer}.
	 */
	public static void validateProducerConfiguration(Properties config) {
		checkNotNull(config, "config can not be null");

		validateAwsConfiguration(config);

		validateOptionalPositiveLongProperty(config, ProducerConfigConstants.COLLECTION_MAX_COUNT,
			"Invalid value given for maximum number of items to pack into a PutRecords request. Must be a valid non-negative long value.");

		validateOptionalPositiveLongProperty(config, ProducerConfigConstants.AGGREGATION_MAX_COUNT,
			"Invalid value given for maximum number of items to pack into an aggregated record. Must be a valid non-negative long value.");
	}

	/**
	 * Validate configuration properties related to Amazon AWS service
	 */
	public static void validateAwsConfiguration(Properties config) {
		if (!config.containsKey(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER)) {
			// if the credential provider type is not specified, it will default to BASIC later on,
			// so the Access Key ID and Secret Key must be given
			if (!config.containsKey(AWSConfigConstants.AWS_ACCESS_KEY_ID)
				|| !config.containsKey(AWSConfigConstants.AWS_SECRET_ACCESS_KEY)) {
				throw new IllegalArgumentException("Please set values for AWS Access Key ID ('"+ AWSConfigConstants.AWS_ACCESS_KEY_ID +"') " +
					"and Secret Key ('" + AWSConfigConstants.AWS_SECRET_ACCESS_KEY + "') when using the BASIC AWS credential provider type.");
			}
		} else {
			String credentialsProviderType = config.getProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER);

			// value specified for AWSConfigConstants.AWS_CREDENTIALS_PROVIDER needs to be recognizable
			CredentialProvider providerType;
			try {
				providerType = CredentialProvider.valueOf(credentialsProviderType);
			} catch (IllegalArgumentException e) {
				StringBuilder sb = new StringBuilder();
				for (CredentialProvider type : CredentialProvider.values()) {
					sb.append(type.toString()).append(", ");
				}
				throw new IllegalArgumentException("Invalid AWS Credential Provider Type set in config. Valid values are: " + sb.toString());
			}

			// if BASIC type is used, also check that the Access Key ID and Secret Key is supplied
			if (providerType == CredentialProvider.BASIC) {
				if (!config.containsKey(AWSConfigConstants.AWS_ACCESS_KEY_ID)
					|| !config.containsKey(AWSConfigConstants.AWS_SECRET_ACCESS_KEY)) {
					throw new IllegalArgumentException("Please set values for AWS Access Key ID ('"+ AWSConfigConstants.AWS_ACCESS_KEY_ID +"') " +
						"and Secret Key ('" + AWSConfigConstants.AWS_SECRET_ACCESS_KEY + "') when using the BASIC AWS credential provider type.");
				}
			}
		}

		if (!config.containsKey(AWSConfigConstants.AWS_REGION)) {
			throw new IllegalArgumentException("The AWS region ('" + AWSConfigConstants.AWS_REGION + "') must be set in the config.");
		} else {
			// specified AWS Region name must be recognizable
			if (!AWSUtil.isValidRegion(config.getProperty(AWSConfigConstants.AWS_REGION))) {
				StringBuilder sb = new StringBuilder();
				for (Regions region : Regions.values()) {
					sb.append(region.getName()).append(", ");
				}
				throw new IllegalArgumentException("Invalid AWS region set in config. Valid values are: " + sb.toString());
			}
		}
	}

	private static void validateOptionalPositiveLongProperty(Properties config, String key, String message) {
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

	private static void validateOptionalPositiveIntProperty(Properties config, String key, String message) {
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

	private static void validateOptionalPositiveDoubleProperty(Properties config, String key, String message) {
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
}
