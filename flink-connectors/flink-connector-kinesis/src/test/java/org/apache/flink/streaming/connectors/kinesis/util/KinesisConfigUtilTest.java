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

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ProducerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;

import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for KinesisConfigUtil.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(KinesisConfigUtil.class)
public class KinesisConfigUtilTest {

	@Rule
	private ExpectedException exception = ExpectedException.none();

	// ----------------------------------------------------------------------
	// getValidatedProducerConfiguration() tests
	// ----------------------------------------------------------------------

	@Test
	public void testUnparsableLongForProducerConfiguration() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Error trying to set field RateLimit with the value 'unparsableLong'");

		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty("RateLimit", "unparsableLong");

		KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);
	}

	@Test
	public void testRateLimitInProducerConfiguration() {
		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		KinesisProducerConfiguration kpc = KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

		assertEquals(100, kpc.getRateLimit());

		testConfig.setProperty(KinesisConfigUtil.RATE_LIMIT, "150");
		kpc = KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

		assertEquals(150, kpc.getRateLimit());
	}

	@Test
	public void testThreadingModelInProducerConfiguration() {
		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		KinesisProducerConfiguration kpc = KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

		assertEquals(KinesisProducerConfiguration.ThreadingModel.POOLED, kpc.getThreadingModel());

		testConfig.setProperty(KinesisConfigUtil.THREADING_MODEL, "PER_REQUEST");
		kpc = KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

		assertEquals(KinesisProducerConfiguration.ThreadingModel.PER_REQUEST, kpc.getThreadingModel());
	}

	@Test
	public void testThreadPoolSizeInProducerConfiguration() {
		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		KinesisProducerConfiguration kpc = KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

		assertEquals(10, kpc.getThreadPoolSize());

		testConfig.setProperty(KinesisConfigUtil.THREAD_POOL_SIZE, "12");
		kpc = KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

		assertEquals(12, kpc.getThreadPoolSize());
	}

	@Test
	public void testReplaceDeprecatedKeys() {
		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		// these deprecated keys should be replaced
		testConfig.setProperty(ProducerConfigConstants.AGGREGATION_MAX_COUNT, "1");
		testConfig.setProperty(ProducerConfigConstants.COLLECTION_MAX_COUNT, "2");
		Properties replacedConfig = KinesisConfigUtil.replaceDeprecatedProducerKeys(testConfig);

		assertEquals("1", replacedConfig.getProperty(KinesisConfigUtil.AGGREGATION_MAX_COUNT));
		assertEquals("2", replacedConfig.getProperty(KinesisConfigUtil.COLLECTION_MAX_COUNT));
	}

	@Test
	public void testCorrectlySetRegionInProducerConfiguration() {
		String region = "us-east-1";
		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, region);
		KinesisProducerConfiguration kpc = KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);

		assertEquals("incorrect region", region, kpc.getRegion());
	}

	@Test
	public void testMissingAwsRegionInProducerConfig() {
		String expectedMessage = String.format("For FlinkKinesisProducer AWS region ('%s') must be set in the config.",
				AWSConfigConstants.AWS_REGION);
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage(expectedMessage);

		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
		testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		KinesisConfigUtil.getValidatedProducerConfiguration(testConfig);
	}

	// ----------------------------------------------------------------------
	// validateAwsConfiguration() tests
	// ----------------------------------------------------------------------

	@Test
	public void testUnrecognizableAwsRegionInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid AWS region");

		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "wrongRegionId");
		testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		KinesisConfigUtil.validateAwsConfiguration(testConfig);
	}

	@Test
	public void testCredentialProviderTypeSetToBasicButNoCredentialSetInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Please set values for AWS Access Key ID ('" + AWSConfigConstants.AWS_ACCESS_KEY_ID + "') " +
				"and Secret Key ('" + AWSConfigConstants.AWS_SECRET_ACCESS_KEY + "') when using the BASIC AWS credential provider type.");

		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");

		KinesisConfigUtil.validateAwsConfiguration(testConfig);
	}

	@Test
	public void testUnrecognizableCredentialProviderTypeInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid AWS Credential Provider Type");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "wrongProviderType");

		KinesisConfigUtil.validateAwsConfiguration(testConfig);
	}

	// ----------------------------------------------------------------------
	// validateConsumerConfiguration() tests
	// ----------------------------------------------------------------------

	@Test
	public void testNoAwsRegionOrEndpointInConsumerConfig() {
		String expectedMessage = String.format("For FlinkKinesisConsumer AWS region ('%s') and/or AWS endpoint ('%s') must be set in the config.",
				AWSConfigConstants.AWS_REGION, AWSConfigConstants.AWS_ENDPOINT);
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage(expectedMessage);

		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
		testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testAwsRegionAndEndpointInConsumerConfig() {
		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(AWSConfigConstants.AWS_ENDPOINT, "fake");
		testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
		testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testAwsRegionInConsumerConfig() {
		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
		testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testEndpointInConsumerConfig() {
		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_ENDPOINT, "fake");
		testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
		testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnrecognizableStreamInitPositionTypeInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid initial position in stream");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "wrongInitPosition");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testStreamInitPositionTypeSetToAtTimestampButNoInitTimestampSetInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Please set value for initial timestamp ('"
				+ ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP + "') when using AT_TIMESTAMP initial position.");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableDateForInitialTimestampInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "unparsableDate");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testIllegalValueForInitialTimestampInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "-1.0");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testDateStringForValidateOptionDateProperty() {
		String timestamp = "2016-04-04T19:58:46.480-00:00";

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, timestamp);

		try {
			KinesisConfigUtil.validateConsumerConfiguration(testConfig);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void testUnixTimestampForValidateOptionDateProperty() {
		String unixTimestamp = "1459799926.480";

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, unixTimestamp);

		try {
			KinesisConfigUtil.validateConsumerConfiguration(testConfig);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void testInvalidPatternForInitialTimestampInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "2016-03-14");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT, "InvalidPattern");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableDateForUserDefinedDateFormatForInitialTimestampInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "stillUnparsable");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT, "yyyy-MM-dd");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testDateStringForUserDefinedDateFormatForValidateOptionDateProperty() {
		String unixTimestamp = "2016-04-04";
		String pattern = "yyyy-MM-dd";

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, unixTimestamp);
		testConfig.setProperty(ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT, pattern);

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForListShardsBackoffBaseMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for list shards operation base backoff milliseconds");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.LIST_SHARDS_BACKOFF_BASE, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForListShardsBackoffMaxMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for list shards operation max backoff milliseconds");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.LIST_SHARDS_BACKOFF_MAX, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableDoubleForListShardsBackoffExponentialConstantInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for list shards operation backoff exponential constant");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT, "unparsableDouble");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableIntForGetRecordsRetriesInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for maximum retry attempts for getRecords shard operation");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_RETRIES, "unparsableInt");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableIntForGetRecordsMaxCountInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for maximum records per getRecords shard operation");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX, "unparsableInt");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForGetRecordsBackoffBaseMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for get records operation base backoff milliseconds");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForGetRecordsBackoffMaxMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for get records operation max backoff milliseconds");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_MAX, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableDoubleForGetRecordsBackoffExponentialConstantInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for get records operation backoff exponential constant");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT, "unparsableDouble");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForGetRecordsIntervalMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for getRecords sleep interval in milliseconds");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableIntForGetShardIteratorRetriesInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for maximum retry attempts for getShardIterator shard operation");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETITERATOR_RETRIES, "unparsableInt");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForGetShardIteratorBackoffBaseMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for get shard iterator operation base backoff milliseconds");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_BASE, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForGetShardIteratorBackoffMaxMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for get shard iterator operation max backoff milliseconds");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_MAX, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableDoubleForGetShardIteratorBackoffExponentialConstantInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for get shard iterator operation backoff exponential constant");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT, "unparsableDouble");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForShardDiscoveryIntervalMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for shard discovery sleep interval in milliseconds");

		Properties testConfig = TestUtils.getStandardProperties();
		testConfig.setProperty(ConsumerConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testParseStreamTimestampStartingPositionUsingDefaultFormat() throws Exception {
		String timestamp = "2020-08-13T09:18:00.0+01:00";
		Date expectedTimestamp = new SimpleDateFormat(DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT).parse(timestamp);

		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);

		Date actualimestamp = KinesisConfigUtil.parseStreamTimestampStartingPosition(consumerProperties);

		assertEquals(expectedTimestamp, actualimestamp);
	}

	@Test
	public void testParseStreamTimestampStartingPositionUsingCustomFormat() throws Exception {
		String format = "yyyy-MM-dd'T'HH:mm";
		String timestamp = "2020-08-13T09:23";
		Date expectedTimestamp = new SimpleDateFormat(format).parse(timestamp);

		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);
		consumerProperties.setProperty(STREAM_TIMESTAMP_DATE_FORMAT, format);

		Date actualimestamp = KinesisConfigUtil.parseStreamTimestampStartingPosition(consumerProperties);

		assertEquals(expectedTimestamp, actualimestamp);
	}

	@Test
	public void testParseStreamTimestampStartingPositionUsingParseError() {
		exception.expect(NumberFormatException.class);

		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, "bad");

		KinesisConfigUtil.parseStreamTimestampStartingPosition(consumerProperties);
	}

	@Test
	public void testParseStreamTimestampStartingPositionIllegalArgumentException() {
		exception.expect(IllegalArgumentException.class);

		Properties consumerProperties = new Properties();

		KinesisConfigUtil.parseStreamTimestampStartingPosition(consumerProperties);
	}

}
