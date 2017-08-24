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

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ProducerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestableFlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Suite of FlinkKinesisConsumer tests for the methods called throughout the source life cycle.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FlinkKinesisConsumer.class, KinesisConfigUtil.class})
public class FlinkKinesisConsumerTest {

	@Rule
	private ExpectedException exception = ExpectedException.none();

	// ----------------------------------------------------------------------
	// FlinkKinesisConsumer.validateAwsConfiguration() tests
	// ----------------------------------------------------------------------

	@Test
	public void testMissingAwsRegionInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("The AWS region ('" + AWSConfigConstants.AWS_REGION + "') must be set in the config.");

		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
		testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		KinesisConfigUtil.validateAwsConfiguration(testConfig);
	}

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

		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "wrongProviderType");
		testConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		KinesisConfigUtil.validateAwsConfiguration(testConfig);
	}

	// ----------------------------------------------------------------------
	// FlinkKinesisConsumer.validateConsumerConfiguration() tests
	// ----------------------------------------------------------------------

	@Test
	public void testUnrecognizableStreamInitPositionTypeInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid initial position in stream");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "wrongInitPosition");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testStreamInitPositionTypeSetToAtTimestampButNoInitTimestampSetInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Please set value for initial timestamp ('"
			+ ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP + "') when using AT_TIMESTAMP initial position.");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableDateForInitialTimestampInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "unparsableDate");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testIllegalValueForInitialTimestampInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "-1.0");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testDateStringForValidateOptionDateProperty() {
		String timestamp = "2016-04-04T19:58:46.480-00:00";

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
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

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
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

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "2016-03-14");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT, "InvalidPattern");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableDateForUserDefinedDateFormatForInitialTimestampInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream.");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, "stillUnparsable");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT, "yyyy-MM-dd");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testDateStringForUserDefinedDateFormatForValidateOptionDateProperty() {
		String unixTimestamp = "2016-04-04";
		String pattern = "yyyy-MM-dd";

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, unixTimestamp);
		testConfig.setProperty(ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT, pattern);

		try {
			KinesisConfigUtil.validateConsumerConfiguration(testConfig);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void testUnparsableLongForDescribeStreamBackoffBaseMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for describe stream operation base backoff milliseconds");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_BASE, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForDescribeStreamBackoffMaxMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for describe stream operation max backoff milliseconds");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_MAX, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableDoubleForDescribeStreamBackoffExponentialConstantInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for describe stream operation backoff exponential constant");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT, "unparsableDouble");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableIntForGetRecordsRetriesInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for maximum retry attempts for getRecords shard operation");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_RETRIES, "unparsableInt");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableIntForGetRecordsMaxCountInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for maximum records per getRecords shard operation");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX, "unparsableInt");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForGetRecordsBackoffBaseMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for get records operation base backoff milliseconds");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForGetRecordsBackoffMaxMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for get records operation max backoff milliseconds");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_MAX, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableDoubleForGetRecordsBackoffExponentialConstantInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for get records operation backoff exponential constant");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT, "unparsableDouble");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForGetRecordsIntervalMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for getRecords sleep interval in milliseconds");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableIntForGetShardIteratorRetriesInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for maximum retry attempts for getShardIterator shard operation");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETITERATOR_RETRIES, "unparsableInt");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForGetShardIteratorBackoffBaseMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for get shard iterator operation base backoff milliseconds");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_BASE, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForGetShardIteratorBackoffMaxMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for get shard iterator operation max backoff milliseconds");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_MAX, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableDoubleForGetShardIteratorBackoffExponentialConstantInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for get shard iterator operation backoff exponential constant");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT, "unparsableDouble");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForShardDiscoveryIntervalMillisInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for shard discovery sleep interval in milliseconds");

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ConsumerConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS, "unparsableLong");

		KinesisConfigUtil.validateConsumerConfiguration(testConfig);
	}

	// ----------------------------------------------------------------------
	// FlinkKinesisConsumer.validateProducerConfiguration() tests
	// ----------------------------------------------------------------------

	@Test
	public void testUnparsableLongForCollectionMaxCountInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for maximum number of items to pack into a PutRecords request");

		Properties testConfig = new Properties();
		testConfig.setProperty(ProducerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ProducerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ProducerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ProducerConfigConstants.COLLECTION_MAX_COUNT, "unparsableLong");

		KinesisConfigUtil.validateProducerConfiguration(testConfig);
	}

	@Test
	public void testUnparsableLongForAggregationMaxCountInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid value given for maximum number of items to pack into an aggregated record");

		Properties testConfig = new Properties();
		testConfig.setProperty(ProducerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ProducerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ProducerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		testConfig.setProperty(ProducerConfigConstants.AGGREGATION_MAX_COUNT, "unparsableLong");

		KinesisConfigUtil.validateProducerConfiguration(testConfig);
	}

	// ----------------------------------------------------------------------
	// Tests related to state initialization
	// ----------------------------------------------------------------------

	@Test
	public void testUseRestoredStateForSnapshotIfFetcherNotInitialized() throws Exception {
		Properties config = new Properties();
		config.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		config.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		config.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		List<Tuple2<StreamShardMetadata, SequenceNumber>> globalUnionState = new ArrayList<>(4);
		globalUnionState.add(Tuple2.of(
			KinesisDataFetcher.convertToStreamShardMetadata(new StreamShardHandle("fakeStream",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0)))),
			new SequenceNumber("1")));
		globalUnionState.add(Tuple2.of(
			KinesisDataFetcher.convertToStreamShardMetadata(new StreamShardHandle("fakeStream",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1)))),
			new SequenceNumber("1")));
		globalUnionState.add(Tuple2.of(
			KinesisDataFetcher.convertToStreamShardMetadata(new StreamShardHandle("fakeStream",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2)))),
			new SequenceNumber("1")));
		globalUnionState.add(Tuple2.of(
			KinesisDataFetcher.convertToStreamShardMetadata(new StreamShardHandle("fakeStream",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(3)))),
			new SequenceNumber("1")));

		TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState = new TestingListState<>();
		for (Tuple2<StreamShardMetadata, SequenceNumber> state : globalUnionState) {
			listState.add(state);
		}

		FlinkKinesisConsumer<String> consumer = new FlinkKinesisConsumer<>("fakeStream", new SimpleStringSchema(), config);
		RuntimeContext context = mock(RuntimeContext.class);
		when(context.getIndexOfThisSubtask()).thenReturn(0);
		when(context.getNumberOfParallelSubtasks()).thenReturn(2);
		consumer.setRuntimeContext(context);

		OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
		when(operatorStateStore.getUnionListState(Matchers.any(ListStateDescriptor.class))).thenReturn(listState);

		StateInitializationContext initializationContext = mock(StateInitializationContext.class);
		when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
		when(initializationContext.isRestored()).thenReturn(true);

		consumer.initializeState(initializationContext);

		// only opened, not run
		consumer.open(new Configuration());

		// arbitrary checkpoint id and timestamp
		consumer.snapshotState(new StateSnapshotContextSynchronousImpl(123, 123));

		Assert.assertTrue(listState.isClearCalled());

		// the checkpointed list state should contain only the shards that it should subscribe to
		Assert.assertEquals(globalUnionState.size() / 2, listState.getList().size());
		Assert.assertTrue(listState.getList().contains(globalUnionState.get(0)));
		Assert.assertTrue(listState.getList().contains(globalUnionState.get(2)));
	}

	@Test
	public void testListStateChangedAfterSnapshotState() throws Exception {

		// ----------------------------------------------------------------------
		// setup config, initial state and expected state snapshot
		// ----------------------------------------------------------------------
		Properties config = new Properties();
		config.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		config.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		config.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		ArrayList<Tuple2<StreamShardMetadata, SequenceNumber>> initialState = new ArrayList<>(1);
		initialState.add(Tuple2.of(
			KinesisDataFetcher.convertToStreamShardMetadata(new StreamShardHandle("fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0)))),
			new SequenceNumber("1")));

		ArrayList<Tuple2<StreamShardMetadata, SequenceNumber>> expectedStateSnapshot = new ArrayList<>(3);
		expectedStateSnapshot.add(Tuple2.of(
			KinesisDataFetcher.convertToStreamShardMetadata(new StreamShardHandle("fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0)))),
			new SequenceNumber("12")));
		expectedStateSnapshot.add(Tuple2.of(
			KinesisDataFetcher.convertToStreamShardMetadata(new StreamShardHandle("fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1)))),
			new SequenceNumber("11")));
		expectedStateSnapshot.add(Tuple2.of(
			KinesisDataFetcher.convertToStreamShardMetadata(new StreamShardHandle("fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2)))),
			new SequenceNumber("31")));

		// ----------------------------------------------------------------------
		// mock operator state backend and initial state for initializeState()
		// ----------------------------------------------------------------------

		TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState = new TestingListState<>();
		for (Tuple2<StreamShardMetadata, SequenceNumber> state : initialState) {
			listState.add(state);
		}

		OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
		when(operatorStateStore.getUnionListState(Matchers.any(ListStateDescriptor.class))).thenReturn(listState);

		StateInitializationContext initializationContext = mock(StateInitializationContext.class);
		when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
		when(initializationContext.isRestored()).thenReturn(true);

		// ----------------------------------------------------------------------
		// mock a running fetcher and its state for snapshot
		// ----------------------------------------------------------------------

		HashMap<StreamShardMetadata, SequenceNumber> stateSnapshot = new HashMap<>();
		for (Tuple2<StreamShardMetadata, SequenceNumber> tuple : expectedStateSnapshot) {
			stateSnapshot.put(tuple.f0, tuple.f1);
		}

		KinesisDataFetcher mockedFetcher = mock(KinesisDataFetcher.class);
		when(mockedFetcher.snapshotState()).thenReturn(stateSnapshot);

		// ----------------------------------------------------------------------
		// create a consumer and test the snapshotState()
		// ----------------------------------------------------------------------

		FlinkKinesisConsumer<String> consumer = new FlinkKinesisConsumer<>("fakeStream", new SimpleStringSchema(), config);
		FlinkKinesisConsumer<?> mockedConsumer = spy(consumer);

		RuntimeContext context = mock(RuntimeContext.class);
		when(context.getIndexOfThisSubtask()).thenReturn(1);

		mockedConsumer.setRuntimeContext(context);
		mockedConsumer.initializeState(initializationContext);
		mockedConsumer.open(new Configuration());
		Whitebox.setInternalState(mockedConsumer, "fetcher", mockedFetcher); // mock consumer as running.

		mockedConsumer.snapshotState(mock(FunctionSnapshotContext.class));

		assertEquals(true, listState.clearCalled);
		assertEquals(3, listState.getList().size());

		for (Tuple2<StreamShardMetadata, SequenceNumber> state : initialState) {
			for (Tuple2<StreamShardMetadata, SequenceNumber> currentState : listState.getList()) {
				assertNotEquals(state, currentState);
			}
		}

		for (Tuple2<StreamShardMetadata, SequenceNumber> state : expectedStateSnapshot) {
			boolean hasOneIsSame = false;
			for (Tuple2<StreamShardMetadata, SequenceNumber> currentState : listState.getList()) {
				hasOneIsSame = hasOneIsSame || state.equals(currentState);
			}
			assertEquals(true, hasOneIsSame);
		}
	}

	// ----------------------------------------------------------------------
	// Tests related to fetcher initialization
	// ----------------------------------------------------------------------

	@Test
	@SuppressWarnings("unchecked")
	public void testFetcherShouldNotBeRestoringFromFailureIfNotRestoringFromCheckpoint() throws Exception {
		KinesisDataFetcher mockedFetcher = Mockito.mock(KinesisDataFetcher.class);
		PowerMockito.whenNew(KinesisDataFetcher.class).withAnyArguments().thenReturn(mockedFetcher);

		// assume the given config is correct
		PowerMockito.mockStatic(KinesisConfigUtil.class);
		PowerMockito.doNothing().when(KinesisConfigUtil.class);

		TestableFlinkKinesisConsumer consumer = new TestableFlinkKinesisConsumer(
			"fakeStream", new Properties(), 10, 2);
		consumer.open(new Configuration());
		consumer.run(Mockito.mock(SourceFunction.SourceContext.class));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFetcherShouldBeCorrectlySeededIfRestoringFromCheckpoint() throws Exception {

		// ----------------------------------------------------------------------
		// setup initial state
		// ----------------------------------------------------------------------

		HashMap<StreamShardHandle, SequenceNumber> fakeRestoredState = getFakeRestoredStore("all");

		// ----------------------------------------------------------------------
		// mock operator state backend and initial state for initializeState()
		// ----------------------------------------------------------------------

		TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState = new TestingListState<>();
		for (Map.Entry<StreamShardHandle, SequenceNumber> state : fakeRestoredState.entrySet()) {
			listState.add(Tuple2.of(KinesisDataFetcher.convertToStreamShardMetadata(state.getKey()), state.getValue()));
		}

		OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
		when(operatorStateStore.getUnionListState(Matchers.any(ListStateDescriptor.class))).thenReturn(listState);

		StateInitializationContext initializationContext = mock(StateInitializationContext.class);
		when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
		when(initializationContext.isRestored()).thenReturn(true);

		// ----------------------------------------------------------------------
		// mock fetcher
		// ----------------------------------------------------------------------

		KinesisDataFetcher mockedFetcher = Mockito.mock(KinesisDataFetcher.class);
		List<StreamShardHandle> shards = new ArrayList<>();
		shards.addAll(fakeRestoredState.keySet());
		when(mockedFetcher.discoverNewShardsToSubscribe()).thenReturn(shards);
		PowerMockito.whenNew(KinesisDataFetcher.class).withAnyArguments().thenReturn(mockedFetcher);

		// assume the given config is correct
		PowerMockito.mockStatic(KinesisConfigUtil.class);
		PowerMockito.doNothing().when(KinesisConfigUtil.class);

		// ----------------------------------------------------------------------
		// start to test fetcher's initial state seeding
		// ----------------------------------------------------------------------

		TestableFlinkKinesisConsumer consumer = new TestableFlinkKinesisConsumer(
			"fakeStream", new Properties(), 10, 2);
		consumer.initializeState(initializationContext);
		consumer.open(new Configuration());
		consumer.run(Mockito.mock(SourceFunction.SourceContext.class));

		for (Map.Entry<StreamShardHandle, SequenceNumber> restoredShard : fakeRestoredState.entrySet()) {
			Mockito.verify(mockedFetcher).registerNewSubscribedShardState(
				new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(restoredShard.getKey()),
					restoredShard.getKey(), restoredShard.getValue()));
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testFetcherShouldBeCorrectlySeededOnlyItsOwnStates() throws Exception {

		// ----------------------------------------------------------------------
		// setup initial state
		// ----------------------------------------------------------------------

		HashMap<StreamShardHandle, SequenceNumber> fakeRestoredState = getFakeRestoredStore("fakeStream1");

		HashMap<StreamShardHandle, SequenceNumber> fakeRestoredStateForOthers = getFakeRestoredStore("fakeStream2");

		// ----------------------------------------------------------------------
		// mock operator state backend and initial state for initializeState()
		// ----------------------------------------------------------------------

		TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState = new TestingListState<>();
		for (Map.Entry<StreamShardHandle, SequenceNumber> state : fakeRestoredState.entrySet()) {
			listState.add(Tuple2.of(KinesisDataFetcher.convertToStreamShardMetadata(state.getKey()), state.getValue()));
		}
		for (Map.Entry<StreamShardHandle, SequenceNumber> state : fakeRestoredStateForOthers.entrySet()) {
			listState.add(Tuple2.of(KinesisDataFetcher.convertToStreamShardMetadata(state.getKey()), state.getValue()));
		}

		OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
		when(operatorStateStore.getUnionListState(Matchers.any(ListStateDescriptor.class))).thenReturn(listState);

		StateInitializationContext initializationContext = mock(StateInitializationContext.class);
		when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
		when(initializationContext.isRestored()).thenReturn(true);

		// ----------------------------------------------------------------------
		// mock fetcher
		// ----------------------------------------------------------------------

		KinesisDataFetcher mockedFetcher = Mockito.mock(KinesisDataFetcher.class);
		List<StreamShardHandle> shards = new ArrayList<>();
		shards.addAll(fakeRestoredState.keySet());
		when(mockedFetcher.discoverNewShardsToSubscribe()).thenReturn(shards);
		PowerMockito.whenNew(KinesisDataFetcher.class).withAnyArguments().thenReturn(mockedFetcher);

		// assume the given config is correct
		PowerMockito.mockStatic(KinesisConfigUtil.class);
		PowerMockito.doNothing().when(KinesisConfigUtil.class);

		// ----------------------------------------------------------------------
		// start to test fetcher's initial state seeding
		// ----------------------------------------------------------------------

		TestableFlinkKinesisConsumer consumer = new TestableFlinkKinesisConsumer(
			"fakeStream", new Properties(), 10, 2);
		consumer.initializeState(initializationContext);
		consumer.open(new Configuration());
		consumer.run(Mockito.mock(SourceFunction.SourceContext.class));

		for (Map.Entry<StreamShardHandle, SequenceNumber> restoredShard : fakeRestoredStateForOthers.entrySet()) {
			// should never get restored state not belonging to itself
			Mockito.verify(mockedFetcher, never()).registerNewSubscribedShardState(
				new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(restoredShard.getKey()),
					restoredShard.getKey(), restoredShard.getValue()));
		}
		for (Map.Entry<StreamShardHandle, SequenceNumber> restoredShard : fakeRestoredState.entrySet()) {
			// should get restored state belonging to itself
			Mockito.verify(mockedFetcher).registerNewSubscribedShardState(
				new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(restoredShard.getKey()),
					restoredShard.getKey(), restoredShard.getValue()));
		}
	}

	/*
	 * This tests that the consumer correctly picks up shards that were not discovered on the previous run.
	 *
	 * Case under test:
	 *
	 * If the original parallelism is 2 and states are:
	 *   Consumer subtask 1:
	 *     stream1, shard1, SequentialNumber(xxx)
	 *   Consumer subtask 2:
	 *     stream1, shard2, SequentialNumber(yyy)
	 *
	 * After discoverNewShardsToSubscribe() if there were two shards (shard3, shard4) created:
	 *   Consumer subtask 1 (late for discoverNewShardsToSubscribe()):
	 *     stream1, shard1, SequentialNumber(xxx)
	 *   Consumer subtask 2:
	 *     stream1, shard2, SequentialNumber(yyy)
	 *     stream1, shard4, SequentialNumber(zzz)
	 *
	 * If snapshotState() occurs and parallelism is changed to 1:
	 *   Union state will be:
	 *     stream1, shard1, SequentialNumber(xxx)
	 *     stream1, shard2, SequentialNumber(yyy)
	 *     stream1, shard4, SequentialNumber(zzz)
	 *   Fetcher should be seeded with:
	 *     stream1, shard1, SequentialNumber(xxx)
	 *     stream1, shard2, SequentialNumber(yyy)
	 *     stream1, share3, SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM
	 *     stream1, shard4, SequentialNumber(zzz)
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testFetcherShouldBeCorrectlySeededWithNewDiscoveredKinesisStreamShard() throws Exception {

		// ----------------------------------------------------------------------
		// setup initial state
		// ----------------------------------------------------------------------

		HashMap<StreamShardHandle, SequenceNumber> fakeRestoredState = getFakeRestoredStore("all");

		// ----------------------------------------------------------------------
		// mock operator state backend and initial state for initializeState()
		// ----------------------------------------------------------------------

		TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState = new TestingListState<>();
		for (Map.Entry<StreamShardHandle, SequenceNumber> state : fakeRestoredState.entrySet()) {
			listState.add(Tuple2.of(KinesisDataFetcher.convertToStreamShardMetadata(state.getKey()), state.getValue()));
		}

		OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
		when(operatorStateStore.getUnionListState(Matchers.any(ListStateDescriptor.class))).thenReturn(listState);

		StateInitializationContext initializationContext = mock(StateInitializationContext.class);
		when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
		when(initializationContext.isRestored()).thenReturn(true);

		// ----------------------------------------------------------------------
		// mock fetcher
		// ----------------------------------------------------------------------

		KinesisDataFetcher mockedFetcher = Mockito.mock(KinesisDataFetcher.class);
		List<StreamShardHandle> shards = new ArrayList<>();
		shards.addAll(fakeRestoredState.keySet());
		shards.add(new StreamShardHandle("fakeStream2",
			new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))));
		when(mockedFetcher.discoverNewShardsToSubscribe()).thenReturn(shards);
		PowerMockito.whenNew(KinesisDataFetcher.class).withAnyArguments().thenReturn(mockedFetcher);

		// assume the given config is correct
		PowerMockito.mockStatic(KinesisConfigUtil.class);
		PowerMockito.doNothing().when(KinesisConfigUtil.class);

		// ----------------------------------------------------------------------
		// start to test fetcher's initial state seeding
		// ----------------------------------------------------------------------

		TestableFlinkKinesisConsumer consumer = new TestableFlinkKinesisConsumer(
			"fakeStream", new Properties(), 10, 2);
		consumer.initializeState(initializationContext);
		consumer.open(new Configuration());
		consumer.run(Mockito.mock(SourceFunction.SourceContext.class));

		fakeRestoredState.put(new StreamShardHandle("fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
			SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get());
		for (Map.Entry<StreamShardHandle, SequenceNumber> restoredShard : fakeRestoredState.entrySet()) {
			Mockito.verify(mockedFetcher).registerNewSubscribedShardState(
				new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(restoredShard.getKey()),
					restoredShard.getKey(), restoredShard.getValue()));
		}
	}

	@Test
	public void testLegacyKinesisStreamShardToStreamShardMetadataConversion() {
		String streamName = "fakeStream1";
		String shardId = "shard-000001";
		String parentShardId = "shard-000002";
		String adjacentParentShardId = "shard-000003";
		String startingHashKey = "key-000001";
		String endingHashKey = "key-000010";
		String startingSequenceNumber = "seq-0000021";
		String endingSequenceNumber = "seq-00000031";

		StreamShardMetadata streamShardMetadata = new StreamShardMetadata();
		streamShardMetadata.setStreamName(streamName);
		streamShardMetadata.setShardId(shardId);
		streamShardMetadata.setParentShardId(parentShardId);
		streamShardMetadata.setAdjacentParentShardId(adjacentParentShardId);
		streamShardMetadata.setStartingHashKey(startingHashKey);
		streamShardMetadata.setEndingHashKey(endingHashKey);
		streamShardMetadata.setStartingSequenceNumber(startingSequenceNumber);
		streamShardMetadata.setEndingSequenceNumber(endingSequenceNumber);

		Shard shard = new Shard()
			.withShardId(shardId)
			.withParentShardId(parentShardId)
			.withAdjacentParentShardId(adjacentParentShardId)
			.withHashKeyRange(new HashKeyRange()
				.withStartingHashKey(startingHashKey)
				.withEndingHashKey(endingHashKey))
			.withSequenceNumberRange(new SequenceNumberRange()
				.withStartingSequenceNumber(startingSequenceNumber)
				.withEndingSequenceNumber(endingSequenceNumber));
		KinesisStreamShard kinesisStreamShard = new KinesisStreamShard(streamName, shard);

		assertEquals(streamShardMetadata, KinesisStreamShard.convertToStreamShardMetadata(kinesisStreamShard));
	}

	@Test
	public void testStreamShardMetadataSerializedUsingPojoSerializer() {
		TypeInformation<StreamShardMetadata> typeInformation = TypeInformation.of(StreamShardMetadata.class);
		assertTrue(typeInformation.createSerializer(new ExecutionConfig()) instanceof PojoSerializer);
	}

	private static final class TestingListState<T> implements ListState<T> {

		private final List<T> list = new ArrayList<>();
		private boolean clearCalled = false;

		@Override
		public void clear() {
			list.clear();
			clearCalled = true;
		}

		@Override
		public Iterable<T> get() throws Exception {
			return list;
		}

		@Override
		public void add(T value) throws Exception {
			list.add(value);
		}

		public List<T> getList() {
			return list;
		}

		public boolean isClearCalled() {
			return clearCalled;
		}
	}

	private HashMap<StreamShardHandle, SequenceNumber> getFakeRestoredStore(String streamName) {
		HashMap<StreamShardHandle, SequenceNumber> fakeRestoredState = new HashMap<>();

		if (streamName.equals("fakeStream1") || streamName.equals("all")) {
			fakeRestoredState.put(
				new StreamShardHandle("fakeStream1",
					new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
				new SequenceNumber(UUID.randomUUID().toString()));
			fakeRestoredState.put(
				new StreamShardHandle("fakeStream1",
					new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
				new SequenceNumber(UUID.randomUUID().toString()));
			fakeRestoredState.put(
				new StreamShardHandle("fakeStream1",
					new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
				new SequenceNumber(UUID.randomUUID().toString()));
		}

		if (streamName.equals("fakeStream2") || streamName.equals("all")) {
			fakeRestoredState.put(
				new StreamShardHandle("fakeStream2",
					new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
				new SequenceNumber(UUID.randomUUID().toString()));
			fakeRestoredState.put(
				new StreamShardHandle("fakeStream2",
					new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
				new SequenceNumber(UUID.randomUUID().toString()));
		}

		return fakeRestoredState;
	}
}
