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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.testutils.ReferenceKinesisShardTopologies;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestableFlinkKinesisConsumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.HashMap;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * Suite of FlinkKinesisConsumer tests, including utility static method tests,
 * and tests for the methods called throughout the source life cycle with mocked KinesisProxy.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FlinkKinesisConsumer.class)
public class FlinkKinesisConsumerTest {

	@Rule
	private ExpectedException exception = ExpectedException.none();

	// ----------------------------------------------------------------------
	// FlinkKinesisConsumer.validatePropertiesConfig() tests
	// ----------------------------------------------------------------------

	@Test
	public void testMissingAwsRegionInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("AWS region must be set");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKey");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");

		FlinkKinesisConsumer.validatePropertiesConfig(testConfig);
	}

	@Test
	public void testUnrecognizableAwsRegionInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid AWS region");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "wrongRegionId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKeyId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");

		FlinkKinesisConsumer.validatePropertiesConfig(testConfig);
	}

	@Test
	public void testCredentialProviderTypeDefaultToBasicButNoCredentialsSetInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Need to set values for AWS Access Key ID and Secret Key");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");

		FlinkKinesisConsumer.validatePropertiesConfig(testConfig);
	}

	@Test
	public void testCredentialProviderTypeSetToBasicButNoCredentialSetInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Need to set values for AWS Access Key ID and Secret Key");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE, "BASIC");

		FlinkKinesisConsumer.validatePropertiesConfig(testConfig);
	}

	@Test
	public void testUnrecognizableCredentialProviderTypeInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid AWS Credential Provider Type");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE, "wrongProviderType");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKeyId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");

		FlinkKinesisConsumer.validatePropertiesConfig(testConfig);
	}

	@Test
	public void testUnrecognizableStreamInitPositionTypeInConfig() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid initial position in stream");

		Properties testConfig = new Properties();
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_TYPE, "BASIC");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKeyId");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");
		testConfig.setProperty(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, "wrongInitPosition");

		FlinkKinesisConsumer.validatePropertiesConfig(testConfig);
	}

	// ----------------------------------------------------------------------
	// FlinkKinesisConsumer.assignShards() tests
	// ----------------------------------------------------------------------

	@Test
	public void testShardNumEqualConsumerNum() {
		try {
			List<KinesisStreamShard> fakeShards = ReferenceKinesisShardTopologies.flatTopologyWithFourOpenShards();
			int consumerTaskCount = fakeShards.size();

			for (int consumerNum=0; consumerNum < consumerTaskCount; consumerNum++) {
				List<KinesisStreamShard> assignedShardsToThisConsumerTask =
					FlinkKinesisConsumer.assignShards(fakeShards, consumerTaskCount, consumerNum);

				// the ith consumer should be assigned exactly 1 shard,
				// which is always the ith shard of a shard list that only has open shards
				assertEquals(1, assignedShardsToThisConsumerTask.size());
				assertTrue(assignedShardsToThisConsumerTask.get(0).equals(fakeShards.get(consumerNum)));
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testShardNumFewerThanConsumerNum() {
		try {
			List<KinesisStreamShard> fakeShards = ReferenceKinesisShardTopologies.flatTopologyWithFourOpenShards();
			int consumerTaskCount = fakeShards.size() + 3;

			for (int consumerNum = 0; consumerNum < consumerTaskCount; consumerNum++) {
				List<KinesisStreamShard> assignedShardsToThisConsumerTask =
					FlinkKinesisConsumer.assignShards(fakeShards, consumerTaskCount, consumerNum);

				// for ith consumer with i < the total num of shards,
				// the ith consumer should be assigned exactly 1 shard,
				// which is always the ith shard of a shard list that only has open shards;
				// otherwise, the consumer should not be assigned any shards
				if (consumerNum < fakeShards.size()) {
					assertEquals(1, assignedShardsToThisConsumerTask.size());
					assertTrue(assignedShardsToThisConsumerTask.get(0).equals(fakeShards.get(consumerNum)));
				} else {
					assertEquals(0, assignedShardsToThisConsumerTask.size());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testShardNumMoreThanConsumerNum() {
		try {
			List<KinesisStreamShard> fakeShards = ReferenceKinesisShardTopologies.flatTopologyWithFourOpenShards();
			int consumerTaskCount = fakeShards.size() - 1;

			for (int consumerNum = 0; consumerNum < consumerTaskCount; consumerNum++) {
				List<KinesisStreamShard> assignedShardsToThisConsumerTask =
					FlinkKinesisConsumer.assignShards(fakeShards, consumerTaskCount, consumerNum);

				// since the number of consumer tasks is short by 1,
				// all but the first consumer task should be assigned 1 shard,
				// while the first consumer task is assigned 2 shards
				if (consumerNum != 0) {
					assertEquals(1, assignedShardsToThisConsumerTask.size());
					assertTrue(assignedShardsToThisConsumerTask.get(0).equals(fakeShards.get(consumerNum)));
				} else {
					assertEquals(2, assignedShardsToThisConsumerTask.size());
					assertTrue(assignedShardsToThisConsumerTask.get(0).equals(fakeShards.get(0)));
					assertTrue(assignedShardsToThisConsumerTask.get(1).equals(fakeShards.get(fakeShards.size()-1)));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testAssignEmptyShards() {
		try {
			List<KinesisStreamShard> fakeShards = new ArrayList<>(0);
			int consumerTaskCount = 4;

			for (int consumerNum = 0; consumerNum < consumerTaskCount; consumerNum++) {
				List<KinesisStreamShard> assignedShardsToThisConsumerTask =
					FlinkKinesisConsumer.assignShards(fakeShards, consumerTaskCount, consumerNum);

				// should not be assigned anything
				assertEquals(0, assignedShardsToThisConsumerTask.size());

			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ----------------------------------------------------------------------
	// Constructor tests with mocked KinesisProxy
	// ----------------------------------------------------------------------

	@Test
	public void testConstructorShouldThrowRuntimeExceptionIfUnableToFindAnyShards() {
		exception.expect(RuntimeException.class);
		exception.expectMessage("Unable to retrieve any shards");

		Properties testConsumerConfig = new Properties();
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKey");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");

		// get a consumer that will not be able to find any shards from AWS Kinesis
		FlinkKinesisConsumer dummyConsumer = getDummyConsumerWithMockedKinesisProxy(
			6, 2, "fake-consumer-task-name",
			new ArrayList<KinesisStreamShard>(), new ArrayList<KinesisStreamShard>(), testConsumerConfig,
			null, null, false, false);
	}

	// ----------------------------------------------------------------------
	// Tests for open() source life cycle method
	// ----------------------------------------------------------------------

	@Test
	public void testOpenWithNoRestoreStateFetcherAdvanceToLatestSentinelSequenceNumberWhenConfigSetToStartFromLatest() throws Exception {

		int fakeNumConsumerTasks = 6;
		int fakeThisConsumerTaskIndex = 2;
		String fakeThisConsumerTaskName = "fake-this-task-name";

		List<KinesisStreamShard> fakeCompleteShardList = ReferenceKinesisShardTopologies.flatTopologyWithFourOpenShards();
		List<KinesisStreamShard> fakeAssignedShardsToThisConsumerTask = fakeCompleteShardList.subList(2,3);

		Properties testConsumerConfig = new Properties();
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKey");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, "LATEST");

		KinesisDataFetcher kinesisDataFetcherMock = Mockito.mock(KinesisDataFetcher.class);
		try {
			whenNew(KinesisDataFetcher.class).withArguments(fakeAssignedShardsToThisConsumerTask, testConsumerConfig, fakeThisConsumerTaskName).thenReturn(kinesisDataFetcherMock);
		} catch (Exception e) {
			throw new RuntimeException("Error when power mocking KinesisDataFetcher in test", e);
		}

		FlinkKinesisConsumer dummyConsumer = getDummyConsumerWithMockedKinesisProxy(
			fakeNumConsumerTasks, fakeThisConsumerTaskIndex, fakeThisConsumerTaskName,
			fakeCompleteShardList, fakeAssignedShardsToThisConsumerTask, testConsumerConfig,
			null, null, false, false);

		dummyConsumer.open(new Configuration());

		for (KinesisStreamShard shard : fakeAssignedShardsToThisConsumerTask) {
			verify(kinesisDataFetcherMock).advanceSequenceNumberTo(shard, SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM.toString());
		}

	}

	@Test
	public void testOpenWithNoRestoreStateFetcherAdvanceToEarliestSentinelSequenceNumberWhenConfigSetToTrimHorizon() throws Exception {

		int fakeNumConsumerTasks = 6;
		int fakeThisConsumerTaskIndex = 2;
		String fakeThisConsumerTaskName = "fake-this-task-name";

		List<KinesisStreamShard> fakeCompleteShardList = ReferenceKinesisShardTopologies.flatTopologyWithFourOpenShards();
		List<KinesisStreamShard> fakeAssignedShardsToThisConsumerTask = fakeCompleteShardList.subList(2,3);

		Properties testConsumerConfig = new Properties();
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKey");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, "TRIM_HORIZON");

		KinesisDataFetcher kinesisDataFetcherMock = Mockito.mock(KinesisDataFetcher.class);
		try {
			whenNew(KinesisDataFetcher.class).withArguments(fakeAssignedShardsToThisConsumerTask, testConsumerConfig, fakeThisConsumerTaskName).thenReturn(kinesisDataFetcherMock);
		} catch (Exception e) {
			throw new RuntimeException("Error when power mocking KinesisDataFetcher in test", e);
		}

		FlinkKinesisConsumer dummyConsumer = getDummyConsumerWithMockedKinesisProxy(
			fakeNumConsumerTasks, fakeThisConsumerTaskIndex, fakeThisConsumerTaskName,
			fakeCompleteShardList, fakeAssignedShardsToThisConsumerTask, testConsumerConfig,
			null, null, false, false);

		dummyConsumer.open(new Configuration());

		for (KinesisStreamShard shard : fakeAssignedShardsToThisConsumerTask) {
			verify(kinesisDataFetcherMock).advanceSequenceNumberTo(shard, SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.toString());
		}

	}

	@Test
	public void testOpenWithRestoreStateFetcherAdvanceToCorrespondingSequenceNumbers() throws Exception {

		int fakeNumConsumerTasks = 6;
		int fakeThisConsumerTaskIndex = 2;
		String fakeThisConsumerTaskName = "fake-this-task-name";

		List<KinesisStreamShard> fakeCompleteShardList = ReferenceKinesisShardTopologies.flatTopologyWithFourOpenShards();
		List<KinesisStreamShard> fakeAssignedShardsToThisConsumerTask = fakeCompleteShardList.subList(2,3);

		Properties testConsumerConfig = new Properties();
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, "us-east-1");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, "accessKey");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, "secretKey");
		testConsumerConfig.setProperty(KinesisConfigConstants.CONFIG_STREAM_INIT_POSITION_TYPE, "TRIM_HORIZON");

		KinesisDataFetcher kinesisDataFetcherMock = Mockito.mock(KinesisDataFetcher.class);
		try {
			whenNew(KinesisDataFetcher.class).withArguments(fakeAssignedShardsToThisConsumerTask, testConsumerConfig, fakeThisConsumerTaskName).thenReturn(kinesisDataFetcherMock);
		} catch (Exception e) {
			throw new RuntimeException("Error when power mocking KinesisDataFetcher in test", e);
		}

		FlinkKinesisConsumer dummyConsumer = getDummyConsumerWithMockedKinesisProxy(
			fakeNumConsumerTasks, fakeThisConsumerTaskIndex, fakeThisConsumerTaskName,
			fakeCompleteShardList, fakeAssignedShardsToThisConsumerTask, testConsumerConfig,
			null, null, false, false);

		// generate random UUIDs as sequence numbers of last checkpointed state for each assigned shard
		ArrayList<String> listOfSeqNumIfAssignedShards = new ArrayList<>(fakeAssignedShardsToThisConsumerTask.size());
		for (KinesisStreamShard shard : fakeAssignedShardsToThisConsumerTask) {
			listOfSeqNumIfAssignedShards.add(UUID.randomUUID().toString());
		}

		HashMap<KinesisStreamShard, String> fakeRestoredState = new HashMap<>();
		for (int i=0; i<fakeAssignedShardsToThisConsumerTask.size(); i++) {
			fakeRestoredState.put(fakeAssignedShardsToThisConsumerTask.get(i), listOfSeqNumIfAssignedShards.get(i));
		}

		dummyConsumer.restoreState(fakeRestoredState);
		dummyConsumer.open(new Configuration());

		for (int i=0; i<fakeAssignedShardsToThisConsumerTask.size(); i++) {
			verify(kinesisDataFetcherMock).advanceSequenceNumberTo(
				fakeAssignedShardsToThisConsumerTask.get(i),
				listOfSeqNumIfAssignedShards.get(i));
		}
	}

	private TestableFlinkKinesisConsumer getDummyConsumerWithMockedKinesisProxy(
		int fakeNumFlinkConsumerTasks,
		int fakeThisConsumerTaskIndex,
		String fakeThisConsumerTaskName,
		List<KinesisStreamShard> fakeCompleteShardList,
		List<KinesisStreamShard> fakeAssignedShardListToThisConsumerTask,
		Properties consumerTestConfig,
		KinesisDataFetcher fetcher,
		HashMap<KinesisStreamShard, String> lastSequenceNumsToRestore,
		boolean hasAssignedShards,
		boolean running) {

		final String dummyKinesisStreamName = "flink-test";

		final List<String> dummyKinesisStreamList = Collections.singletonList(dummyKinesisStreamName);

		final KinesisProxy kinesisProxyMock = mock(KinesisProxy.class);

		// mock KinesisProxy that is instantiated in the constructor, as well as its getShardList call
		try {
			whenNew(KinesisProxy.class).withArguments(consumerTestConfig).thenReturn(kinesisProxyMock);
		} catch (Exception e) {
			throw new RuntimeException("Error when power mocking KinesisProxy in tests", e);
		}

		when(kinesisProxyMock.getShardList(dummyKinesisStreamList)).thenReturn(fakeCompleteShardList);

		TestableFlinkKinesisConsumer dummyConsumer =
			new TestableFlinkKinesisConsumer(dummyKinesisStreamName, fakeNumFlinkConsumerTasks,
				fakeThisConsumerTaskIndex, fakeThisConsumerTaskName, consumerTestConfig);

		try {
			Field fetcherField = FlinkKinesisConsumer.class.getDeclaredField("fetcher");
			fetcherField.setAccessible(true);
			fetcherField.set(dummyConsumer, fetcher);

			Field lastSequenceNumsField = FlinkKinesisConsumer.class.getDeclaredField("lastSequenceNums");
			lastSequenceNumsField.setAccessible(true);
			lastSequenceNumsField.set(dummyConsumer, lastSequenceNumsToRestore);

			Field hasAssignedShardsField = FlinkKinesisConsumer.class.getDeclaredField("hasAssignedShards");
			hasAssignedShardsField.setAccessible(true);
			hasAssignedShardsField.set(dummyConsumer, hasAssignedShards);

			Field runningField = FlinkKinesisConsumer.class.getDeclaredField("running");
			runningField.setAccessible(true);
			runningField.set(dummyConsumer, running);
		} catch (IllegalAccessException | NoSuchFieldException e) {
			// no reason to end up here ...
			throw new RuntimeException(e);
		}

		// mock FlinkKinesisConsumer utility static methods
		mockStatic(FlinkKinesisConsumer.class);

		try {
			// assume assignShards static method is correct by mocking
			PowerMockito.when(
				FlinkKinesisConsumer.assignShards(
					fakeCompleteShardList,
					fakeNumFlinkConsumerTasks,
					fakeThisConsumerTaskIndex))
				.thenReturn(fakeAssignedShardListToThisConsumerTask);

			// assume validatePropertiesConfig static method is correct by mocking
			PowerMockito.doNothing().when(FlinkKinesisConsumer.class, "validatePropertiesConfig", Mockito.any(Properties.class));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error when power mocking static methods of FlinkKinesisConsumer", e);
		}

		return dummyConsumer;
	}
}
