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

package org.apache.flink.streaming.connectors.kinesis.internals;

import com.amazonaws.services.kinesis.model.*;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.HashMap;
import java.util.UUID;
import java.util.LinkedList;

import static org.junit.Assert.assertEquals;

/**
 * Tests on how the ShardConsumerThread behaves with mocked KinesisProxy behaviours.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ShardConsumerThread.class)
public class ShardConsumerThreadTest {

	@Test
	public void testAllRecordsFetchedFromKinesisAreCorrectlyCollected() {
		int totalRecordCount = 500;

		KinesisStreamShard assignedShardUnderTest = new KinesisStreamShard(
			"fake-stream-name",
			new Shard()
				.withShardId("fake-shard-id")
				.withAdjacentParentShardId(null)
				.withParentShardId(null)
				.withHashKeyRange(new HashKeyRange().withStartingHashKey("0").withEndingHashKey(StringUtils.repeat("FF", 16))));

		// ------------------------------------------------------------------------------------------
		// the part below prepares the behaviour of the mocked KinesisProxy for getting the inital shard iterator,
		// followed by consecutive getRecords() calls until total of 500 records fetched
		// ------------------------------------------------------------------------------------------

		KinesisProxy kinesisProxyMock = Mockito.mock(KinesisProxy.class);
		Mockito.when(kinesisProxyMock.getShardIterator(Matchers.any(KinesisStreamShard.class), Matchers.anyString(), Matchers.anyString()))
			.thenReturn("fake-initial-shard-itr");

		// 1st getRecords() returns 100 records
		GetRecordsResult getRecordsResultFirst = new GetRecordsResult()
			.withRecords(generateFakeListOfRecordsFromToIncluding(0, 99))
			.withNextShardIterator("fake-1st-shard-itr");

		// 2nd getRecords() returns 90 records
		GetRecordsResult getRecordsResultSecond = new GetRecordsResult()
			.withRecords(generateFakeListOfRecordsFromToIncluding(100, 189))
			.withNextShardIterator("fake-2nd-shard-itr");

		// 3rd getRecords() returns 78 records
		GetRecordsResult getRecordsResultThird = new GetRecordsResult()
			.withRecords(generateFakeListOfRecordsFromToIncluding(190, 267))
			.withNextShardIterator("fake-3rd-shard-itr");

		// 4th getRecords() returns 100 records
		GetRecordsResult getRecordsResultFourth = new GetRecordsResult()
			.withRecords(generateFakeListOfRecordsFromToIncluding(268, 367))
			.withNextShardIterator("fake-4th-shard-itr");

		GetRecordsResult getRecordsResultFifth = new GetRecordsResult()
			.withRecords(generateFakeListOfRecordsFromToIncluding(368, 459))
			.withNextShardIterator("fake-5th-shard-itr");

		GetRecordsResult getRecordsResultFinal = new GetRecordsResult()
			.withRecords(generateFakeListOfRecordsFromToIncluding(460, 499))
			.withNextShardIterator(null);

		Mockito.when(kinesisProxyMock.getRecords(Matchers.anyString(), Matchers.anyInt()))
			.thenReturn(getRecordsResultFirst)
			.thenReturn(getRecordsResultSecond)
			.thenReturn(getRecordsResultThird)
			.thenReturn(getRecordsResultFourth)
			.thenReturn(getRecordsResultFifth)
			.thenReturn(getRecordsResultFinal);

		// assuming that all fetched records are not aggregated,
		// so we are mocking the static deaggregateRecords() to return the original list of records
		PowerMockito.mockStatic(ShardConsumerThread.class);
		PowerMockito.when(ShardConsumerThread.deaggregateRecords(Matchers.anyListOf(Record.class), Matchers.anyString(), Matchers.anyString()))
			.thenReturn(getRecordsResultFirst.getRecords())
			.thenReturn(getRecordsResultSecond.getRecords())
			.thenReturn(getRecordsResultThird.getRecords())
			.thenReturn(getRecordsResultFourth.getRecords())
			.thenReturn(getRecordsResultFifth.getRecords())
			.thenReturn(getRecordsResultFinal.getRecords());

		// ------------------------------------------------------------------------------------------

		Properties testConsumerConfig = new Properties();
		HashMap<KinesisStreamShard, String> seqNumState = new HashMap<>();

		DummySourceContext dummySourceContext = new DummySourceContext();
		ShardConsumerThread dummyShardConsumerThread = getDummyShardConsumerThreadWithMockedKinesisProxy(
			dummySourceContext, kinesisProxyMock, Mockito.mock(KinesisDataFetcher.class),
			testConsumerConfig, assignedShardUnderTest, "fake-last-seq-num", seqNumState);

		dummyShardConsumerThread.run();

		// the final sequence number state for the assigned shard to this consumer thread
		// should store SENTINEL_SHARD_ENDING_SEQUENCE_NUMBER since the final nextShardItr should be null
		assertEquals(seqNumState.get(assignedShardUnderTest), SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.toString());

		// the number of elements collected should equal the number of records generated by mocked KinesisProxy
		assertEquals(dummySourceContext.getNumOfElementsCollected(), totalRecordCount);
	}

	private ShardConsumerThread getDummyShardConsumerThreadWithMockedKinesisProxy(
		SourceFunction.SourceContext<String> dummySourceContext,
		KinesisProxy kinesisProxyMock,
		KinesisDataFetcher owningFetcherRefMock,
		Properties testConsumerConfig,
		KinesisStreamShard assignedShard,
		String lastSequenceNum,
		HashMap<KinesisStreamShard, String> seqNumState) {

		try {
			PowerMockito.whenNew(KinesisProxy.class).withArguments(testConsumerConfig).thenReturn(kinesisProxyMock);
		} catch (Exception e) {
			throw new RuntimeException("Error when power mocking KinesisProxy in test", e);
		}

		return new ShardConsumerThread<>(owningFetcherRefMock, testConsumerConfig,
			assignedShard, lastSequenceNum, dummySourceContext, new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()), seqNumState);
	}

	private List<Record> generateFakeListOfRecordsFromToIncluding(int startingSeq, int endingSeq) {
		List<Record> fakeListOfRecords = new LinkedList<>();
		for (int i=0; i <= (endingSeq - startingSeq); i++) {
			fakeListOfRecords.add(new Record()
				.withData(ByteBuffer.wrap(String.valueOf(i).getBytes()))
				.withPartitionKey(UUID.randomUUID().toString()) // the partition key assigned doesn't matter here
				.withSequenceNumber(String.valueOf(i))); // assign the order of the record within the whole sequence as the sequence num
		}
		return fakeListOfRecords;
	}

	private static class DummySourceContext implements SourceFunction.SourceContext<String> {
		private static final Object lock = new Object();

		private static long numElementsCollected;

		public DummySourceContext() {
			numElementsCollected = 0;
		}

		@Override
		public void collect(String element) {
			numElementsCollected++;
		}

		@Override
		public void collectWithTimestamp(java.lang.String element, long timestamp) {
		}

		@Override
		public void emitWatermark(Watermark mark) {
		}

		@Override
		public Object getCheckpointLock() {
			return lock;
		}

		@Override
		public void close() {
		}

		public long getNumOfElementsCollected() {
			return numElementsCollected;
		}
	}

}
