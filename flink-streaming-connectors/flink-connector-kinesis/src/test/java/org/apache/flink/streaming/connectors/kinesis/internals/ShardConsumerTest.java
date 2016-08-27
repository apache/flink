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

import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestableKinesisDataFetcher;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;

public class ShardConsumerTest {

	@Test
	public void testCorrectNumOfCollectedRecordsAndUpdatedState() {
		KinesisStreamShard fakeToBeConsumedShard = new KinesisStreamShard(
			"fakeStream",
			new Shard()
				.withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))
				.withHashKeyRange(
					new HashKeyRange()
						.withStartingHashKey("0")
						.withEndingHashKey(new BigInteger(StringUtils.repeat("FF", 16), 16).toString())));

		LinkedList<KinesisStreamShardState> subscribedShardsStateUnderTest = new LinkedList<>();
		subscribedShardsStateUnderTest.add(
			new KinesisStreamShardState(fakeToBeConsumedShard, new SequenceNumber("fakeStartingState")));

		TestableKinesisDataFetcher fetcher =
			new TestableKinesisDataFetcher(
				Collections.singletonList("fakeStream"),
				new Properties(),
				10,
				2,
				new AtomicReference<Throwable>(),
				subscribedShardsStateUnderTest,
				KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(Collections.singletonList("fakeStream")),
				Mockito.mock(KinesisProxyInterface.class));

		new ShardConsumer<>(
			fetcher,
			0,
			subscribedShardsStateUnderTest.get(0).getKinesisStreamShard(),
			subscribedShardsStateUnderTest.get(0).getLastProcessedSequenceNum(),
			FakeKinesisBehavioursFactory.totalNumOfRecordsAfterNumOfGetRecordsCalls(1000, 9)).run();

		assertTrue(fetcher.getNumOfElementsCollected() == 1000);
		assertTrue(subscribedShardsStateUnderTest.get(0).getLastProcessedSequenceNum().equals(
			SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get()));
	}

}
