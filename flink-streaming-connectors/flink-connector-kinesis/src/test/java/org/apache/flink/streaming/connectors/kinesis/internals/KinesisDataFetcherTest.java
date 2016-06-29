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

import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.testutils.ReferenceKinesisShardTopologies;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Properties;

public class KinesisDataFetcherTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testAdvanceSequenceNumberOnNotOwnedShard() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Can't advance sequence number on a shard we are not going to read.");

		List<KinesisStreamShard> fakeCompleteListOfShards = ReferenceKinesisShardTopologies.flatTopologyWithFourOpenShards();
		List<KinesisStreamShard> assignedShardsToThisFetcher = fakeCompleteListOfShards.subList(0,1);

		KinesisDataFetcher fetcherUnderTest = new KinesisDataFetcher(assignedShardsToThisFetcher, new Properties(), "fake-task-name");

		// advance the fetcher on a shard that it does not own
		fetcherUnderTest.advanceSequenceNumberTo(fakeCompleteListOfShards.get(2), new SequenceNumber("fake-seq-num"));
	}

}
