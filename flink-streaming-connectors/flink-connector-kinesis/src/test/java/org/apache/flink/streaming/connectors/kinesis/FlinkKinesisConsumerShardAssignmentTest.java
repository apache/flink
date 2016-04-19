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

import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.util.ReferenceKinesisShardTopologies;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class FlinkKinesisConsumerShardAssignmentTest {

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

}
