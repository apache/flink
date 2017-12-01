/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link FlinkFixedPartitioner}.
 */
public class FlinkFixedPartitionerTest {

	/**
	 * Test for when there are more sinks than partitions.
	 * <pre>
	 *   		Flink Sinks:		Kafka Partitions
	 * 			1	---------------->	1
	 * 			2   --------------/
	 * 			3   -------------/
	 * 			4	------------/
	 * </pre>
	 */
	@Test
	public void testMoreFlinkThanBrokers() {
		FlinkFixedPartitioner<String> part = new FlinkFixedPartitioner<>();

		int[] partitions = new int[]{0};

		part.open(0, 4);
		Assert.assertEquals(0, part.partition("abc1", null, null, null, partitions));

		part.open(1, 4);
		Assert.assertEquals(0, part.partition("abc2", null, null, null, partitions));

		part.open(2, 4);
		Assert.assertEquals(0, part.partition("abc3", null, null, null, partitions));
		Assert.assertEquals(0, part.partition("abc3", null, null, null, partitions)); // check if it is changing ;)

		part.open(3, 4);
		Assert.assertEquals(0, part.partition("abc4", null, null, null, partitions));
	}

	/**
	 * Tests for when there are more partitions than sinks.
	 * <pre>
	 * 		Flink Sinks:		Kafka Partitions
	 * 			1	---------------->	1
	 * 			2	---------------->	2
	 * 									3
	 * 									4
	 * 									5
	 *
	 * </pre>
	 */
	@Test
	public void testFewerPartitions() {
		FlinkFixedPartitioner<String> part = new FlinkFixedPartitioner<>();

		int[] partitions = new int[]{0, 1, 2, 3, 4};
		part.open(0, 2);
		Assert.assertEquals(0, part.partition("abc1", null, null, null, partitions));
		Assert.assertEquals(0, part.partition("abc1", null, null, null, partitions));

		part.open(1, 2);
		Assert.assertEquals(1, part.partition("abc1", null, null, null, partitions));
		Assert.assertEquals(1, part.partition("abc1", null, null, null, partitions));
	}

	/*
	 * 		Flink Sinks:		Kafka Partitions
	 * 			1	------------>--->	1
	 * 			2	-----------/----> 	2
	 * 			3	----------/
	 */
	@Test
	public void testMixedCase() {
		FlinkFixedPartitioner<String> part = new FlinkFixedPartitioner<>();
		int[] partitions = new int[]{0, 1};

		part.open(0, 3);
		Assert.assertEquals(0, part.partition("abc1", null, null, null, partitions));

		part.open(1, 3);
		Assert.assertEquals(1, part.partition("abc1", null, null, null, partitions));

		part.open(2, 3);
		Assert.assertEquals(0, part.partition("abc1", null, null, null, partitions));
	}

	/**
	 * Tests that the determined target partition is independently calculated for different topics.
	 */
	@Test
	public void testDifferentTopics() {
		final String topic1 = "topic-1";
		final String topic2 = "topic-2";

		final int[] partitionsTopic1 = new int[]{0, 1, 2};
		final int[] partitionsTopic2 = new int[]{0, 1};

		final FlinkFixedPartitioner<String> partitioner1 = new FlinkFixedPartitioner<>();
		partitioner1.open(0, 3);
		Assert.assertEquals(0, partitioner1.partition("abc1", null, null, topic1, partitionsTopic1));
		Assert.assertEquals(0, partitioner1.partition("abc1", null, null, topic2, partitionsTopic2));

		final FlinkFixedPartitioner<String> partitioner2 = new FlinkFixedPartitioner<>();
		partitioner2.open(1, 3);
		Assert.assertEquals(1, partitioner2.partition("abc1", null, null, topic1, partitionsTopic1));
		Assert.assertEquals(1, partitioner2.partition("abc1", null, null, topic2, partitionsTopic2));

		final FlinkFixedPartitioner<String> partitioner3 = new FlinkFixedPartitioner<>();
		partitioner3.open(2, 3);
		Assert.assertEquals(2, partitioner3.partition("abc1", null, null, topic1, partitionsTopic1));
		Assert.assertEquals(0, partitioner3.partition("abc1", null, null, topic2, partitionsTopic2));
	}

	/**
	 * Tests that the determined target partition for individual topics remains identical in the case of new partitions.
	 */
	@Test
	public void testIncreasingPartitions() {
		final String topic1 = "topic-1";
		final String topic2 = "topic-2";

		final int[] initialPartitionsTopic1 = new int[]{0, 1};
		final int[] increasedPartitionsTopic1 = new int[]{0, 1, 2, 3};

		final int[] initialPartitionsTopic2 = new int[]{0, 1, 2};
		final int[] increasedPartitionsTopic2 = new int[]{0, 1, 2, 3, 4};

		final FlinkFixedPartitioner<String> partitioner1 = new FlinkFixedPartitioner<>();
		partitioner1.open(0, 3);
		Assert.assertEquals(
			partitioner1.partition("abc1", null, null, topic1, initialPartitionsTopic1),
			partitioner1.partition("abc1", null, null, topic1, increasedPartitionsTopic1));
		Assert.assertEquals(
			partitioner1.partition("abc1", null, null, topic2, initialPartitionsTopic2),
			partitioner1.partition("abc1", null, null, topic2, increasedPartitionsTopic2));

		final FlinkFixedPartitioner<String> partitioner2 = new FlinkFixedPartitioner<>();
		partitioner2.open(1, 3);
		Assert.assertEquals(
			partitioner2.partition("abc1", null, null, topic1, initialPartitionsTopic1),
			partitioner2.partition("abc1", null, null, topic1, increasedPartitionsTopic1));
		Assert.assertEquals(
			partitioner2.partition("abc1", null, null, topic2, initialPartitionsTopic2),
			partitioner2.partition("abc1", null, null, topic2, increasedPartitionsTopic2));

		final FlinkFixedPartitioner<String> partitioner3 = new FlinkFixedPartitioner<>();
		partitioner3.open(2, 3);
		Assert.assertEquals(
			partitioner3.partition("abc1", null, null, topic1, initialPartitionsTopic1),
			partitioner3.partition("abc1", null, null, topic1, increasedPartitionsTopic1));
		Assert.assertEquals(
			partitioner3.partition("abc1", null, null, topic2, initialPartitionsTopic2),
			partitioner3.partition("abc1", null, null, topic2, increasedPartitionsTopic2));
	}

}
