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

import org.junit.Assert;
import org.junit.Test;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;

public class TestFixedPartitioner {


	/**
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
		FixedPartitioner<String> part = new FixedPartitioner<>();

		int[] partitions = new int[]{0};

		part.open(0, 4, partitions);
		Assert.assertEquals(0, part.partition("abc1", null, null, partitions.length));

		part.open(1, 4, partitions);
		Assert.assertEquals(0, part.partition("abc2", null, null, partitions.length));

		part.open(2, 4, partitions);
		Assert.assertEquals(0, part.partition("abc3", null, null, partitions.length));
		Assert.assertEquals(0, part.partition("abc3", null, null, partitions.length)); // check if it is changing ;)

		part.open(3, 4, partitions);
		Assert.assertEquals(0, part.partition("abc4", null, null, partitions.length));
	}

	/**
	 *
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
		FixedPartitioner<String> part = new FixedPartitioner<>();

		int[] partitions = new int[]{0, 1, 2, 3, 4};
		part.open(0, 2, partitions);
		Assert.assertEquals(0, part.partition("abc1", null, null, partitions.length));
		Assert.assertEquals(0, part.partition("abc1", null, null, partitions.length));

		part.open(1, 2, partitions);
		Assert.assertEquals(1, part.partition("abc1", null, null, partitions.length));
		Assert.assertEquals(1, part.partition("abc1", null, null, partitions.length));
	}

	/*
	 * 		Flink Sinks:		Kafka Partitions
	 * 			1	------------>--->	1
	 * 			2	-----------/----> 	2
	 * 			3	----------/
	 */
	@Test
	public void testMixedCase() {
		FixedPartitioner<String> part = new FixedPartitioner<>();
		int[] partitions = new int[]{0,1};

		part.open(0, 3, partitions);
		Assert.assertEquals(0, part.partition("abc1", null, null, partitions.length));

		part.open(1, 3, partitions);
		Assert.assertEquals(1, part.partition("abc1", null, null, partitions.length));

		part.open(2, 3, partitions);
		Assert.assertEquals(0, part.partition("abc1", null, null, partitions.length));

	}

}
