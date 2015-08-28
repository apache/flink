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
	 *   		Flink Sinks:		Kafka Partitions
	 * 			1	---------------->	1
	 * 			2   --------------/
	 * 			3   -------------/
	 * 			4	------------/
	 */
	@Test
	public void testMoreFlinkThanBrokers() {
		FixedPartitioner part = new FixedPartitioner();

		int[] partitions = new int[]{0};

		part.prepare(0, 4, partitions);
		Assert.assertEquals(0, part.partition("abc1", partitions.length));

		part.prepare(1, 4, partitions);
		Assert.assertEquals(0, part.partition("abc2", partitions.length));

		part.prepare(2, 4, partitions);
		Assert.assertEquals(0, part.partition("abc3", partitions.length));
		Assert.assertEquals(0, part.partition("abc3", partitions.length)); // check if it is changing ;)

		part.prepare(3, 4, partitions);
		Assert.assertEquals(0, part.partition("abc4", partitions.length));
	}

	/*
	 * 		Flink Sinks:		Kafka Partitions
	 * 			1	---------------->	1
	 * 			2	---------------->	2
	 * 									3
	 * 									4
	 * 									5
	 */
	@Test
	public void testFewerPartitions() {
		FixedPartitioner part = new FixedPartitioner();

		int[] partitions = new int[]{0, 1, 2, 3, 4};
		part.prepare(0, 2, partitions);
		Assert.assertEquals(0, part.partition("abc1", partitions.length));
		Assert.assertEquals(0, part.partition("abc1", partitions.length));

		part.prepare(1, 2, partitions);
		Assert.assertEquals(1, part.partition("abc1", partitions.length));
		Assert.assertEquals(1, part.partition("abc1", partitions.length));
	}

	/*
	 * 		Flink Sinks:		Kafka Partitions
	 * 			1	------------>--->	1
	 * 			2	-----------/----> 	2
	 * 			3	----------/
	 */
	@Test
	public void testMixedCase() {
		FixedPartitioner part = new FixedPartitioner();
		int[] partitions = new int[]{0,1};

		part.prepare(0, 3, partitions);
		Assert.assertEquals(0, part.partition("abc1", partitions.length));

		part.prepare(1, 3, partitions);
		Assert.assertEquals(1, part.partition("abc1", partitions.length));

		part.prepare(2, 3, partitions);
		Assert.assertEquals(0, part.partition("abc1", partitions.length));

	}

}
