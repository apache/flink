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

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKeyHashPartitioner;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

/**
 * Tests for the {@link FlinkKeyHashPartitioner}.
 */
public class FlinkKeyHashPartitionerTest {

	@Test
	public void testMoreFlinkThanBrokers() {
		FlinkKeyHashPartitioner<String> part = new FlinkKeyHashPartitioner<>();

		int[] partitions = new int[]{0, 1};

		part.open(0, 4);
		Assert.assertEquals(0, part.partition("abc", "abc1".getBytes(), null, null, partitions));

		part.open(1, 4);
		Assert.assertEquals(1, part.partition("abc", "abc2".getBytes(), null, null, partitions));

		part.open(2, 4);
		Assert.assertEquals(0, part.partition("abc", "abc3".getBytes(), null, null, partitions));
		Assert.assertEquals(0, part.partition("abc", "abc3".getBytes(), null, null, partitions)); // check if it is changing ;)

		part.open(3, 4);
		Assert.assertEquals(1, part.partition("abc", "abc4".getBytes(), null, null, partitions));
	}

	@Test
	public void testFewerPartitions() {
		FlinkKeyHashPartitioner<String> part = new FlinkKeyHashPartitioner<>();

		int[] partitions = new int[]{0, 1, 2, 3, 4};
		part.open(0, 2);
		Assert.assertEquals(4, part.partition("abc1", "abc1".getBytes(), null, null, partitions));
		Assert.assertEquals(4, part.partition("abc1", "abc1".getBytes(), null, null, partitions));

		// The resulting kafka partition is dependent ONLY on the key
		part.open(1, 2);
		Assert.assertEquals(4, part.partition("abc1", "abc1".getBytes(), null, null, partitions));
		Assert.assertEquals(4, part.partition("abc1", "abc1".getBytes(), null, null, partitions));
	}

	@Test
	public void testSameNumber() {
		FlinkKeyHashPartitioner<String> part = new FlinkKeyHashPartitioner<>();
		int[] partitions = new int[]{0, 1, 2};

		part.open(0, 3);
		Assert.assertEquals(0, part.partition("abc", "abc2".getBytes(), null, null, partitions));

		part.open(1, 3);
		Assert.assertEquals(0, part.partition("abc", "abc2".getBytes(), null, null, partitions));

		part.open(2, 3);
		Assert.assertEquals(0, part.partition("abc", "abc2".getBytes(), null, null, partitions));
	}

	private static class NegativeHashPartitioner<T> extends FlinkKeyHashPartitioner<T> {
		private static final long serialVersionUID = 6591915886288619235L;
		@Override
		protected int hash(@Nullable byte[] key) {
			// Force the return value to ALWAYS be nagative for this test
			return -1 * Math.abs(super.hash(key));
		}
	}

	/**
	 * Ensure it is ok if the hash function returns a negative value.
	 */
	@Test
	public void testNegativeHashGuard() {
		FlinkKeyHashPartitioner<String> part = new NegativeHashPartitioner<>();

		int[] partitions = new int[]{0, 1};

		part.open(0, 4);
		Assert.assertEquals(0, part.partition("abc", "abc1".getBytes(), null, null, partitions));
		Assert.assertEquals(1, part.partition("abc", "abc2".getBytes(), null, null, partitions));
		Assert.assertEquals(0, part.partition("abc", "abc3".getBytes(), null, null, partitions));
		Assert.assertEquals(1, part.partition("abc", "abc4".getBytes(), null, null, partitions));
	}

}
