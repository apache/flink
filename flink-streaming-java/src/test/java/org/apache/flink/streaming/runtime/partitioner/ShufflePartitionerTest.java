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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.api.java.tuple.Tuple;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ShufflePartitioner}.
 */
public class ShufflePartitionerTest extends StreamPartitionerTest {

	@Override
	public StreamPartitioner<Tuple> createPartitioner() {
		return new ShufflePartitioner<>();
	}

	@Test
	public void testSelectChannelsInterval() {
		assertSelectedChannel(0, 1);

		assertTrue(0 <= selectChannel(2));
		assertTrue(2 > selectChannel(2));

		assertTrue(0 <= selectChannel(1024));
		assertTrue(1024 > selectChannel(1024));
	}

	private int selectChannel(int numberOfChannels) {
		return streamPartitioner.selectChannels(serializationDelegate, numberOfChannels)[0];
	}
}
