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
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link HashPartitioner}.
 */
public class HashPartitionerTest {
	private HashPartitioner<Tuple> hashPartitioner1;
	private HashPartitioner<Tuple> hashPartitioner2;
	private HashPartitioner<Tuple> hashPartitioner3;

	private SerializationDelegate<StreamRecord<Tuple>> sd = new SerializationDelegate<StreamRecord<Tuple>>(null);

	@Before
	public void setPartitioner() {
		hashPartitioner1 = new HashPartitioner<Tuple>(1);
		hashPartitioner2 = new HashPartitioner<Tuple>(2);
		hashPartitioner3 = new HashPartitioner<Tuple>(6);
	}

	@Test
	public void testRecourdNullSelectChannels() {
		int[] first = new int[] { 0 };
		int[] second = new int[] { 0 };
		int[] sixth = new int[] { 0 };
		StreamRecord<Tuple> streamRecord = new StreamRecord<Tuple>(null);
		sd.setInstance(streamRecord);
		assertArrayEquals(first, hashPartitioner1.selectChannels(sd, 1));
		assertArrayEquals(second, hashPartitioner2.selectChannels(sd, 2));
		assertArrayEquals(sixth, hashPartitioner3.selectChannels(sd, 6));
	}

	@Test
	public void testSelectChannels() {
		int[] first = new int[] { 0 };
		int[] second = new int[] { 1 };
		int[] sixth = new int[] {  5 };
		StreamRecord<Tuple> streamRecord = new StreamRecord<Tuple>(new Tuple5<>(0L, 100L, 1001L, 10000L, 100000L));
		sd.setInstance(streamRecord);
		assertArrayEquals(first, hashPartitioner1.selectChannels(sd, 1));
		assertArrayEquals(second, hashPartitioner2.selectChannels(sd, 2));
		assertArrayEquals(sixth, hashPartitioner3.selectChannels(sd, 6));
	}

}
