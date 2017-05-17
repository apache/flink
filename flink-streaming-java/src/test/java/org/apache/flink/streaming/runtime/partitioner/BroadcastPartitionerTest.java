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
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link BroadcastPartitioner}.
 */
public class BroadcastPartitionerTest {

	private BroadcastPartitioner<Tuple> broadcastPartitioner1;
	private BroadcastPartitioner<Tuple> broadcastPartitioner2;
	private BroadcastPartitioner<Tuple> broadcastPartitioner3;

	private StreamRecord<Tuple> streamRecord = new StreamRecord<Tuple>(null);
	private SerializationDelegate<StreamRecord<Tuple>> sd = new SerializationDelegate<StreamRecord<Tuple>>(null);

	@Before
	public void setPartitioner() {
		broadcastPartitioner1 = new BroadcastPartitioner<Tuple>();
		broadcastPartitioner2 = new BroadcastPartitioner<Tuple>();
		broadcastPartitioner3 = new BroadcastPartitioner<Tuple>();

	}

	@Test
	public void testSelectChannels() {
		int[] first = new int[] { 0 };
		int[] second = new int[] { 0, 1 };
		int[] sixth = new int[] { 0, 1, 2, 3, 4, 5 };
		sd.setInstance(streamRecord);
		assertArrayEquals(first, broadcastPartitioner1.selectChannels(sd, 1));
		assertArrayEquals(second, broadcastPartitioner2.selectChannels(sd, 2));
		assertArrayEquals(sixth, broadcastPartitioner3.selectChannels(sd, 6));
	}
}
