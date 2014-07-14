/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package org.apache.flink.streaming.partitioner;

import static org.junit.Assert.assertArrayEquals;

import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.junit.Before;
import org.junit.Test;

public class BroadcastPartitionerTest {

	private BroadcastPartitioner broadcastPartitioner1;
	private BroadcastPartitioner broadcastPartitioner2;
	private BroadcastPartitioner broadcastPartitioner3;
	
	private StreamRecord streamRecord = new StreamRecord();

	@Before
	public void setPartitioner() {
		broadcastPartitioner1 = new BroadcastPartitioner();
		broadcastPartitioner2 = new BroadcastPartitioner();
		broadcastPartitioner3 = new BroadcastPartitioner();

	}

	@Test
	public void testSelectChannels() {
		int[] first = new int[] { 0 };
		int[] second = new int[] { 0, 1 };
		int[] sixth = new int[] { 0, 1, 2, 3, 4, 5 };

		assertArrayEquals(first, broadcastPartitioner1.selectChannels(streamRecord, 1));
		assertArrayEquals(second, broadcastPartitioner2.selectChannels(streamRecord, 2));
		assertArrayEquals(sixth, broadcastPartitioner3.selectChannels(streamRecord, 6));
	}
}
