/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package org.apache.flink.streaming.partitioner;

import static org.junit.Assert.assertArrayEquals;

import org.apache.flink.streaming.api.streamrecord.ArrayStreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.partitioner.BroadcastPartitioner;
import org.junit.Before;
import org.junit.Test;

public class BroadcastPartitionerTest {

	private BroadcastPartitioner broadcastPartitioner;
	private StreamRecord streamRecord = new ArrayStreamRecord();

	@Before
	public void setPartitioner() {
		broadcastPartitioner = new BroadcastPartitioner();
	}

	@Test
	public void testSelectChannels() {
		int[] first = new int[] { 0 };
		int[] second = new int[] { 0, 1 };
		int[] sixth = new int[] { 0, 1, 2, 3, 4, 5 };

		assertArrayEquals(first, broadcastPartitioner.selectChannels(streamRecord, 1));
		assertArrayEquals(second, broadcastPartitioner.selectChannels(streamRecord, 2));
		assertArrayEquals(sixth, broadcastPartitioner.selectChannels(streamRecord, 6));
	}
}
