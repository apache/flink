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

package eu.stratosphere.streaming.partitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.streaming.api.streamrecord.ArrayStreamRecord;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class ShufflePartitionerTest {

	private ShufflePartitioner shufflePartitioner;
	private StreamRecord streamRecord = new ArrayStreamRecord();

	@Before
	public void setPartitioner() {
		shufflePartitioner = new ShufflePartitioner();
	}

	@Test
	public void testSelectChannelsLength() {
		assertEquals(1,
				shufflePartitioner.selectChannels(streamRecord, 1).length);
		assertEquals(1,
				shufflePartitioner.selectChannels(streamRecord, 2).length);
		assertEquals(1,
				shufflePartitioner.selectChannels(streamRecord, 1024).length);
	}

	@Test
	public void testSelectChannelsInterval() {
		assertEquals(0, shufflePartitioner.selectChannels(streamRecord, 1)[0]);

		assertTrue(0 <= shufflePartitioner.selectChannels(streamRecord, 2)[0]);
		assertTrue(2 > shufflePartitioner.selectChannels(streamRecord, 2)[0]);

		assertTrue(0 <= shufflePartitioner.selectChannels(streamRecord, 1024)[0]);
		assertTrue(1024 > shufflePartitioner.selectChannels(streamRecord, 1024)[0]);
	}
}
