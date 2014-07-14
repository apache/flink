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

import static org.junit.Assert.assertArrayEquals;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class GlobalPartitionerTest {

	private GlobalPartitioner globalPartitioner;
	private StreamRecord streamRecord = new StreamRecord();

	@Before
	public void setPartitioner() {
		globalPartitioner = new GlobalPartitioner();
	}

	@Test
	public void testSelectChannels() {
		int[] result = new int[] { 0 };

		assertArrayEquals(result,
				globalPartitioner.selectChannels(streamRecord, 1));
		assertArrayEquals(result,
				globalPartitioner.selectChannels(streamRecord, 2));
		assertArrayEquals(result,
				globalPartitioner.selectChannels(streamRecord, 1024));
	}
}