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
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.streaming.api.streamrecord.ArrayStreamRecord;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class FieldsPartitionerTest {

	private FieldsPartitioner fieldsPartitioner;
	private StreamRecord streamRecord1 = new ArrayStreamRecord(1).setTuple(0, new Tuple2<String, Integer>("test", 0));
	private StreamRecord streamRecord2 = new ArrayStreamRecord(1).setTuple(0, new Tuple2<String, Integer>("test", 42));

	@Before
	public void setPartitioner() {
		fieldsPartitioner = new FieldsPartitioner(0);
	}

	@Test
	public void testSelectChannelsLength() {
		assertEquals(1,
				fieldsPartitioner.selectChannels(streamRecord1, 1).length);
		assertEquals(1,
				fieldsPartitioner.selectChannels(streamRecord1, 2).length);
		assertEquals(1,
				fieldsPartitioner.selectChannels(streamRecord1, 1024).length);
	}

	@Test
	public void testSelectChannelsGrouping() {
		assertArrayEquals(fieldsPartitioner.selectChannels(streamRecord1, 1),
				fieldsPartitioner.selectChannels(streamRecord2, 1));
		assertArrayEquals(fieldsPartitioner.selectChannels(streamRecord1, 2),
				fieldsPartitioner.selectChannels(streamRecord2, 2));
		assertArrayEquals(
				fieldsPartitioner.selectChannels(streamRecord1, 1024),
				fieldsPartitioner.selectChannels(streamRecord2, 1024));
	}
}
