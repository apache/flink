/**
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
 */

package org.apache.flink.streaming.partitioner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.junit.Before;
import org.junit.Test;

public class FieldsPartitionerTest {

	private FieldsPartitioner fieldsPartitioner;
	private StreamRecord streamRecord1 = new StreamRecord().setTuple(new Tuple2<String, Integer>("test", 0));
	private StreamRecord streamRecord2 = new StreamRecord().setTuple(new Tuple2<String, Integer>("test", 42));

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
