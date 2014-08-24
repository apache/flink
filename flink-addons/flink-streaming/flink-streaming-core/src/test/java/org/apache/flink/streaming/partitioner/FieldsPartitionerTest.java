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

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.junit.Before;
import org.junit.Test;

public class FieldsPartitionerTest {

	private FieldsPartitioner<Tuple> fieldsPartitioner;
	private StreamRecord<Tuple> streamRecord1 = new StreamRecord<Tuple>()
			.setObject(new Tuple2<String, Integer>("test", 0));
	private StreamRecord<Tuple> streamRecord2 = new StreamRecord<Tuple>()
			.setObject(new Tuple2<String, Integer>("test", 42));
	private SerializationDelegate<StreamRecord<Tuple>> sd1 = new SerializationDelegate<StreamRecord<Tuple>>(
			null);
	private SerializationDelegate<StreamRecord<Tuple>> sd2 = new SerializationDelegate<StreamRecord<Tuple>>(
			null);

	@Before
	public void setPartitioner() {
		fieldsPartitioner = new FieldsPartitioner<Tuple>(0);
	}

	@Test
	public void testSelectChannelsLength() {
		sd1.setInstance(streamRecord1);
		assertEquals(1, fieldsPartitioner.selectChannels(sd1, 1).length);
		assertEquals(1, fieldsPartitioner.selectChannels(sd1, 2).length);
		assertEquals(1, fieldsPartitioner.selectChannels(sd1, 1024).length);
	}

	@Test
	public void testSelectChannelsGrouping() {
		sd1.setInstance(streamRecord1);
		sd2.setInstance(streamRecord2);

		assertArrayEquals(fieldsPartitioner.selectChannels(sd1, 1),
				fieldsPartitioner.selectChannels(sd2, 1));
		assertArrayEquals(fieldsPartitioner.selectChannels(sd1, 2),
				fieldsPartitioner.selectChannels(sd2, 2));
		assertArrayEquals(fieldsPartitioner.selectChannels(sd1, 1024),
				fieldsPartitioner.selectChannels(sd2, 1024));
	}
}
