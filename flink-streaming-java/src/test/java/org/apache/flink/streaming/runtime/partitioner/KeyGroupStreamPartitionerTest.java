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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link KeyGroupStreamPartitioner}.
 */
public class KeyGroupStreamPartitionerTest extends TestLogger {

	private KeyGroupStreamPartitioner<Tuple2<String, Integer>, String> keyGroupPartitioner;
	private StreamRecord<Tuple2<String, Integer>> streamRecord1 = new StreamRecord<>(new Tuple2<>("test", 0));
	private StreamRecord<Tuple2<String, Integer>> streamRecord2 = new StreamRecord<>(new Tuple2<>("test", 42));

	@Before
	public void setPartitioner() {
		keyGroupPartitioner = new KeyGroupStreamPartitioner<>(new KeySelector<Tuple2<String, Integer>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String getKey(Tuple2<String, Integer> value) throws Exception {
				return value.getField(0);
			}
		},
		1024);
	}

	@Test
	public void testSelectChannelsGrouping() {
		assertEquals(keyGroupPartitioner.selectChannel(streamRecord1, 1),
				keyGroupPartitioner.selectChannel(streamRecord2, 1));
		assertEquals(keyGroupPartitioner.selectChannel(streamRecord1, 2),
				keyGroupPartitioner.selectChannel(streamRecord2, 2));
		assertEquals(keyGroupPartitioner.selectChannel(streamRecord1, 1024),
				keyGroupPartitioner.selectChannel(streamRecord2, 1024));
	}
}
