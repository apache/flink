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
 * Tests for {@link GlobalPartitioner}.
 */
public class GlobalPartitionerTest {

	private GlobalPartitioner<Tuple> globalPartitioner;
	private StreamRecord<Tuple> streamRecord = new StreamRecord<Tuple>(null);
	private SerializationDelegate<StreamRecord<Tuple>> sd = new SerializationDelegate<StreamRecord<Tuple>>(
			null);

	@Before
	public void setPartitioner() {
		globalPartitioner = new GlobalPartitioner<Tuple>();
	}

	@Test
	public void testSelectChannels() {
		int[] result = new int[] { 0 };

		sd.setInstance(streamRecord);

		assertArrayEquals(result, globalPartitioner.selectChannels(sd, 1));
		assertArrayEquals(result, globalPartitioner.selectChannels(sd, 2));
		assertArrayEquals(result, globalPartitioner.selectChannels(sd, 1024));
	}
}
