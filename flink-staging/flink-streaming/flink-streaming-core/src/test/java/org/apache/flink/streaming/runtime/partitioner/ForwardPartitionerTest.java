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

import static org.junit.Assert.*;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.Before;
import org.junit.Test;

public class ForwardPartitionerTest {

	private RebalancePartitioner<Tuple> forwardPartitioner;
	private StreamRecord<Tuple> streamRecord = new StreamRecord<Tuple>();
	private SerializationDelegate<StreamRecord<Tuple>> sd = new SerializationDelegate<StreamRecord<Tuple>>(
			null);

	@Before
	public void setPartitioner() {
		forwardPartitioner = new RebalancePartitioner<Tuple>(true);
	}

	@Test
	public void testSelectChannelsLength() {
		sd.setInstance(streamRecord);
		assertEquals(1, forwardPartitioner.selectChannels(sd, 1).length);
		assertEquals(1, forwardPartitioner.selectChannels(sd, 2).length);
		assertEquals(1, forwardPartitioner.selectChannels(sd, 1024).length);
	}

	@Test
	public void testSelectChannelsInterval() {
		sd.setInstance(streamRecord);
		assertEquals(0, forwardPartitioner.selectChannels(sd, 1)[0]);
		assertEquals(1, forwardPartitioner.selectChannels(sd, 2)[0]);
		assertEquals(2, forwardPartitioner.selectChannels(sd, 1024)[0]);
	}
}
