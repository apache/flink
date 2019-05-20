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

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link BroadcastPartitioner}.
 */
public class BroadcastPartitionerTest extends StreamPartitionerTest {

	@Override
	public StreamPartitioner<Tuple> createPartitioner() {
		StreamPartitioner<Tuple> partitioner = new BroadcastPartitioner<>();
		assertTrue(partitioner.isBroadcast());
		return partitioner;
	}

	@Test
	public void testSelectChannels() {
		try {
			streamPartitioner.selectChannel(serializationDelegate);
		} catch (UnsupportedOperationException ex) {
			return;
		}

		fail("Broadcast selector does not support select channels.");
	}
}
