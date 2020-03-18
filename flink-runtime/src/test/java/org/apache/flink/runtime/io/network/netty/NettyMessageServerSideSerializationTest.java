/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.event.task.IntegerTaskEvent;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.encodeAndDecode;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the serialization and deserialization of the various {@link NettyMessage} sub-classes
 * sent from client side to server side.
 */
public class NettyMessageServerSideSerializationTest extends TestLogger {

	private final Random random = new Random();

	private EmbeddedChannel channel;

	@Before
	public void setup() {
		channel = new EmbeddedChannel(
			new NettyMessage.NettyMessageEncoder(), // For outbound messages
			new NettyMessage.NettyMessageDecoder()); // For inbound messages
	}

	@After
	public void tearDown() {
		if (channel != null) {
			channel.close();
		}
	}

	@Test
	public void testPartitionRequest() {
		NettyMessage.PartitionRequest expected = new NettyMessage.PartitionRequest(
			new ResultPartitionID(),
			random.nextInt(),
			new InputChannelID(),
			random.nextInt());

		NettyMessage.PartitionRequest actual = encodeAndDecode(expected, channel);

		assertEquals(expected.partitionId, actual.partitionId);
		assertEquals(expected.queueIndex, actual.queueIndex);
		assertEquals(expected.receiverId, actual.receiverId);
		assertEquals(expected.credit, actual.credit);
	}

	@Test
	public void testTaskEventRequest() {
		NettyMessage.TaskEventRequest expected = new NettyMessage.TaskEventRequest(
			new IntegerTaskEvent(random.nextInt()),
			new ResultPartitionID(),
			new InputChannelID());
		NettyMessage.TaskEventRequest actual = encodeAndDecode(expected, channel);

		assertEquals(expected.event, actual.event);
		assertEquals(expected.partitionId, actual.partitionId);
		assertEquals(expected.receiverId, actual.receiverId);
	}

	@Test
	public void testCancelPartitionRequest() {
		NettyMessage.CancelPartitionRequest expected = new NettyMessage.CancelPartitionRequest(
			new InputChannelID());
		NettyMessage.CancelPartitionRequest actual = encodeAndDecode(expected, channel);

		assertEquals(expected.receiverId, actual.receiverId);
	}

	@Test
	public void testCloseRequest() {
		NettyMessage.CloseRequest expected = new NettyMessage.CloseRequest();
		NettyMessage.CloseRequest actual = encodeAndDecode(expected, channel);

		assertEquals(expected.getClass(), actual.getClass());
	}

	@Test
	public void testAddCredit() {
		NettyMessage.AddCredit expected = new NettyMessage.AddCredit(
			random.nextInt(Integer.MAX_VALUE) + 1,
			new InputChannelID());
		NettyMessage.AddCredit actual = encodeAndDecode(expected, channel);

		assertEquals(expected.credit, actual.credit);
		assertEquals(expected.receiverId, actual.receiverId);
	}
}
