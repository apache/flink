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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.util.CloseableIterator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequest.completeInput;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequest.completeOutput;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequest.write;
import static org.apache.flink.runtime.state.ChannelPersistenceITCase.getStreamFactoryFactory;
import static org.junit.Assert.fail;

/**
 * {@link ChannelStateWriteRequestDispatcherImpl} tests.
 */
@RunWith(Parameterized.class)
public class ChannelStateWriteRequestDispatcherTest {

	private final List<ChannelStateWriteRequest> requests;
	private final Optional<Class<?>> expectedException;
	public static final long CHECKPOINT_ID = 42L;

	@Parameters
	public static Object[][] data() {

		return new Object[][]{
			// valid calls
			new Object[]{empty(), asList(start(), completeIn(), completeOut())},
			new Object[]{empty(), asList(start(), writeIn(), completeIn())},
			new Object[]{empty(), asList(start(), writeOut(), completeOut())},
			new Object[]{empty(), asList(start(), completeIn(), writeOut())},
			new Object[]{empty(), asList(start(), completeOut(), writeIn())},
			// invalid without start
			new Object[]{of(IllegalArgumentException.class), singletonList(writeIn())},
			new Object[]{of(IllegalArgumentException.class), singletonList(writeOut())},
			new Object[]{of(IllegalArgumentException.class), singletonList(completeIn())},
			new Object[]{of(IllegalArgumentException.class), singletonList(completeOut())},
			// invalid double complete
			new Object[]{of(IllegalArgumentException.class), asList(start(), completeIn(), completeIn())},
			new Object[]{of(IllegalArgumentException.class), asList(start(), completeOut(), completeOut())},
			// invalid write after complete
			new Object[]{of(IllegalStateException.class), asList(start(), completeIn(), writeIn())},
			new Object[]{of(IllegalStateException.class), asList(start(), completeOut(), writeOut())},
			// invalid double start
			new Object[]{of(IllegalStateException.class), asList(start(), start())}
		};
	}

	private static CheckpointInProgressRequest completeOut() {
		return completeOutput(CHECKPOINT_ID);
	}

	private static CheckpointInProgressRequest completeIn() {
		return completeInput(CHECKPOINT_ID);
	}

	private static ChannelStateWriteRequest writeIn() {
		return write(CHECKPOINT_ID, new InputChannelInfo(1, 1), CloseableIterator.ofElement(
			new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(1), FreeingBufferRecycler.INSTANCE),
			Buffer::recycleBuffer
		));
	}

	private static ChannelStateWriteRequest writeOut() {
		return write(CHECKPOINT_ID, new ResultSubpartitionInfo(1, 1), new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(1), FreeingBufferRecycler.INSTANCE));
	}

	private static CheckpointStartRequest start() {
		return new CheckpointStartRequest(CHECKPOINT_ID, new ChannelStateWriteResult(), new CheckpointStorageLocationReference(new byte[]{1}));
	}

	public ChannelStateWriteRequestDispatcherTest(Optional<Class<?>> expectedException, List<ChannelStateWriteRequest> requests) {
		this.requests = requests;
		this.expectedException = expectedException;
	}

	@Test
	public void doRun() {
		ChannelStateWriteRequestDispatcher processor = new ChannelStateWriteRequestDispatcherImpl(getStreamFactoryFactory(), new ChannelStateSerializerImpl());
		try {
			for (ChannelStateWriteRequest request : requests) {
				processor.dispatch(request);
			}
		} catch (Throwable t) {
			if (expectedException.filter(e -> e.isInstance(t)).isPresent()) {
				return;
			}
			throw new RuntimeException("unexpected exception", t);
		}
		expectedException.ifPresent(e -> fail("expected exception " + e));
	}

}
