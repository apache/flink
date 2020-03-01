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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A mocked input channel.
 */
public class TestInputChannel extends InputChannel {

	private final Queue<BufferAndAvailabilityProvider> buffers = new ConcurrentLinkedQueue<>();

	private final Collection<Buffer> allReturnedBuffers = new ArrayList<>();

	private BufferAndAvailabilityProvider lastProvider = null;

	private boolean isReleased = false;

	TestInputChannel(SingleInputGate inputGate, int channelIndex) {
		super(inputGate, channelIndex, new ResultPartitionID(), 0, 0, new SimpleCounter(), new SimpleCounter());
	}

	public TestInputChannel read(Buffer buffer) throws IOException, InterruptedException {
		return read(buffer, true);
	}

	public TestInputChannel read(Buffer buffer, boolean moreAvailable) throws IOException, InterruptedException {
		addBufferAndAvailability(new BufferAndAvailability(buffer, moreAvailable, 0));

		return this;
	}

	TestInputChannel readBuffer() throws IOException, InterruptedException {
		return readBuffer(true);
	}

	TestInputChannel readBuffer(boolean moreAvailable) throws IOException, InterruptedException {
		final Buffer buffer = mock(Buffer.class);
		when(buffer.isBuffer()).thenReturn(true);

		return read(buffer, moreAvailable);
	}

	TestInputChannel readEndOfPartitionEvent() {
		addBufferAndAvailability(
			() -> {
				setReleased();
				return Optional.of(new BufferAndAvailability(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE),
					false,
					0));
			}
		);
		return this;
	}

	void addBufferAndAvailability(BufferAndAvailability bufferAndAvailability) {
		buffers.add(() -> Optional.of(bufferAndAvailability));
	}

	void addBufferAndAvailability(BufferAndAvailabilityProvider bufferAndAvailability) {
		buffers.add(bufferAndAvailability);
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates test input channels and attaches them to the specified input gate.
	 *
	 * @return The created test input channels.
	 */
	static TestInputChannel[] createInputChannels(SingleInputGate inputGate, int numberOfInputChannels) {
		checkNotNull(inputGate);
		checkArgument(numberOfInputChannels > 0);

		TestInputChannel[] mocks = new TestInputChannel[numberOfInputChannels];

		for (int i = 0; i < numberOfInputChannels; i++) {
			mocks[i] = new TestInputChannel(inputGate, i);

			inputGate.setInputChannel(new IntermediateResultPartitionID(), mocks[i]);
		}

		return mocks;
	}

	@Override
	void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {

	}

	@Override
	Optional<BufferAndAvailability> getNextBuffer() throws IOException, InterruptedException {
		BufferAndAvailabilityProvider provider = buffers.poll();

		if (provider != null) {
			lastProvider = provider;
			Optional<BufferAndAvailability> baa = provider.getBufferAvailability();
			baa.ifPresent((v) -> allReturnedBuffers.add(v.buffer()));
			return baa;
		} else if (lastProvider != null) {
			return lastProvider.getBufferAvailability();
		} else {
			return Optional.empty();
		}
	}

	@Override
	void sendTaskEvent(TaskEvent event) throws IOException {

	}

	@Override
	boolean isReleased() {
		return isReleased;
	}

	void setReleased() {
		this.isReleased = true;
	}

	@Override
	void releaseAllResources() throws IOException {

	}

	@Override
	protected void notifyChannelNonEmpty() {

	}

	public void assertReturnedDataBuffersAreRecycled() {
		assertReturnedBuffersAreRecycled(true, false);
	}

	public void assertReturnedEventsAreRecycled() {
		assertReturnedBuffersAreRecycled(false, true);
	}

	public void assertAllReturnedBuffersAreRecycled() {
		assertReturnedBuffersAreRecycled(true, true);
	}

	private void assertReturnedBuffersAreRecycled(boolean assertBuffers, boolean assertEvents) {
		for (Buffer b : allReturnedBuffers) {
			if (b.isBuffer() && assertBuffers && !b.isRecycled()) {
				fail("Data Buffer " + b + " not recycled");
			}
			if (!b.isBuffer() && assertEvents && !b.isRecycled()) {
				fail("Event Buffer " + b + " not recycled");
			}
		}
	}

	interface BufferAndAvailabilityProvider {
		Optional<BufferAndAvailability> getBufferAvailability() throws IOException, InterruptedException;
	}

}
