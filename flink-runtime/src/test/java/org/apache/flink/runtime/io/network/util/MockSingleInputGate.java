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

package org.apache.flink.runtime.io.network.util;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static org.mockito.Mockito.spy;

public class MockSingleInputGate {

	protected final SingleInputGate inputGate;

	protected final MockInputChannel[] inputChannels;

	public MockSingleInputGate(int numberOfInputChannels) {
		this(numberOfInputChannels, true);
	}

	public MockSingleInputGate(int numberOfInputChannels, boolean initialize) {
		checkArgument(numberOfInputChannels >= 1);

		this.inputGate = spy(new SingleInputGate(new IntermediateDataSetID(), 0, numberOfInputChannels));

		this.inputChannels = new MockInputChannel[numberOfInputChannels];

		if (initialize) {
			for (int i = 0; i < numberOfInputChannels; i++) {
				inputChannels[i] = new MockInputChannel(inputGate, i);
				inputGate.setInputChannel(new IntermediateResultPartitionID(), inputChannels[i].getInputChannel());
			}
		}
	}

	public MockSingleInputGate read(Buffer buffer, int channelIndex) throws IOException {
		checkElementIndex(channelIndex, inputGate.getNumberOfInputChannels());

		inputChannels[channelIndex].read(buffer);

		return this;
	}

	public MockSingleInputGate readBuffer() throws IOException {
		return readBuffer(0);
	}

	public MockSingleInputGate readBuffer(int channelIndex) throws IOException {
		inputChannels[channelIndex].readBuffer();

		return this;
	}

	public MockSingleInputGate readEvent() throws IOException {
		return readEvent(0);
	}

	public MockSingleInputGate readEvent(int channelIndex) throws IOException {
		inputChannels[channelIndex].readEvent();

		return this;
	}

	public MockSingleInputGate readEndOfSuperstepEvent() throws IOException {
		for (MockInputChannel inputChannel : inputChannels) {
			inputChannel.readEndOfSuperstepEvent();
		}

		return this;
	}

	public MockSingleInputGate readEndOfSuperstepEvent(int channelIndex) throws IOException {
		inputChannels[channelIndex].readEndOfSuperstepEvent();

		return this;
	}

	public MockSingleInputGate readEndOfPartitionEvent() throws IOException {
		for (MockInputChannel inputChannel : inputChannels) {
			inputChannel.readEndOfPartitionEvent();
		}

		return this;
	}

	public MockSingleInputGate readEndOfPartitionEvent(int channelIndex) throws IOException {
		inputChannels[channelIndex].readEndOfPartitionEvent();

		return this;
	}

	public SingleInputGate getInputGate() {
		return inputGate;
	}

	// ------------------------------------------------------------------------

	public List<Integer> readAllChannels() throws IOException {
		final List<Integer> readOrder = new ArrayList<Integer>(inputChannels.length);

		for (int i = 0; i < inputChannels.length; i++) {
			readOrder.add(i);
		}

		Collections.shuffle(readOrder);

		for (int channelIndex : readOrder) {
			inputChannels[channelIndex].readBuffer();
		}

		return readOrder;
	}
}
