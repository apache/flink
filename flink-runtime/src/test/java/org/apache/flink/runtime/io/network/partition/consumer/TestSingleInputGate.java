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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static org.mockito.Mockito.spy;

/**
 * A test input gate to mock reading data.
 */
public class TestSingleInputGate {

	protected final SingleInputGate inputGate;

	protected final TestInputChannel[] inputChannels;

	public TestSingleInputGate(int numberOfInputChannels) {
		this(numberOfInputChannels, true);
	}

	public TestSingleInputGate(int numberOfInputChannels, boolean initialize) {
		checkArgument(numberOfInputChannels >= 1);

		this.inputGate = spy(new SingleInputGate(
				"Test Task Name", new IntermediateDataSetID(), 0, numberOfInputChannels));

		this.inputChannels = new TestInputChannel[numberOfInputChannels];

		if (initialize) {
			for (int i = 0; i < numberOfInputChannels; i++) {
				inputChannels[i] = new TestInputChannel(inputGate, i);
				inputGate.setInputChannel(new IntermediateResultPartitionID(), inputChannels[i].getInputChannel());
			}
		}
	}

	public TestSingleInputGate read(Buffer buffer, int channelIndex) throws IOException, InterruptedException {
		checkElementIndex(channelIndex, inputGate.getNumberOfInputChannels());

		inputChannels[channelIndex].read(buffer);

		return this;
	}

	public TestSingleInputGate readBuffer() throws IOException, InterruptedException {
		return readBuffer(0);
	}

	public TestSingleInputGate readBuffer(int channelIndex) throws IOException, InterruptedException {
		inputChannels[channelIndex].readBuffer();

		return this;
	}

	public TestSingleInputGate readEvent() throws IOException, InterruptedException {
		return readEvent(0);
	}

	public TestSingleInputGate readEvent(int channelIndex) throws IOException, InterruptedException {
		inputChannels[channelIndex].readEvent();

		return this;
	}

	public TestSingleInputGate readEndOfSuperstepEvent() throws IOException, InterruptedException {
		for (TestInputChannel inputChannel : inputChannels) {
			inputChannel.readEndOfSuperstepEvent();
		}

		return this;
	}

	public TestSingleInputGate readEndOfSuperstepEvent(int channelIndex) throws IOException, InterruptedException {
		inputChannels[channelIndex].readEndOfSuperstepEvent();

		return this;
	}

	public TestSingleInputGate readEndOfPartitionEvent() throws IOException, InterruptedException {
		for (TestInputChannel inputChannel : inputChannels) {
			inputChannel.readEndOfPartitionEvent();
		}

		return this;
	}

	public TestSingleInputGate readEndOfPartitionEvent(int channelIndex) throws IOException, InterruptedException {
		inputChannels[channelIndex].readEndOfPartitionEvent();

		return this;
	}

	public SingleInputGate getInputGate() {
		return inputGate;
	}

	// ------------------------------------------------------------------------

	public List<Integer> readAllChannels() throws IOException, InterruptedException {
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
