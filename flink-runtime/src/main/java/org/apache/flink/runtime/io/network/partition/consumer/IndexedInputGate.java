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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

/**
 * An {@link InputGate} with a specific index.
 */
public abstract class IndexedInputGate extends InputGate implements CheckpointableInput {
	/**
	 * Returns the index of this input gate. Only supported on
	 */
	public abstract int getGateIndex();

	@Override
	public void checkpointStarted(CheckpointBarrier barrier) {
		for (int index = 0, numChannels = getNumberOfInputChannels(); index < numChannels; index++) {
			getChannel(index).checkpointStarted(barrier);
		}
	}

	@Override
	public void checkpointStopped(long cancelledCheckpointId) {
		for (int index = 0, numChannels = getNumberOfInputChannels(); index < numChannels; index++) {
			getChannel(index).checkpointStopped(cancelledCheckpointId);
		}
	}

	@Override
	public int getInputGateIndex() {
		return getGateIndex();
	}

	@Override
	public void blockConsumption(int inputChannelIdx) {
		// Unused. Network stack is blocking consumption automatically by revoking credits.
	}
}
