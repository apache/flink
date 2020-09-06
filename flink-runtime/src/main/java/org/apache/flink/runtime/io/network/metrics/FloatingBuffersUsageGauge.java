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

package org.apache.flink.runtime.io.network.metrics;

import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Gauge metric measuring the floating buffers usage gauge for {@link SingleInputGate}s.
 */
public class FloatingBuffersUsageGauge extends AbstractBuffersUsageGauge {

	public FloatingBuffersUsageGauge(SingleInputGate[] inputGates) {
		super(checkNotNull(inputGates));
	}

	@Override
	public int calculateUsedBuffers(SingleInputGate inputGate) {
		int availableFloatingBuffers = 0;
		BufferPool bufferPool = inputGate.getBufferPool();
		if (bufferPool != null) {
			int requestedFloatingBuffers = bufferPool.bestEffortGetNumOfUsedBuffers();
			for (InputChannel ic : inputGate.getInputChannels().values()) {
				if (ic instanceof RemoteInputChannel) {
					availableFloatingBuffers += ((RemoteInputChannel) ic).unsynchronizedGetFloatingBuffersAvailable();
				}
			}
			return Math.max(0, requestedFloatingBuffers - availableFloatingBuffers);
		}
		return 0;
	}

	@Override
	public int calculateTotalBuffers(SingleInputGate inputGate) {
		BufferPool bufferPool = inputGate.getBufferPool();
		if (bufferPool != null) {
			return inputGate.getBufferPool().getNumBuffers();
		}
		return 0;
	}
}
