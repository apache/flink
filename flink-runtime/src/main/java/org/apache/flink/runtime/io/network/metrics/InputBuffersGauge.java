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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

/**
 * Gauge metric measuring the number of queued input buffers for {@link SingleInputGate}s.
 */
public class InputBuffersGauge implements Gauge<Integer> {

	private final SingleInputGate[] inputGates;

	public InputBuffersGauge(SingleInputGate[] inputGates) {
		this.inputGates = inputGates;
	}

	@Override
	public Integer getValue() {
		int totalBuffers = 0;

		for (SingleInputGate inputGate : inputGates) {
			totalBuffers += inputGate.getNumberOfQueuedBuffers();
		}

		return totalBuffers;
	}
}
