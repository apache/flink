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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Collects metrics of an input gate.
 */
public class InputGateMetrics {

	private final SingleInputGate inputGate;

	// ------------------------------------------------------------------------

	private InputGateMetrics(SingleInputGate inputGate) {
		this.inputGate = checkNotNull(inputGate);
	}

	// ------------------------------------------------------------------------

	// these methods are package private to make access from the nested classes faster

	/**
	 * Iterates over all input channels and collects the total number of queued buffers in a
	 * best-effort way.
	 *
	 * @return total number of queued buffers
	 */
	long refreshAndGetTotal() {
		long total = 0;

		for (InputChannel channel : inputGate.getInputChannels().values()) {
			if (channel instanceof RemoteInputChannel) {
				RemoteInputChannel rc = (RemoteInputChannel) channel;

				total += rc.unsynchronizedGetNumberOfQueuedBuffers();
			}
		}

		return total;
	}

	/**
	 * Iterates over all input channels and collects the minimum number of queued buffers in a
	 * channel in a best-effort way.
	 *
	 * @return minimum number of queued buffers per channel (<tt>0</tt> if no channels exist)
	 */
	int refreshAndGetMin() {
		int min = Integer.MAX_VALUE;

		Collection<InputChannel> channels = inputGate.getInputChannels().values();

		for (InputChannel channel : channels) {
			if (channel instanceof RemoteInputChannel) {
				RemoteInputChannel rc = (RemoteInputChannel) channel;

				int size = rc.unsynchronizedGetNumberOfQueuedBuffers();
				min = Math.min(min, size);
			}
		}

		if (min == Integer.MAX_VALUE) { // in case all channels are local, or the channel collection was empty
			return 0;
		}
		return min;
	}

	/**
	 * Iterates over all input channels and collects the maximum number of queued buffers in a
	 * channel in a best-effort way.
	 *
	 * @return maximum number of queued buffers per channel
	 */
	int refreshAndGetMax() {
		int max = 0;

		for (InputChannel channel : inputGate.getInputChannels().values()) {
			if (channel instanceof RemoteInputChannel) {
				RemoteInputChannel rc = (RemoteInputChannel) channel;

				int size = rc.unsynchronizedGetNumberOfQueuedBuffers();
				max = Math.max(max, size);
			}
		}

		return max;
	}

	/**
	 * Iterates over all input channels and collects the average number of queued buffers in a
	 * channel in a best-effort way.
	 *
	 * @return average number of queued buffers per channel
	 */
	float refreshAndGetAvg() {
		long total = 0;
		int count = 0;

		for (InputChannel channel : inputGate.getInputChannels().values()) {
			if (channel instanceof RemoteInputChannel) {
				RemoteInputChannel rc = (RemoteInputChannel) channel;

				int size = rc.unsynchronizedGetNumberOfQueuedBuffers();
				total += size;
				++count;
			}
		}

		return count == 0 ? 0 : total / (float) count;
	}

	// ------------------------------------------------------------------------
	//  Gauges to access the stats
	// ------------------------------------------------------------------------

	private Gauge<Long> getTotalQueueLenGauge() {
		return new Gauge<Long>() {
			@Override
			public Long getValue() {
				return refreshAndGetTotal();
			}
		};
	}

	private Gauge<Integer> getMinQueueLenGauge() {
		return new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return refreshAndGetMin();
			}
		};
	}

	private Gauge<Integer> getMaxQueueLenGauge() {
		return new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return refreshAndGetMax();
			}
		};
	}

	private Gauge<Float> getAvgQueueLenGauge() {
		return new Gauge<Float>() {
			@Override
			public Float getValue() {
				return refreshAndGetAvg();
			}
		};
	}

	// ------------------------------------------------------------------------
	//  Static access
	// ------------------------------------------------------------------------

	public static void registerQueueLengthMetrics(MetricGroup parent, SingleInputGate[] gates) {
		for (int i = 0; i < gates.length; i++) {
			InputGateMetrics metrics = new InputGateMetrics(gates[i]);

			MetricGroup group = parent.addGroup(i);
			group.gauge("totalQueueLen", metrics.getTotalQueueLenGauge());
			group.gauge("minQueueLen", metrics.getMinQueueLenGauge());
			group.gauge("maxQueueLen", metrics.getMaxQueueLenGauge());
			group.gauge("avgQueueLen", metrics.getAvgQueueLenGauge());
		}
	}
}
