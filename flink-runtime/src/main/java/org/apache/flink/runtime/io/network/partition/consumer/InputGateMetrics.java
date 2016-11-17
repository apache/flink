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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class InputGateMetrics {

	private final SingleInputGate inputGate;

	private long lastTotal = -1;

	private int lastMin = -1;

	private int lastMax = -1;

	private float lastAvg = -1.0f;

	// ------------------------------------------------------------------------

	private InputGateMetrics(SingleInputGate inputGate) {
		this.inputGate = checkNotNull(inputGate);
	}

	// ------------------------------------------------------------------------

	// these methods are package private to make access from the nested classes faster 

	long refreshAndGetTotal() {
		long total;
		if ((total = lastTotal) == -1) {
			refresh();
			total = lastTotal;
		}

		lastTotal = -1;
		return total;
	}

	int refreshAndGetMin() {
		int min;
		if ((min = lastMin) == -1) {
			refresh();
			min = lastMin;
		}

		lastMin = -1;
		return min;
	}

	int refreshAndGetMax() {
		int max;
		if ((max = lastMax) == -1) {
			refresh();
			max = lastMax;
		}

		lastMax = -1;
		return max;
	}

	float refreshAndGetAvg() {
		float avg;
		if ((avg = lastAvg) < 0.0f) {
			refresh();
			avg = lastAvg;
		}

		lastAvg = -1.0f;
		return avg;
	}

	private void refresh() {
		long total = 0;
		int min = Integer.MAX_VALUE;
		int max = 0;
		int count = 0;

		for (InputChannel channel : inputGate.getInputChannels().values()) {
			if (channel.getClass() == RemoteInputChannel.class) {
				RemoteInputChannel rc = (RemoteInputChannel) channel;

				int size = rc.unsynchronizedGetNumberOfQueuedBuffers();
				total += size;
				min = Math.min(min, size);
				max = Math.max(max, size);
				count++;
			}
		}

		this.lastMin = min;
		this.lastMax = max;
		this.lastAvg = total / (float) count;
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

	public static void registerQueueLengthMetrics(MetricGroup group, SingleInputGate gate) {
		InputGateMetrics metrics = new InputGateMetrics(gate);

		group.gauge("total-queue-len", metrics.getTotalQueueLenGauge());
		group.gauge("min-queue-len", metrics.getMinQueueLenGauge());
		group.gauge("max-queue-len", metrics.getMaxQueueLenGauge());
		group.gauge("avg-queue-len", metrics.getAvgQueueLenGauge());
	}
}
