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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class ResultPartitionMetrics {

	private final ResultPartition partition;

	private int lastMin = -1;

	private int lastMax = -1;

	private float lastAvg = -1.0f;

	// ------------------------------------------------------------------------

	private ResultPartitionMetrics(ResultPartition partition) {
		this.partition = checkNotNull(partition);
	}

	// ------------------------------------------------------------------------

	// these methods are package private to make access from the nested classes faster 

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
		int min = Integer.MAX_VALUE;
		int max = 0;

		for (ResultSubpartition part : partition.getAllPartitions()) {
			int size = part.unsynchronizedGetNumberOfQueuedBuffers();
			min = Math.min(min, size);
			max = Math.max(max, size);
		}

		this.lastMin = min;
		this.lastMax = max;
		this.lastAvg = partition.getTotalNumberOfBuffers() / (float) partition.getNumberOfSubpartitions();
	}

	// ------------------------------------------------------------------------
	//  Gauges to access the stats
	// ------------------------------------------------------------------------

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

	public static void registerQueueLengthMetrics(MetricGroup group, ResultPartition partition) {
		ResultPartitionMetrics metrics = new ResultPartitionMetrics(partition);

		group.gauge("min-queue-len", metrics.getMinQueueLenGauge());
		group.gauge("max-queue-len", metrics.getMaxQueueLenGauge());
		group.gauge("avg-queue-len", metrics.getAvgQueueLenGauge());
	}
}
