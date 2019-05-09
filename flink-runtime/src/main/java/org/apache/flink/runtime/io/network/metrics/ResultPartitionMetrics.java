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
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Collects metrics of a result partition.
 */
public class ResultPartitionMetrics {

	private final ResultPartition partition;

	// ------------------------------------------------------------------------

	private ResultPartitionMetrics(ResultPartition partition) {
		this.partition = checkNotNull(partition);
	}

	// ------------------------------------------------------------------------

	// these methods are package private to make access from the nested classes faster

	/**
	 * Iterates over all sub-partitions and collects the total number of queued buffers in a
	 * best-effort way.
	 *
	 * @return total number of queued buffers
	 */
	long refreshAndGetTotal() {
		long total = 0;

		for (ResultSubpartition part : partition.getAllPartitions()) {
			total += part.unsynchronizedGetNumberOfQueuedBuffers();
		}

		return total;
	}

	/**
	 * Iterates over all sub-partitions and collects the minimum number of queued buffers in a
	 * sub-partition in a best-effort way.
	 *
	 * @return minimum number of queued buffers per sub-partition (<tt>0</tt> if sub-partitions exist)
	 */
	int refreshAndGetMin() {
		int min = Integer.MAX_VALUE;

		ResultSubpartition[] allPartitions = partition.getAllPartitions();
		if (allPartitions.length == 0) {
			// meaningful value when no channels exist:
			return 0;
		}

		for (ResultSubpartition part : allPartitions) {
			int size = part.unsynchronizedGetNumberOfQueuedBuffers();
			min = Math.min(min, size);
		}

		return min;
	}

	/**
	 * Iterates over all sub-partitions and collects the maximum number of queued buffers in a
	 * sub-partition in a best-effort way.
	 *
	 * @return maximum number of queued buffers per sub-partition
	 */
	int refreshAndGetMax() {
		int max = 0;

		for (ResultSubpartition part : partition.getAllPartitions()) {
			int size = part.unsynchronizedGetNumberOfQueuedBuffers();
			max = Math.max(max, size);
		}

		return max;
	}

	/**
	 * Iterates over all sub-partitions and collects the average number of queued buffers in a
	 * sub-partition in a best-effort way.
	 *
	 * @return average number of queued buffers per sub-partition
	 */
	float refreshAndGetAvg() {
		long total = 0;

		ResultSubpartition[] allPartitions = partition.getAllPartitions();
		for (ResultSubpartition part : allPartitions) {
			int size = part.unsynchronizedGetNumberOfQueuedBuffers();
			total += size;
		}

		return total / (float) allPartitions.length;
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

	public static void registerQueueLengthMetrics(MetricGroup parent, ResultPartition[] partitions) {
		for (int i = 0; i < partitions.length; i++) {
			ResultPartitionMetrics metrics = new ResultPartitionMetrics(partitions[i]);

			MetricGroup group = parent.addGroup(i);
			group.gauge("totalQueueLen", metrics.getTotalQueueLenGauge());
			group.gauge("minQueueLen", metrics.getMinQueueLenGauge());
			group.gauge("maxQueueLen", metrics.getMaxQueueLenGauge());
			group.gauge("avgQueueLen", metrics.getAvgQueueLenGauge());
		}
	}
}
