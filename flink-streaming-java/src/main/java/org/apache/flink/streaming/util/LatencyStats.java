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

package org.apache.flink.streaming.util;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;

import java.util.HashMap;
import java.util.Map;

/**
 * The {@link LatencyStats} objects are used to track and report on the behavior of latencies across measurements.
 */
public class LatencyStats {
	private final Map<String, DescriptiveStatisticsHistogram> latencyStats = new HashMap<>();
	private final MetricGroup metricGroup;
	private final int historySize;

	public LatencyStats(MetricGroup metricGroup, int historySize) {
		this.metricGroup = metricGroup;
		this.historySize = historySize;
	}

	public void reportLatency(LatencyMarker marker, boolean isSink) {
		String latencyMetricName = identifyLatencySource(marker, !isSink);
		DescriptiveStatisticsHistogram latencyHistogram = this.latencyStats.get(latencyMetricName);
		if (latencyHistogram == null) {
			latencyHistogram = new DescriptiveStatisticsHistogram(this.historySize);
			this.latencyStats.put(latencyMetricName, latencyHistogram);
			this.metricGroup.histogram(latencyMetricName, latencyHistogram);
		}

		long now = System.currentTimeMillis();
		latencyHistogram.update(now - marker.getMarkedTime());
	}

	/**
	 * Creates an identifier for a latency source. from a given {@code LatencyMarker}.
	 *
	 * @param marker             The latency marker to extract the LatencySourceDescriptor from.
	 * @param ignoreSubtaskIndex Set to true to ignore the subtask index, to treat the latencies
	 *                           from all the parallel instances of a source as the same.
	 * @return A string that identifies the latency source.
	 */
	private String identifyLatencySource(LatencyMarker marker, boolean ignoreSubtaskIndex) {
		if (ignoreSubtaskIndex) {
			return String.valueOf(marker.getVertexID());
		} else {
			return marker.getVertexID() + "_" + marker.getSubtaskIndex();
		}
	}
}
