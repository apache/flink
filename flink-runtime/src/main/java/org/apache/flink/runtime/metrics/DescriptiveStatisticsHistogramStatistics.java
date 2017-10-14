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

package org.apache.flink.runtime.metrics;

import org.apache.flink.metrics.HistogramStatistics;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.Arrays;

/**
 * DescriptiveStatistics histogram statistics implementation returned by {@link DescriptiveStatisticsHistogram}.
 * The statistics class wraps a {@link DescriptiveStatistics} instance and forwards the method calls accordingly.
 */
public class DescriptiveStatisticsHistogramStatistics extends HistogramStatistics {
	private final DescriptiveStatistics descriptiveStatistics;

	public DescriptiveStatisticsHistogramStatistics(DescriptiveStatistics latencyHistogram) {
		this.descriptiveStatistics = latencyHistogram;
	}

	@Override
	public double getQuantile(double quantile) {
		return descriptiveStatistics.getPercentile(quantile * 100);
	}

	@Override
	public long[] getValues() {
		return Arrays.stream(descriptiveStatistics.getValues()).mapToLong(i -> (long) i).toArray();
	}

	@Override
	public int size() {
		return (int) descriptiveStatistics.getN();
	}

	@Override
	public double getMean() {
		return descriptiveStatistics.getMean();
	}

	@Override
	public double getStdDev() {
		return descriptiveStatistics.getStandardDeviation();
	}

	@Override
	public long getMax() {
		return (long) descriptiveStatistics.getMax();
	}

	@Override
	public long getMin() {
		return (long) descriptiveStatistics.getMin();
	}
}
