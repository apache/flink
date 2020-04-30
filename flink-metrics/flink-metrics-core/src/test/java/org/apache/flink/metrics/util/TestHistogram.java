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

package org.apache.flink.metrics.util;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

/**
 * Stateless test histogram for which all methods return a static value.
 */
public class TestHistogram implements Histogram {
	private long count = 1;
	private int size = 3;
	private double mean = 4;
	private double stdDev = 5;
	private long max = 6;
	private long min = 7;
	@Override
	public void update(long value) {
	}

	@Override
	public long getCount() {
		return count;
	}

	@Override
	public HistogramStatistics getStatistics() {
		return new HistogramStatistics() {
			@Override
			public double getQuantile(double quantile) {
				return quantile;
			}

			@Override
			public long[] getValues() {
				return new long[0];
			}

			@Override
			public int size() {
				return size;
			}

			@Override
			public double getMean() {
				return mean;
			}

			@Override
			public double getStdDev() {
				return stdDev;
			}

			@Override
			public long getMax() {
				return max;
			}

			@Override
			public long getMin() {
				return min;
			}
		};
	}

	public void setSize(int size) {
		this.size = size;
	}

	public void setMean(double mean) {
		this.mean = mean;
	}

	public void setStdDev(double stdDev) {
		this.stdDev = stdDev;
	}

	public void setMax(long max) {
		this.max = max;
	}

	public void setMin(long min) {
		this.min = min;
	}

	public void setCount(long count) {
		this.count = count;
	}
}
