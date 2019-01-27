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

package org.apache.flink.runtime.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;

/**
 * A group of metrics to keep track of sum and count of recorded values.
 */
public class SumAndCount {
	private double sum;
	private Counter count;
	private Histogram histogram;

	public SumAndCount(String name, MetricGroup metricGroup) {
		MetricGroup group = metricGroup.addGroup(name);
		count = group.counter("count");
		group.gauge("sum", new Gauge<Double>() {
			@Override
			public Double getValue() {
				return sum;
			}
		});
		histogram = group.histogram("histogram", new SimpleHistogram());
	}

	/**
	 * Used only for testing purpose. Don't use in production!
	 */
	@VisibleForTesting
	public SumAndCount(String name) {
		sum = 0;
		count = new SimpleCounter();
		histogram = new SimpleHistogram();
	}

	public void update(long value) {
		count.inc();
		sum += value;
		histogram.update(value);
	}

	public double getSum() {
		return sum;
	}

	public Counter getCounter() {
		return count;
	}

	public Histogram getHistogram() {
		return histogram;
	}
}
