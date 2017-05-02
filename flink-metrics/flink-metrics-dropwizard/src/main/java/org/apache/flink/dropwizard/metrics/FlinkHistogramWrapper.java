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

package org.apache.flink.dropwizard.metrics;

import org.apache.flink.metrics.Histogram;

import com.codahale.metrics.Snapshot;

/**
 * Wrapper to use a Flink {@link Histogram} as a Dropwizard {@link com.codahale.metrics.Histogram}.
 * This is necessary to report Flink's histograms via the Dropwizard
 * {@link com.codahale.metrics.Reporter}.
 */
public class FlinkHistogramWrapper extends com.codahale.metrics.Histogram {

	private final Histogram histogram;

	public FlinkHistogramWrapper(Histogram histogram) {
		super(null);
		this.histogram = histogram;
	}

	@Override
	public void update(long value) {
		histogram.update(value);
	}

	@Override
	public long getCount() {
		return histogram.getCount();
	}

	@Override
	public Snapshot getSnapshot() {
		return new HistogramStatisticsWrapper(histogram.getStatistics());
	}
}
