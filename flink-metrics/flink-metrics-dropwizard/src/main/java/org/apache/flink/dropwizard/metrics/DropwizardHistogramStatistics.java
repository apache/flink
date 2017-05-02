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

import org.apache.flink.metrics.HistogramStatistics;

import com.codahale.metrics.Snapshot;

/**
 * Dropwizard histogram statistics implementation returned by {@link DropwizardHistogramWrapper}.
 * The statistics class wraps a {@link Snapshot} instance and forwards the method calls accordingly.
 */
class DropwizardHistogramStatistics extends HistogramStatistics {

	private final com.codahale.metrics.Snapshot snapshot;

	DropwizardHistogramStatistics(com.codahale.metrics.Snapshot snapshot) {
		this.snapshot = snapshot;
	}

	@Override
	public double getQuantile(double quantile) {
		return snapshot.getValue(quantile);
	}

	@Override
	public long[] getValues() {
		return snapshot.getValues();
	}

	@Override
	public int size() {
		return snapshot.size();
	}

	@Override
	public double getMean() {
		return snapshot.getMean();
	}

	@Override
	public double getStdDev() {
		return snapshot.getStdDev();
	}

	@Override
	public long getMax() {
		return snapshot.getMax();
	}

	@Override
	public long getMin() {
		return snapshot.getMin();
	}
}
