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
package org.apache.flink.metrics.reservoir.dropwizard;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.reservoir.Snapshot;

/**
 * Wrapper that allows Flink reporters to use {@link com.codahale.metrics.Snapshot}.
 */
@Internal
public class DropwizardToFlinkSnapshotWrapper implements Snapshot {
	private final com.codahale.metrics.Snapshot snapshot;

	public DropwizardToFlinkSnapshotWrapper(com.codahale.metrics.Snapshot snapshot) {
		this.snapshot = snapshot;
	}

	@Override
	public double getValue(double v) {
		return this.snapshot.getValue(v);
	}

	@Override
	public long[] getValues() {
		return this.snapshot.getValues();
	}

	@Override
	public int size() {
		return this.snapshot.size();
	}

	@Override
	public long getMax() {
		return this.snapshot.getMax();
	}

	@Override
	public double getMean() {
		return this.snapshot.getMean();
	}

	@Override
	public long getMin() {
		return this.snapshot.getMin();
	}

	@Override
	public double getStdDev() {
		return this.snapshot.getStdDev();
	}

	@Override
	public double getMedian() {
		return getValue(0.5);
	}

	@Override
	public double get75thPercentile() {
		return getValue(0.75);
	}

	@Override
	public double get95thPercentile() {
		return getValue(0.95);
	}

	@Override
	public double get98thPercentile() {
		return getValue(0.98);
	}

	@Override
	public double get99thPercentile() {
		return getValue(0.99);
	}

	@Override
	public double get999thPercentile() {
		return getValue(0.999);
	}
}
