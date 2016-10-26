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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

/**
 * Metric group that contains shareable pre-defined IO-related metrics. The metrics registration is
 * forwarded to the parent task metric group.
 */
public class TaskIOMetricGroup extends ProxyMetricGroup<TaskMetricGroup> {

	private final Counter numBytesOut;
	private final Counter numBytesInLocal;
	private final Counter numBytesInRemote;

	private final Meter numBytesInRateLocal;
	private final Meter numBytesInRateRemote;
	private final Meter numBytesOutRate;

	public TaskIOMetricGroup(TaskMetricGroup parent) {
		super(parent);

		this.numBytesOut = counter("numBytesOut");
		this.numBytesInLocal = counter("numBytesInLocal");
		this.numBytesInRemote = counter("numBytesInRemote");
		this.numBytesOutRate = meter("numBytesOutPerSecond", new MeterView(numBytesOut, 60));
		this.numBytesInRateLocal = meter("numBytesInLocalPerSecond", new MeterView(numBytesInLocal, 60));
		this.numBytesInRateRemote = meter("numBytesInRemotePerSecond", new MeterView(numBytesInRemote, 60));
	}

	public Counter getNumBytesOutCounter() {
		return numBytesOut;
	}

	public Counter getNumBytesInLocalCounter() {
		return numBytesInLocal;
	}

	public Counter getNumBytesInRemoteCounter() {
		return numBytesInRemote;
	}

	public Meter getNumBytesInRateLocalMeter() {
		return numBytesInRateLocal;
	}

	public Meter getNumBytesInRateRemoteMeter() {
		return numBytesInRateRemote;
	}

	public Meter getNumBytesOutRateMeter() {
		return numBytesOutRate;
	}
}
