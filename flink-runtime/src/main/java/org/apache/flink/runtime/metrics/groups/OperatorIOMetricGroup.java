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
 * forwarded to the parent operator metric group.
 */
public class OperatorIOMetricGroup extends ProxyMetricGroup<OperatorMetricGroup> {

	private final Counter numRecordsIn;
	private final Counter numRecordsOut;

	private final Meter numRecordsInRate;
	private final Meter numRecordsOutRate;

	public OperatorIOMetricGroup(OperatorMetricGroup parentMetricGroup) {
		super(parentMetricGroup);
		numRecordsIn = parentMetricGroup.counter("numRecordsIn");
		numRecordsOut = parentMetricGroup.counter("numRecordsOut");
		numRecordsInRate = parentMetricGroup.meter("numRecordsInPerSecond", new MeterView(numRecordsIn, 60));
		numRecordsOutRate = parentMetricGroup.meter("numRecordsOutPerSecond", new MeterView(numRecordsOut, 60));
	}

	public Counter getNumRecordsInCounter() {
		return numRecordsIn;
	}

	public Counter getNumRecordsOutCounter() {
		return numRecordsOut;
	}

	public Meter getNumRecordsInRateMeter() {
		return numRecordsInRate;
	}

	public Meter getNumRecordsOutRate() {
		return numRecordsOutRate;
	}
}
