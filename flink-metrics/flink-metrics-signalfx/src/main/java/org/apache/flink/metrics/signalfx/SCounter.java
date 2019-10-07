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

package org.apache.flink.metrics.signalfx;

import org.apache.flink.metrics.Counter;

import com.signalfx.metrics.protobuf.SignalFxProtocolBuffers;
import com.signalfx.metrics.protobuf.SignalFxProtocolBuffers.DataPoint;
import com.signalfx.metrics.protobuf.SignalFxProtocolBuffers.MetricType;

import java.util.ArrayList;

/**
 * A wrapper that allows a Flink counter to be used as a SignalFX counter.
 */
public class SCounter extends SMetric {
	private final Counter counter;

	public SCounter(Counter counter, String metricName, ArrayList<SignalFxProtocolBuffers.Dimension> dimensions) {
		super(MetricType.CUMULATIVE_COUNTER, metricName, dimensions);
		this.counter = counter;
	}

	@Override
	public Number getMetricValue() {
		return this.counter.getCount();
	}

	@Override
	public DataPoint getDataPoint() {
		return DataPoint.newBuilder()
			.setMetric(this.metricName)
			.addAllDimensions(this.dimensions)
			.setValue(SignalFxProtocolBuffers.Datum.newBuilder().setIntValue(this.counter.getCount()))
			.build();
	}
}
