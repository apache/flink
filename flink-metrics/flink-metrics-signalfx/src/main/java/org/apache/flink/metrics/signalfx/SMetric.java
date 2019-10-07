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

import com.signalfx.metrics.protobuf.SignalFxProtocolBuffers.DataPoint;
import com.signalfx.metrics.protobuf.SignalFxProtocolBuffers.Dimension;
import com.signalfx.metrics.protobuf.SignalFxProtocolBuffers.MetricType;

import java.util.ArrayList;

/**
 * Abstract metric of SignalFX.
 */
public abstract class SMetric {

	protected final String metricName;
	protected final MetricType metricType;
	protected final ArrayList<Dimension> dimensions;

	public SMetric(MetricType metricType, String metricName, ArrayList<Dimension> dimensions) {
		this.metricName = metricName;
		this.metricType = metricType;
		this.dimensions = dimensions;
	}

	public abstract Number getMetricValue();

	public abstract DataPoint getDataPoint();
}
