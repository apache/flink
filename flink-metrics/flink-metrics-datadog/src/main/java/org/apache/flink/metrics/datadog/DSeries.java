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

package org.apache.flink.metrics.datadog;

import java.util.ArrayList;
import java.util.List;

/**
 * Json serialization between Flink and Datadog.
 */
public class DSeries {
	/**
	 * Names of series field and its getters must not be changed
	 * since they are mapped to json objects in a Datadog-defined format.
	 */
	private final List<DMetric> series;

	public DSeries() {
		series = new ArrayList<>();
	}

	public DSeries(List<DMetric> series) {
		this.series = series;
	}

	public void addGauge(DGauge gauge) {
		series.add(gauge);
	}

	public void addCounter(DCounter counter) {
		series.add(counter);
	}

	public void addMeter(DMeter meter) {
		series.add(meter);
	}

	public List<DMetric> getSeries() {
		return series;
	}
}
