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

import org.apache.flink.metrics.NumberGauge;

/**
 * A wrapper that allows a Flink {@link NumberGauge} to be used as a DropWizard number.
 */
public class FlinkNumberGaugeWrapper implements com.codahale.metrics.Gauge<Number> {

	private final NumberGauge gauge;

	private FlinkNumberGaugeWrapper(NumberGauge gauge) {
		this.gauge = gauge;
	}

	@Override
	public Number getValue() {
		return this.gauge.getNumberValue();
	}

	public static FlinkNumberGaugeWrapper fromGauge(NumberGauge gauge) {
		return new FlinkNumberGaugeWrapper(gauge);
	}
}
