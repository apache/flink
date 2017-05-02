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

import org.apache.flink.metrics.Meter;

import com.codahale.metrics.Clock;

/**
 * Wrapper to use a Flink {@link Meter} as a Dropwizard {@link com.codahale.metrics.Meter}.
 * This is necessary to report Flink's meters via the Dropwizard
 * {@link com.codahale.metrics.Reporter}.
 */
public class FlinkMeterWrapper extends com.codahale.metrics.Meter {

	private final Meter meter;

	public FlinkMeterWrapper(Meter meter) {
		super();
		this.meter = meter;
	}

	public FlinkMeterWrapper(Meter meter, Clock clock) {
		super(clock);
		this.meter = meter;
	}

	@Override
	public void mark() {
		meter.markEvent();
	}

	@Override
	public void mark(long n) {
		meter.markEvent(n);
	}

	@Override
	public long getCount() {
		return meter.getCount();
	}

	@Override
	public double getOneMinuteRate() {
		return meter.getRate();
	}

	@Override
	public double getFiveMinuteRate() {
		return 0;
	}

	@Override
	public double getFifteenMinuteRate() {
		return 0;
	}

	@Override
	public double getMeanRate() {
		return 0;
	}
}
