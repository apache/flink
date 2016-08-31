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
package org.apache.flink.metrics;

/**
 * A MeterView provides a rate of events per second over a given time period. The events are counted by a {@link Counter}.
 * A history of measurements is maintained from which the rate of events is calculated on demand.
 */
public class MeterView implements Meter, View {
	private final Counter counter;
	private final int timeSpanInSeconds;
	private final long[] values;
	private int time = 0;

	private boolean updateRate = false;
	private double currentRate = 0;

	public MeterView(Counter counter, int timeSpanInSeconds) {
		this.counter = counter;
		this.timeSpanInSeconds = timeSpanInSeconds - (timeSpanInSeconds % UPDATE_INTERVAL_SECONDS);
		this.values = new long[this.timeSpanInSeconds / UPDATE_INTERVAL_SECONDS + 1];
	}

	@Override
	public void markEvent() {
		this.counter.inc();
	}

	@Override
	public void markEvent(long n) {
		this.counter.inc(n);
	}

	@Override
	public long getCount() {
		return counter.getCount();
	}

	@Override
	public double getRate() {
		if (updateRate) {
			final int time = this.time;
			currentRate =  ((double) (values[time] - values[(time + 1) % values.length]) / timeSpanInSeconds);
			updateRate = false;
		}
		return currentRate;
	}

	@Override
	public void update() {
		time = (time + 1) % values.length;
		values[time] = counter.getCount();
		updateRate = true;
	}
}
