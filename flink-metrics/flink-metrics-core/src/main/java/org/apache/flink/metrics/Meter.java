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
 * Metric for measuring average throughput.
 */
public interface Meter extends Metric {

	/**
	 * Mark occurrence of an event.
	 */
	void markEvent();

	/**
	 * Mark occurrence of multiple events.
	 *
	 * @param n number of events occurred
	 */
	void markEvent(long n);

	/**
	 * Returns one-minute exponentially-weighted moving average rate.
	 *
	 * @return one-minute exponentially-weighted moving average rate
	 */
	double getOneMinuteRate();

	/**
	 * Returns five-minute exponentially-weighted moving average rate.
	 *
	 * @return five-minute exponentially-weighted moving average rate
	 */
	double getFiveMinuteRate();

	/**
	 * Returns fifteen-minute exponentially-weighted moving average rate.
	 *
	 * @return fifteen-minute exponentially-weighted moving average rate
	 */
	double getFifteenMinuteRate();

	/**
	 * Return mean rate at which events has occurred.
	 *
	 * @return mean rate at which events has occurred
	 */
	double getMeanRate();

	/**
	 * Get number of events marked on the meter.
	 *
	 * @return number of events marked on the meter
	 */
	long getCount();
}
