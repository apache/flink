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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.reservoir.Snapshot;

/**
 * A Timer is a {@link org.apache.flink.metrics.Timer} that measures how long a certain piece of code takes to execute
 * and how often it is called.
 */
@PublicEvolving
public interface Timer extends Metric {
	/**
	 * Starts the timer.
	 */
	void start();

	/**
	 * Stops the timer.
	 */
	void stop();

	/**
	 * Returns the number of measurements made.
	 *
	 * @return current number of measurements made
	 */
	long getCount();

	/**
	 * Returns the mean rate at which measurements have occurred since this timer was created.
	 *
	 * @return the one-minute moving average rate
	 */
	double getMeanRate();

	/**
	 * Returns the one-minute moving average rate at which measurements have occurred since this timer was created.
	 *
	 * @return the one-minute moving average rate
	 */
	double getOneMinuteRate();

	/**
	 * Returns the five-minute moving average rate at which measurements have occurred since this timer was created.
	 *
	 * @return the five-minute moving average rate
	 */
	double getFiveMinuteRate();

	/**
	 * Returns the fifteen-minute moving average rate at which measurements have occurred since this timer was created.
	 *
	 * @return the fifteen-minute moving average rate
	 */
	double getFifteenMinuteRate();

	/**
	 * Creates a snapshot of the underlying histogram.
	 *
	 * @return snapshot of this histogram.
	 */
	Snapshot createSnapshot();
}
