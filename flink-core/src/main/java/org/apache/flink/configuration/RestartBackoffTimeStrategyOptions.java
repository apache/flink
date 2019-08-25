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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Configuration options for the RestartBackoffTimeStrategy.
 */
@PublicEvolving
public class RestartBackoffTimeStrategyOptions {
	/**
	 * Maximum number of failures in given time interval {@link #RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL}
	 * before failing a job in FailureRateRestartBackoffTimeStrategy.
	 */
	@PublicEvolving
	public static final ConfigOption<Integer> RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL = ConfigOptions
		.key("restart-backoff-time-strategy.failure-rate.max-failures-per-interval")
		.defaultValue(1)
		.withDescription("Maximum number of failures in given time interval before failing a job.");

	/**
	 * Time interval in which greater amount of failures than {@link #RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL}
	 * causes job fail in FailureRateRestartBackoffTimeStrategy.
	 */
	@PublicEvolving
	public static final ConfigOption<Long> RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL = ConfigOptions
		.key("restart-backoff-time-strategy.failure-rate.failure-rate-interval")
		.defaultValue(60_000L)
		.withDescription("Time interval in milliseconds for measuring failure rate.");

	/**
	 * Backoff time between two consecutive restart attempts in FailureRateRestartBackoffTimeStrategy.
	 */
	@PublicEvolving
	public static final ConfigOption<Long> RESTART_BACKOFF_TIME_STRATEGY_FAILURE_RATE_FAILURE_RATE_BACKOFF_TIME = ConfigOptions
		.key("restart-backoff-time-strategy.failure-rate.backoff-time")
		.defaultValue(0L)
		.withDescription("Backoff time in milliseconds between two consecutive restart attempts.");

	/**
	 * Maximum number of attempts the fixed delay restart strategy will try before failing a job.
	 */
	@PublicEvolving
	public static final ConfigOption<Integer> RESTART_BACKOFF_TIME_STRATEGY_FIXED_DELAY_ATTEMPTS = ConfigOptions
		.key("restart-backoff-time-strategy.fixed-delay.attempts")
		.defaultValue(Integer.MAX_VALUE)
		.withDescription("Maximum number of attempts the fixed delay restart strategy will try before failing a job.");

	/**
	 * Backoff time between two consecutive restart attempts in FixedDelayRestartBackoffTimeStrategy.
	 */
	@PublicEvolving
	public static final ConfigOption<Long> RESTART_BACKOFF_TIME_STRATEGY_FIXED_DELAY_BACKOFF_TIME = ConfigOptions
		.key("restart-backoff-time-strategy.fixed-delay.backoff-time")
		.defaultValue(0L)
		.withDescription("Backoff time in milliseconds between two consecutive restart attempts.");
}
