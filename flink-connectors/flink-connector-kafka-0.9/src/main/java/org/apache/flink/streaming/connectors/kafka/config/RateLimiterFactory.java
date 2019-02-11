/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.config;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;

import java.util.Properties;

/**
 * A RateLimiterFactory that configures and creates a rate limiter.
 */
public class RateLimiterFactory {

	/** Flag that indicates if ratelimiting is enabled. */
	private static final String RATELIMITING_FLAG = "consumer.ratelimiting.enabled";

	/** Max bytes per second that can be read by the consumer. */
	private static final String RATELIMITING_MAX_BYTES_PER_SECOND = "consumer.ratelimiting.maxbytespersecond";

	/** Delimiter for properties. */
	private static final String DELIMITER = ".";

	/** Default value for ratelimiting flag. */
	private static final boolean DEFAULT_USE_RATELIMITING = false;

	/** Prefix for the consumer. */
	private String consumerPrefix;

	/** Max bytes per second per consumer subtask. */
	private long localMaxBytesPerSecond;

	/** Flag to indicate if rate limiting is enabled. */
	private boolean rateLimitingEnabled;

	/** Max bytes per second consumer property. */
	private String maxBytesPerSecondProperty;

	/** Rate limiting flag consumer property. */
	private String useRateLimitingProperty;

	/**
	 * Configure the properties required to create a rate limiter.
	 *
	 * @param consumerPrefix Consumer name.
	 * @param runtimeContext Runtime context.
	 * @param properties Consumer properties.
	 */
	public void configure(String consumerPrefix,
		StreamingRuntimeContext runtimeContext, Properties properties) {
		this.consumerPrefix = consumerPrefix;
		this.useRateLimitingProperty = getUseRateLimitingProperty(consumerPrefix);
		this.maxBytesPerSecondProperty = getMaxBytesPerSecondProperty(consumerPrefix);
		this.rateLimitingEnabled = Boolean.valueOf(properties.getProperty(useRateLimitingProperty, String.valueOf(DEFAULT_USE_RATELIMITING)));
		if (rateLimitingEnabled) {
			long globalMaxBytesPerSecond = Long.valueOf(properties.getProperty(maxBytesPerSecondProperty));
			this.localMaxBytesPerSecond = globalMaxBytesPerSecond / runtimeContext.getNumberOfParallelSubtasks();
		}
	}

	/**
	 * A method to create a rate limiter with a rate of {{@link #localMaxBytesPerSecond}}.
	 * @return a RateLimiter.
	 */
	public RateLimiter createRateLimiter() {
		return RateLimiter.create(localMaxBytesPerSecond);
	}

	public String getConsumerPrefix() {
		return consumerPrefix;
	}

	public boolean isRateLimitingEnabled() {
		return rateLimitingEnabled;
	}

	public String getMaxBytesPerSecondProperty(String consumerPrefix) {
		return consumerPrefix.concat(DELIMITER).concat(RATELIMITING_MAX_BYTES_PER_SECOND);
	}

	public String getUseRateLimitingProperty(String consumerPrefix) {
		return consumerPrefix.concat(DELIMITER).concat(RATELIMITING_FLAG);
	}
}
