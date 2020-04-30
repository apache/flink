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

package org.apache.flink.client.program.rest.retry;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link WaitStrategy} with exponentially increasing sleep time.
 */
public class ExponentialWaitStrategy implements WaitStrategy {

	private final long initialWait;

	private final long maxWait;

	public ExponentialWaitStrategy(final long initialWait, final long maxWait) {
		checkArgument(initialWait > 0, "initialWait must be positive, was %s", initialWait);
		checkArgument(maxWait > 0, "maxWait must be positive, was %s", maxWait);
		checkArgument(initialWait <= maxWait, "initialWait must be lower than or equal to maxWait", maxWait);
		this.initialWait = initialWait;
		this.maxWait = maxWait;
	}

	@Override
	public long sleepTime(final long attempt) {
		checkArgument(attempt >= 0, "attempt must not be negative (%s)", attempt);
		final long exponentialSleepTime = initialWait * Math.round(Math.pow(2, attempt));
		return exponentialSleepTime >= 0 && exponentialSleepTime < maxWait ? exponentialSleepTime : maxWait;
	}
}
