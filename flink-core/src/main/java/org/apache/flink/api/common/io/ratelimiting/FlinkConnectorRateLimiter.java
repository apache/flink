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

package org.apache.flink.api.common.io.ratelimiting;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;
/**
 * An interface to create a ratelimiter
 *
 * <p>The ratelimiter is configured via {@link #setRate(long)} and
 * created via {@link #open(RuntimeContext)}.
 * An example implementation can be found {@link GuavaFlinkConnectorRateLimiter}.
 * */

@PublicEvolving
public interface FlinkConnectorRateLimiter extends Serializable {

	/**
	 * A method that can be used to create and configure a ratelimiter
	 * based on the runtimeContext.
	 * @param runtimeContext
	 */
	void open(RuntimeContext runtimeContext);

	/**
	 * Sets the desired rate for the rate limiter.
	 * @param rate
	 */
	void setRate(long rate);

	/**
	 * Acquires permits for the rate limiter.
	 */
	void acquire(int permits);

	long getRate();

	void close();
}
