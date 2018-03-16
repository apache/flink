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

package org.apache.flink.queryablestate.network.stats;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Atomic {@link KvStateRequestStats} implementation.
 */
public class AtomicKvStateRequestStats implements KvStateRequestStats {

	/**
	 * Number of active connections.
	 */
	private final AtomicLong numConnections = new AtomicLong();

	/**
	 * Total number of reported requests.
	 */
	private final AtomicLong numRequests = new AtomicLong();

	/**
	 * Total number of successful requests (<= reported requests).
	 */
	private final AtomicLong numSuccessful = new AtomicLong();

	/**
	 * Total duration of all successful requests.
	 */
	private final AtomicLong successfulDuration = new AtomicLong();

	/**
	 * Total number of failed requests (<= reported requests).
	 */
	private final AtomicLong numFailed = new AtomicLong();

	@Override
	public void reportActiveConnection() {
		numConnections.incrementAndGet();
	}

	@Override
	public void reportInactiveConnection() {
		numConnections.decrementAndGet();
	}

	@Override
	public void reportRequest() {
		numRequests.incrementAndGet();
	}

	@Override
	public void reportSuccessfulRequest(long durationTotalMillis) {
		numSuccessful.incrementAndGet();
		successfulDuration.addAndGet(durationTotalMillis);
	}

	@Override
	public void reportFailedRequest() {
		numFailed.incrementAndGet();
	}

	public long getNumConnections() {
		return numConnections.get();
	}

	public long getNumRequests() {
		return numRequests.get();
	}

	public long getNumSuccessful() {
		return numSuccessful.get();
	}

	public long getNumFailed() {
		return numFailed.get();
	}

	@Override
	public String toString() {
		return "AtomicKvStateRequestStats{" +
				"numConnections=" + numConnections +
				", numRequests=" + numRequests +
				", numSuccessful=" + numSuccessful +
				", numFailed=" + numFailed +
				'}';
	}
}
