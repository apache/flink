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

package org.apache.flink.runtime.taskexecutor;

import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Simple configuration object for the parameters for the server-side of queryable state.
 */
public class QueryableStateConfiguration {

	private final boolean enabled;

	private final Iterator<Integer> proxyPortRange;

	private final Iterator<Integer> qserverPortRange;

	private final int numServerThreads;

	private final int numQueryThreads;

	public QueryableStateConfiguration(
			boolean enabled,
			Iterator<Integer> proxyPortRange,
			Iterator<Integer> qserverPortRange,
			int numServerThreads,
			int numQueryThreads) {

		checkArgument(!enabled || (proxyPortRange != null && proxyPortRange.hasNext()));
		checkArgument(!enabled || (qserverPortRange != null && qserverPortRange.hasNext()));
		checkArgument(numServerThreads >= 0, "queryable state number of server threads must be zero or larger");
		checkArgument(numQueryThreads >= 0, "queryable state number of query threads must be zero or larger");

		this.enabled = enabled;
		this.proxyPortRange = proxyPortRange;
		this.qserverPortRange = qserverPortRange;
		this.numServerThreads = numServerThreads;
		this.numQueryThreads = numQueryThreads;
	}

	// ------------------------------------------------------------------------

	/**
	 * Returns whether queryable state is enabled.
	 */
	public boolean isEnabled() {
		return enabled;
	}

	/**
	 * Returns the port range where the queryable state client proxy can listen.
	 * See {@link org.apache.flink.configuration.QueryableStateOptions#PROXY_PORT_RANGE QueryableStateOptions.PROXY_PORT_RANGE}.
	 */
	public Iterator<Integer> getProxyPortRange() {
		return proxyPortRange;
	}

	/**
	 * Returns the port range where the queryable state client proxy can listen.
	 * See {@link org.apache.flink.configuration.QueryableStateOptions#SERVER_PORT_RANGE QueryableStateOptions.SERVER_PORT_RANGE}.
	 */
	public Iterator<Integer> getStateServerPortRange() {
		return qserverPortRange;
	}

	/**
	 * Returns the number of threads for the query server NIO event loop.
	 * These threads only process network events and dispatch query requests to the query threads.
	 */
	public int numServerThreads() {
		return numServerThreads;
	}

	/**
	 * Returns the number of threads for the thread pool that performs the actual state lookup.
	 * These threads perform the actual state lookup.
	 */
	public int numQueryThreads() {
		return numQueryThreads;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "QueryableStateConfiguration {" +
				"enabled=" + enabled +
				", numServerThreads=" + numServerThreads +
				", numQueryThreads=" + numQueryThreads +
				'}';
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the configuration describing the queryable state as deactivated.
	 */
	public static QueryableStateConfiguration disabled() {
		return new QueryableStateConfiguration(false, null, null, 0, 0);
	}
}
