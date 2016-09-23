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

package org.apache.flink.runtime.rpc.akka.messages;

import org.apache.flink.runtime.rpc.RpcGateway;

import java.io.Serializable;

/**
 * Base class for watch operation
 */
public abstract class WatchOperation implements Serializable {

	private static final long serialVersionUID = 2018682807453586774L;

	/** the rpcGateway to take watch operation on */
	private final transient RpcGateway rpcGateway;

	/** timeout for look service on rpcGateway address in millisecond */
	private final long timeoutInMillis;

	/**
	 * Constructs a base watch operation
	 *
	 * @param rpcGateway      the rpcGateway to take watch operation on
	 * @param timeoutInMillis timeout for look service on rpcGateway address in millisecond
	 */
	public WatchOperation(RpcGateway rpcGateway, long timeoutInMillis) {
		this.rpcGateway = rpcGateway;
		this.timeoutInMillis = timeoutInMillis;
	}

	/**
	 * Gets the rpcGateway to take watch operation on
	 *
	 * @return the rpcGateway to take watch operation on
	 */
	public RpcGateway getRpcGateway() {
		return rpcGateway;
	}

	/**
	 * Gets timeout for connect rpcGateway address at first time in millisecond
	 *
	 * @return timeout for look service on rpcGateway address in millisecond
	 */
	public long getTimeoutInMillis() {
		return timeoutInMillis;
	}

	public static final class AddWatch extends WatchOperation {

		private static final long serialVersionUID = 1287429984556496379L;

		public AddWatch(RpcGateway rpcGateway, long timeoutInMillis) {
			super(rpcGateway, timeoutInMillis);
		}

	}

	public static final class RemoveWatch extends WatchOperation {

		private static final long serialVersionUID = -3120037314548359084L;

		public RemoveWatch(RpcGateway rpcGateway, long timeoutInMillis) {
			super(rpcGateway, timeoutInMillis);
		}

	}
}

