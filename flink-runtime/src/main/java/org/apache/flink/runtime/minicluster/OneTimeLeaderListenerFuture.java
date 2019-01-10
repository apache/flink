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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.runtime.leaderelection.LeaderAddressAndId;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A leader listener that exposes a future for the first leader notification.
 *
 * <p>The future can be obtained via the {@link #future()} method.
 */
public class OneTimeLeaderListenerFuture implements LeaderRetrievalListener {

	private final CompletableFuture<LeaderAddressAndId> future;

	public OneTimeLeaderListenerFuture() {
		this.future = new CompletableFuture<>();
	}

	/**
	 * Gets the future that is completed with the leader address and ID.
	 * @return The future.
	 */
	public CompletableFuture<LeaderAddressAndId> future() {
		return future;
	}

	// ------------------------------------------------------------------------

	@Override
	public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
		future.complete(new LeaderAddressAndId(leaderAddress, leaderSessionID));
	}

	@Override
	public void handleError(Exception exception) {
		future.completeExceptionally(exception);
	}
}
