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

package org.apache.flink.runtime.leaderelection;

import javax.annotation.Nonnull;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Test {@link LeaderElectionService} implementation which directly forwards isLeader and notLeader
 * calls to the contender.
 */
public class TestingLeaderElectionService implements LeaderElectionService {

	private LeaderContender contender = null;
	private boolean hasLeadership = false;
	private CompletableFuture<UUID> confirmationFuture = null;
	private CompletableFuture<Void> startFuture = new CompletableFuture<>();
	private UUID issuedLeaderSessionId = null;

	/**
	 * Gets a future that completes when leadership is confirmed.
	 *
	 * <p>Note: the future is created upon calling {@link #isLeader(UUID)}.
	 */
	public synchronized CompletableFuture<UUID> getConfirmationFuture() {
		return confirmationFuture;
	}

	@Override
	public synchronized void start(LeaderContender contender) {
		assert(!getStartFuture().isDone());

		this.contender = contender;

		if (hasLeadership) {
			contender.grantLeadership(issuedLeaderSessionId);
		}

		startFuture.complete(null);
	}

	@Override
	public synchronized void stop() throws Exception {
		contender = null;
		hasLeadership = false;
		issuedLeaderSessionId = null;
		startFuture.cancel(false);
		startFuture = new CompletableFuture<>();
	}

	@Override
	public synchronized void confirmLeaderSessionID(UUID leaderSessionID) {
		if (confirmationFuture != null) {
			confirmationFuture.complete(leaderSessionID);
		}
	}

	@Override
	public synchronized boolean hasLeadership(@Nonnull UUID leaderSessionId) {
		return hasLeadership && leaderSessionId.equals(issuedLeaderSessionId);
	}

	public synchronized CompletableFuture<UUID> isLeader(UUID leaderSessionID) {
		if (confirmationFuture != null) {
			confirmationFuture.cancel(false);
		}
		confirmationFuture = new CompletableFuture<>();
		hasLeadership = true;
		issuedLeaderSessionId = leaderSessionID;

		if (contender != null) {
			contender.grantLeadership(leaderSessionID);
		}

		return confirmationFuture;
	}

	public synchronized void notLeader() {
		hasLeadership = false;

		if (contender != null) {
			contender.revokeLeadership();
		}
	}

	public synchronized String getAddress() {
		if (contender != null) {
			return contender.getAddress();
		} else {
			throw new IllegalStateException("TestingLeaderElectionService has not been started.");
		}
	}

	/**
	 * Returns the start future indicating whether this leader election service
	 * has been started or not.
	 *
	 * @return Future which is completed once this service has been started
	 */
	public synchronized CompletableFuture<Void> getStartFuture() {
		return startFuture;
	}

}
