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

	private LeaderContender contender;
	private boolean hasLeadership = false;
	private CompletableFuture<UUID> confirmationFuture = null;
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
	public synchronized void start(LeaderContender contender) throws Exception {
		this.contender = contender;
	}

	@Override
	public synchronized void stop() throws Exception {

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
		contender.grantLeadership(leaderSessionID);

		return confirmationFuture;
	}

	public synchronized void notLeader() {
		hasLeadership = false;
		contender.revokeLeadership();
	}

	public synchronized void reset() {
		contender = null;
		hasLeadership  = false;
	}

	public synchronized String getAddress() {
		return contender.getAddress();
	}

	/**
	 * Returns <code>true</code> if {@link #start(LeaderContender)} was called,
	 * <code>false</code> otherwise.
	 */
	public synchronized boolean isStarted() {
		return contender != null;
	}

}
