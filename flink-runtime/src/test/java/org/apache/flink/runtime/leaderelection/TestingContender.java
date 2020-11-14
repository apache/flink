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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * {@link LeaderContender} implementation which provides some convenience functions for testing
 * purposes.
 */
public class TestingContender extends TestingLeaderBase implements LeaderContender {
	private static final Logger LOG = LoggerFactory.getLogger(TestingContender.class);

	private final String address;
	private final LeaderElectionService leaderElectionService;

	private UUID leaderSessionID = null;

	public TestingContender(
			final String address,
			final LeaderElectionService leaderElectionService) {
		this.address = address;
		this.leaderElectionService = leaderElectionService;
	}

	@Override
	public void grantLeadership(UUID leaderSessionID) {
		LOG.debug("Was granted leadership with session ID {}.", leaderSessionID);

		this.leaderSessionID = leaderSessionID;

		leaderElectionService.confirmLeadership(leaderSessionID, address);

		leaderEventQueue.offer(LeaderInformation.known(leaderSessionID, address));
	}

	@Override
	public void revokeLeadership() {
		LOG.debug("Was revoked leadership. Old session ID {}.", leaderSessionID);

		leaderSessionID = null;
		leaderEventQueue.offer(LeaderInformation.empty());
	}

	@Override
	public String getDescription() {
		return address;
	}

	@Override
	public void handleError(Exception exception) {
		super.handleError(exception);
	}

	public UUID getLeaderSessionID() {
		return leaderSessionID;
	}
}
