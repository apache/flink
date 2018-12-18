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

import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.UUID;

/**
 * Standalone implementation of the {@link LeaderElectionService} interface. The standalone
 * implementation assumes that there is only a single {@link LeaderContender} and thus directly
 * grants him the leadership upon start up. Furthermore, there is no communication needed between
 * multiple standalone leader election services.
 */
public class StandaloneLeaderElectionService implements LeaderElectionService {

	private LeaderContender contender = null;

	@Override
	public void start(LeaderContender newContender) throws Exception {
		if (contender != null) {
			// Service was already started
			throw new IllegalArgumentException("Leader election service cannot be started multiple times.");
		}

		contender = Preconditions.checkNotNull(newContender);

		// directly grant leadership to the given contender
		contender.grantLeadership(HighAvailabilityServices.DEFAULT_LEADER_ID);
	}

	@Override
	public void stop() {
		if (contender != null) {
			contender.revokeLeadership();
			contender = null;
		}
	}

	@Override
	public void confirmLeaderSessionID(UUID leaderSessionID) {}

	@Override
	public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
		return (contender != null && HighAvailabilityServices.DEFAULT_LEADER_ID.equals(leaderSessionId));
	}
}
