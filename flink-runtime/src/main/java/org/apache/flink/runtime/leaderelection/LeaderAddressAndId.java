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

import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A combination of a leader address and leader id.
 */
public class LeaderAddressAndId {

	private final String leaderAddress;
	private final UUID leaderId;

	public LeaderAddressAndId(String leaderAddress, UUID leaderId) {
		this.leaderAddress = checkNotNull(leaderAddress);
		this.leaderId = checkNotNull(leaderId);
	}

	// ------------------------------------------------------------------------

	public String leaderAddress() {
		return leaderAddress;
	}

	public UUID leaderId() {
		return leaderId;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return 31 * leaderAddress.hashCode()+ leaderId.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o != null && o.getClass() == LeaderAddressAndId.class) {
			final LeaderAddressAndId that = (LeaderAddressAndId) o;
			return this.leaderAddress.equals(that.leaderAddress) && this.leaderId.equals(that.leaderId);
		}
		else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "LeaderAddressAndId (" + leaderAddress + " / " + leaderId + ')';
	}
}
