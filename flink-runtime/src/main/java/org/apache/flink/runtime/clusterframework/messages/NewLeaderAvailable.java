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

package org.apache.flink.runtime.clusterframework.messages;

import java.util.UUID;

/**
 * Message sent to the Flink resource manager to indicate that a new leader is available.
 */
public class NewLeaderAvailable implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	
	/** The leader Akka URL */
	private final String leaderAddress;
	
	/** The session ID for the leadership status */
	private final UUID leaderSessionId;
	
	public NewLeaderAvailable(String leaderAddress, UUID leaderSessionId) {
		this.leaderAddress = leaderAddress;
		this.leaderSessionId = leaderSessionId;
	}
	
	// ------------------------------------------------------------------------

	public String leaderAddress() {
		return leaderAddress;
	}
	
	public UUID leaderSessionId() {
		return leaderSessionId;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "NewLeaderAvailable (" + leaderAddress + " / " + leaderSessionId + ')';
	}
}
