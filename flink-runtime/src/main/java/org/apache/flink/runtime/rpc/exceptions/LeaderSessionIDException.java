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

package org.apache.flink.runtime.rpc.exceptions;

import java.util.UUID;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An exception specifying that the received leader session ID is not the same as expected.
 */
public class LeaderSessionIDException extends Exception {

	private static final long serialVersionUID = -3276145308053264636L;

	/** expected leader session id */
	private final UUID expectedLeaderSessionID;

	/** actual leader session id */
	private final UUID actualLeaderSessionID;

	public LeaderSessionIDException(UUID expectedLeaderSessionID, UUID actualLeaderSessionID) {
		super("Unmatched leader session ID : expected " + expectedLeaderSessionID + ", actual " + actualLeaderSessionID);
		this.expectedLeaderSessionID =  checkNotNull(expectedLeaderSessionID);
		this.actualLeaderSessionID = checkNotNull(actualLeaderSessionID);
	}

	/**
	 * Get expected leader session id
	 *
	 * @return expect leader session id
	 */
	public UUID getExpectedLeaderSessionID() {
		return expectedLeaderSessionID;
	}

	/**
	 * Get actual leader session id
	 *
	 * @return actual leader session id
	 */
	public UUID getActualLeaderSessionID() {
		return actualLeaderSessionID;
	}
}
