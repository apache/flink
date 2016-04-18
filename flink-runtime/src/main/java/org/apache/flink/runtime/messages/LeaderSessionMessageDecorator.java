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

package org.apache.flink.runtime.messages;

import java.util.UUID;

/**
 * Message decorator which wraps message which implement {@link RequiresLeaderSessionID} into
 * a {@link org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage}.
 */
public class LeaderSessionMessageDecorator implements MessageDecorator {

	private static final long serialVersionUID = 5359618147408392706L;
	
	/** Leader session ID with which the RequiresLeaderSessionID messages will be decorated */
	private final UUID leaderSessionID;

	/**
	 * Sets the leader session ID with which the messages will be decorated.
	 *
	 * @param leaderSessionID Leader session ID to be used for decoration
	 */
	public LeaderSessionMessageDecorator(UUID leaderSessionID) {
		this.leaderSessionID = leaderSessionID;
	}

	@Override
	public Object decorate(Object message) {
		if (message instanceof RequiresLeaderSessionID) {
			return new JobManagerMessages.LeaderSessionMessage(leaderSessionID, message);
		} else {
			return message;
		}
	}
}
