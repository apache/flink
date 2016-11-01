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

package org.apache.flink.runtime.taskmanager.heartbeat.messages;

import org.apache.flink.runtime.messages.RequiresLeaderSessionID;

import java.io.Serializable;

/**
 * Response to a {@link Heartbeat}.
 */
public class HeartbeatResponse implements Serializable, RequiresLeaderSessionID {
	private static final long serialVersionUID = 5395932416824920529L;

	private static final HeartbeatResponse INSTANCE = new HeartbeatResponse();

	public static HeartbeatResponse getInstance() {
		return INSTANCE;
	}

	private HeartbeatResponse() {}
}
