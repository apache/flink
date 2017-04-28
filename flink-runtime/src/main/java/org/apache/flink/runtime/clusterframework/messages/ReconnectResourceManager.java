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

import akka.actor.ActorRef;
import org.apache.flink.runtime.messages.RequiresLeaderSessionID;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * This message signals that the ResourceManager should reconnect to the JobManager. It is processed
 * by the JobManager if it fails to register resources with the ResourceManager. The JobManager wants
 * the ResourceManager to go through the reconciliation phase to sync up with the JobManager bookkeeping.
 * This is done by forcing the ResourceManager to reconnect.
 */
public class ReconnectResourceManager implements RequiresLeaderSessionID, Serializable {
	private static final long serialVersionUID = 1L;

	private final ActorRef resourceManager;

	private final long connectionId;

	public ReconnectResourceManager(ActorRef resourceManager, long connectionId) {
		this.resourceManager = Preconditions.checkNotNull(resourceManager);
		this.connectionId = Preconditions.checkNotNull(connectionId);
	}
	
	public ActorRef resourceManager() {
		return resourceManager;
	}

	public long getConnectionId() {
		return connectionId;
	}

	@Override
	public String toString() {
		return "ReconnectResourceManager(" +
			resourceManager.path() + ", " +
			connectionId + ')';
	}
}
