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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.messages.RequiresLeaderSessionID;

/**
 * Notifies the ResourceManager that a TaskManager has been started in a container with the given
 * resource id.
 */
public class NotifyResourceStarted implements RequiresLeaderSessionID, java.io.Serializable {
	private static final long serialVersionUID = 1L;

	private final ResourceID resourceID;

	public NotifyResourceStarted(ResourceID resourceID) {
		this.resourceID = resourceID;
	}

	public ResourceID getResourceID() {
		return resourceID;
	}

	@Override
	public String toString() {
		return "NotifyResourceStarted{" +
			", resourceID=" + resourceID +
			'}';
	}
}
