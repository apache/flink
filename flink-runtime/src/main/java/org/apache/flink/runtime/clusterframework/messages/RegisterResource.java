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
import org.apache.flink.runtime.messages.RegistrationMessages;
import org.apache.flink.runtime.messages.RequiresLeaderSessionID;

/**
 * Triggers a lookup at the ResourceManager to check if the resource for a TaskManager is registered.
 */
public class RegisterResource implements RequiresLeaderSessionID, java.io.Serializable {
	private static final long serialVersionUID = 1L;

	private final ActorRef taskManager;
	private final RegistrationMessages.RegisterTaskManager registerMessage;


	public RegisterResource(ActorRef taskManager, RegistrationMessages.RegisterTaskManager registerMessage) {
		this.taskManager = taskManager;
		this.registerMessage = registerMessage;
	}

	public ActorRef getTaskManager() {
		return taskManager;
	}

	public RegistrationMessages.RegisterTaskManager getRegisterMessage() {
		return registerMessage;
	}

	@Override
	public String toString() {
		return "RegisterResource{" +
			"taskManager=" + taskManager +
			", registerMessage=" + registerMessage +
			'}';
	}
}
