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

package org.apache.flink.mesos.scheduler.messages;

import org.apache.mesos.Protos;

/**
 * Message sent by the callback handler to the scheduler actor
 * when the status of a task has changed (e.g., a slave is lost and so the task is lost,
 * a task finishes and an executor sends a status update saying so, etc).
 */
public class StatusUpdate {
	private Protos.TaskStatus status;

	public StatusUpdate(Protos.TaskStatus status) {
		this.status = status;
	}

	public Protos.TaskStatus status() {
		return status;
	}

	@Override
	public String toString() {
		return "StatusUpdate{" +
			"status=" + status +
			'}';
	}
}
