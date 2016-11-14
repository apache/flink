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

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.messages.RequiresLeaderSessionID;

import static java.util.Objects.requireNonNull;

/**
 * Generic message to signal the cluster framework to shut the cluster down.
 */
public class StopCluster implements RequiresLeaderSessionID, java.io.Serializable {

	private static final long serialVersionUID = -8957259342982181684L;

	private final ApplicationStatus finalStatus;

	private final String message;

	public StopCluster(ApplicationStatus finalStatus, String message) {
		this.finalStatus = requireNonNull(finalStatus);
		this.message = message == null ? "" : message;
	}

	// ------------------------------------------------------------------------

	public ApplicationStatus finalStatus() {
		return finalStatus;
	}

	public String message() {
		return message;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "StopApplication { message='" + message + "' }";
	}
}
