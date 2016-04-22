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

import java.io.Serializable;

/**
 * The reply to a {@code GetClusterStatus} message sent by the job manager. Sends over the
 * current number of task managers and the available task slots.
 */
public class GetClusterStatusResponse implements Serializable {

	private static final long serialVersionUID = 1L;

	private final int numRegisteredTaskManagers;

	private final int totalNumberOfSlots;

	public GetClusterStatusResponse(int numRegisteredTaskManagers, int totalNumberOfSlots) {
		this.numRegisteredTaskManagers = numRegisteredTaskManagers;
		this.totalNumberOfSlots = totalNumberOfSlots;
	}

	// ------------------------------------------------------------------------

	public int numRegisteredTaskManagers() {
		return numRegisteredTaskManagers;
	}

	public int totalNumberOfSlots() {
		return totalNumberOfSlots;
	}

	// ------------------------------------------------------------------------


	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o == null || getClass() != o.getClass()) {
			return false;
		}

		GetClusterStatusResponse that = (GetClusterStatusResponse) o;

		return numRegisteredTaskManagers == that.numRegisteredTaskManagers
			&& totalNumberOfSlots == that.totalNumberOfSlots;

	}

	@Override
	public String toString() {
		return "GetClusterStatusResponse {" +
			"numRegisteredTaskManagers=" + numRegisteredTaskManagers +
			", totalNumberOfSlots=" + totalNumberOfSlots +
			'}';
	}
}
