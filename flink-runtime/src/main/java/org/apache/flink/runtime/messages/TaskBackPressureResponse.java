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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Response to the task back pressure request rpc call.
 */
public class TaskBackPressureResponse implements Serializable {

	private static final long serialVersionUID = -4786454630050578031L;

	private final int requestId;

	private final ExecutionAttemptID executionAttemptID;

	private final double backPressureRatio;

	public TaskBackPressureResponse(
			int requestId,
			ExecutionAttemptID executionAttemptID,
			double backPressureRatio) {
		this.requestId = requestId;
		this.executionAttemptID = Preconditions.checkNotNull(executionAttemptID);
		this.backPressureRatio = backPressureRatio;
	}

	public int getRequestId() {
		return requestId;
	}

	public ExecutionAttemptID getExecutionAttemptID() {
		return executionAttemptID;
	}

	public double getBackPressureRatio() {
		return backPressureRatio;
	}
}
