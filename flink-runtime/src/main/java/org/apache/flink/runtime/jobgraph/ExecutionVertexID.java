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

package org.apache.flink.runtime.jobgraph;

import java.io.Serializable;
import java.util.Objects;

/**
 * A class for statistically unique task IDs.
 */
public class ExecutionVertexID implements Serializable {

	private static final long serialVersionUID = 1L;

	private final JobVertexID jobVertexID;

	private final int subTaskIndex;

	public ExecutionVertexID(JobVertexID jobVertexID, int subTaskIndex) {
		this.jobVertexID = jobVertexID;
		this.subTaskIndex = subTaskIndex;
	}

	public JobVertexID getJobVertexID() {
		return jobVertexID;
	}

	public int getSubTaskIndex() {
		return subTaskIndex;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ExecutionVertexID) {
			ExecutionVertexID other = (ExecutionVertexID) obj;

			return Objects.equals(jobVertexID, other.jobVertexID) &&
				subTaskIndex == other.subTaskIndex;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int result = subTaskIndex;
		result += result * 31 + jobVertexID.hashCode();

		return result;
	}
}
