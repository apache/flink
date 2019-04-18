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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Objects;

/**
 * Id identifying {@link ExecutionVertex}.
 */
public class ExecutionVertexID {
	private final JobVertexID jobVertexId;

	private final int subtaskIndex;

	public ExecutionVertexID(JobVertexID jobVertexId, int subtaskIndex) {
		this.jobVertexId = jobVertexId;
		this.subtaskIndex = subtaskIndex;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ExecutionVertexID that = (ExecutionVertexID) o;

		if (subtaskIndex != that.subtaskIndex) {
			return false;
		}

		return Objects.equals(jobVertexId, that.jobVertexId);

	}

	@Override
	public int hashCode() {
		int result = jobVertexId != null ? jobVertexId.hashCode() : 0;
		result = 31 * result + subtaskIndex;
		return result;
	}
}
