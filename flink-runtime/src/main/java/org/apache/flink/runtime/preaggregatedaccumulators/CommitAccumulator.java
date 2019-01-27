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

package org.apache.flink.runtime.preaggregatedaccumulators;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;
import java.util.Set;

/**
 * Partially aggregated accumulators committed to JobMaster.
 */
public class CommitAccumulator implements Serializable {
	/** The job vertex ID of the tasks that commit the values. */
	private final JobVertexID jobVertexId;

	/** The name of the accumulator. */
	private final String name;

	/** The partially aggregated value. */
	private final Accumulator accumulator;

	/** The index of the tasks that commit the value. */
	private final Set<Integer> committedTasks;

	public CommitAccumulator(JobVertexID jobVertexId, String name, Accumulator accumulator, Set<Integer> committedTasks) {
		this.jobVertexId = jobVertexId;
		this.name = name;
		this.accumulator = accumulator;
		this.committedTasks = committedTasks;
	}

	public JobVertexID getJobVertexId() {
		return jobVertexId;
	}

	public String getName() {
		return name;
	}

	public Accumulator getAccumulator() {
		return accumulator;
	}

	public Set<Integer> getCommittedTasks() {
		return committedTasks;
	}
}
