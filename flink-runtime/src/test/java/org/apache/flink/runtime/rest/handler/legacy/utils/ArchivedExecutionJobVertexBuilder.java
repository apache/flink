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

package org.apache.flink.runtime.rest.handler.legacy.utils;

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import java.util.Random;

/**
 * Utility class for constructing an ArchivedExecutionJobVertex.
 */
public class ArchivedExecutionJobVertexBuilder {

	private static final Random RANDOM = new Random();

	private ArchivedExecutionVertex[] taskVertices;
	private JobVertexID id;
	private String name;
	private int parallelism;
	private int maxParallelism;
	private StringifiedAccumulatorResult[] archivedUserAccumulators;

	public ArchivedExecutionJobVertexBuilder setTaskVertices(ArchivedExecutionVertex[] taskVertices) {
		this.taskVertices = taskVertices;
		return this;
	}

	public ArchivedExecutionJobVertexBuilder setId(JobVertexID id) {
		this.id = id;
		return this;
	}

	public ArchivedExecutionJobVertexBuilder setName(String name) {
		this.name = name;
		return this;
	}

	public ArchivedExecutionJobVertexBuilder setParallelism(int parallelism) {
		this.parallelism = parallelism;
		return this;
	}

	public ArchivedExecutionJobVertexBuilder setMaxParallelism(int maxParallelism) {
		this.maxParallelism = maxParallelism;
		return this;
	}

	public ArchivedExecutionJobVertexBuilder setArchivedUserAccumulators(StringifiedAccumulatorResult[] archivedUserAccumulators) {
		this.archivedUserAccumulators = archivedUserAccumulators;
		return this;
	}

	public ArchivedExecutionJobVertex build() {
		Preconditions.checkNotNull(taskVertices);
		return new ArchivedExecutionJobVertex(
			taskVertices,
			id != null ? id : new JobVertexID(),
			name != null ? name : "task_" + RANDOM.nextInt(),
			parallelism,
			maxParallelism,
			archivedUserAccumulators != null ? archivedUserAccumulators : new StringifiedAccumulatorResult[0]
		);
	}
}
