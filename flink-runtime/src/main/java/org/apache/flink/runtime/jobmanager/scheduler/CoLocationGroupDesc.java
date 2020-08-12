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

package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.AbstractID;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A read-only and light weight version of {@link CoLocationGroup}.
 */
public class CoLocationGroupDesc {

	private final AbstractID id;

	private final List<JobVertexID> vertices;

	private CoLocationGroupDesc(final AbstractID id, final List<JobVertexID> vertices) {
		this.id = checkNotNull(id);
		this.vertices = checkNotNull(vertices);
	}

	public AbstractID getId() {
		return id;
	}

	public List<JobVertexID> getVertices() {
		return Collections.unmodifiableList(vertices);
	}

	public CoLocationConstraintDesc getLocationConstraint(final int index) {
		return new CoLocationConstraintDesc(id, index);
	}

	public static CoLocationGroupDesc from(final CoLocationGroup group) {
		return new CoLocationGroupDesc(
			group.getId(),
			group.getVertices().stream().map(JobVertex::getID).collect(Collectors.toList()));
	}

	@VisibleForTesting
	public static CoLocationGroupDesc from(final JobVertexID ...ids) {
		return new CoLocationGroupDesc(new AbstractID(), Arrays.asList(ids));
	}
}
