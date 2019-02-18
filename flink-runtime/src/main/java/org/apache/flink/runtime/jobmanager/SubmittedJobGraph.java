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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A recoverable {@link JobGraph}.
 */
public class SubmittedJobGraph implements Serializable {

	private static final long serialVersionUID = 2836099271734771825L;

	/** The submitted {@link JobGraph}. */
	private final JobGraph jobGraph;

	/**
	 * Creates a {@link SubmittedJobGraph}.
	 *
	 * @param jobGraph The submitted {@link JobGraph}
	 */
	public SubmittedJobGraph(JobGraph jobGraph) {
		this.jobGraph = checkNotNull(jobGraph, "Job graph");
	}

	/**
	 * Returns the submitted {@link JobGraph}.
	 */
	public JobGraph getJobGraph() {
		return jobGraph;
	}

	/**
	 * Returns the {@link JobID} of the submitted {@link JobGraph}.
	 */
	public JobID getJobId() {
		return jobGraph.getJobID();
	}

	@Override
	public String toString() {
		return String.format("SubmittedJobGraph(%s)", jobGraph.getJobID());
	}
}
