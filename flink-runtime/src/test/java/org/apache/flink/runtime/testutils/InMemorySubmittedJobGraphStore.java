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

package org.apache.flink.runtime.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * In-Memory implementation of {@link SubmittedJobGraphStore} for testing purposes.
 */
public class InMemorySubmittedJobGraphStore implements SubmittedJobGraphStore {

	private final Map<JobID, SubmittedJobGraph> storedJobs = new HashMap<>();

	private volatile boolean started;

	@Override
	public void start(@Nullable SubmittedJobGraphListener jobGraphListener) throws Exception {
		started = true;
	}

	@Override
	public void stop() throws Exception {
		started = false;
	}

	@Override
	public SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception {
		verifyIsStarted();
		return storedJobs.getOrDefault(jobId, null);
	}

	@Override
	public void putJobGraph(SubmittedJobGraph jobGraph) throws Exception {
		verifyIsStarted();
		storedJobs.put(jobGraph.getJobId(), jobGraph);
	}

	@Override
	public void removeJobGraph(JobID jobId) throws Exception {
		verifyIsStarted();
		storedJobs.remove(jobId);
	}

	@Override
	public Collection<JobID> getJobIds() throws Exception {
		verifyIsStarted();
		return storedJobs.keySet();
	}

	public boolean contains(JobID jobId) {
		verifyIsStarted();
		return storedJobs.containsKey(jobId);
	}

	private void verifyIsStarted() {
		Preconditions.checkState(started, "Not running. Forgot to call start()?");
	}

}
