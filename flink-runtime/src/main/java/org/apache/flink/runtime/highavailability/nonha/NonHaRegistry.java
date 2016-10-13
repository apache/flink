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

package org.apache.flink.runtime.highavailability.nonha;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;

import java.util.HashSet;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A registry for running jobs, not-highly available.
 */
public class NonHaRegistry implements RunningJobsRegistry {

	/** The currently running jobs */
	private final HashSet<JobID> running = new HashSet<>();

	@Override
	public void setJobRunning(JobID jobID) {
		checkNotNull(jobID);

		synchronized (running) {
			running.add(jobID);
		}
	}

	@Override
	public void setJobFinished(JobID jobID) {
		checkNotNull(jobID);

		synchronized (running) {
			running.remove(jobID);
		}
	}

	@Override
	public boolean isJobRunning(JobID jobID) {
		checkNotNull(jobID);

		synchronized (running) {
			return running.contains(jobID);
		}
	}
}
