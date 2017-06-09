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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;

import java.util.HashMap;
import java.util.Map;

/**
 * Container for multiple {@link JobManagerConnection} registered under their respective job id.
 */
public class JobManagerTable {
	private final Map<JobID, JobManagerConnection> jobManagerConnections;

	public JobManagerTable() {
		jobManagerConnections = new HashMap<>(4);
	}

	public boolean contains(JobID jobId) {
		return jobManagerConnections.containsKey(jobId);
	}

	public boolean put(JobID jobId, JobManagerConnection jobManagerConnection) {
		JobManagerConnection previousJMC = jobManagerConnections.put(jobId, jobManagerConnection);

		if (previousJMC != null) {
			jobManagerConnections.put(jobId, previousJMC);

			return false;
		} else {
			return true;
		}
	}

	public JobManagerConnection remove(JobID jobId) {
		return jobManagerConnections.remove(jobId);
	}

	public JobManagerConnection get(JobID jobId) {
		return jobManagerConnections.get(jobId);
	}
}
