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

package org.apache.flink.core.execution;

import org.apache.flink.api.common.JobID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A job context is used to create a {@link JobClient} or to simply get the {@link JobID}.
 */
public class JobContext {

	private final JobClient jobClient;

	public JobContext(JobClient jobClient) {
		this.jobClient = checkNotNull(jobClient);
	}

	/**
	 * Create a new {@link JobClient} to communicate with the job. The caller is responsible for
	 * closing the returned {@link JobClient}.
	 */
	public JobClient createJobClient() throws Exception {
		return jobClient.duplicate();
	}

	/**
	 * Return {@link JobID} of the job.
	 */
	public JobID getJobID() {
		return jobClient.getJobID();
	}
}
