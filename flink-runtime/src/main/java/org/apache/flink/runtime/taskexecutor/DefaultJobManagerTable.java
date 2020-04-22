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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.slotpool.DualKeyLinkedMap;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * Default implementation of {@link JobManagerTable}.
 */
public class DefaultJobManagerTable implements JobManagerTable {
	private final DualKeyLinkedMap<JobID, ResourceID, JobManagerConnection> jobManagerConnections;

	public DefaultJobManagerTable() {
		jobManagerConnections = new DualKeyLinkedMap<>(4);
	}

	@Override
	public boolean contains(JobID jobId) {
		return jobManagerConnections.containsKeyA(jobId);
	}

	@Override
	public boolean contains(ResourceID resourceId) {
		return jobManagerConnections.containsKeyB(resourceId);
	}

	@Override
	public boolean put(JobID jobId, ResourceID resourceId, JobManagerConnection jobManagerConnection) {
		JobManagerConnection previousJMC = jobManagerConnections.put(jobId, resourceId, jobManagerConnection);

		if (previousJMC != null) {
			jobManagerConnections.put(jobId, resourceId, previousJMC);

			return false;
		} else {
			return true;
		}
	}

	@Override
	@Nullable
	public JobManagerConnection remove(JobID jobId) {
		return jobManagerConnections.removeKeyA(jobId);
	}

	@Override
	@Nullable
	public JobManagerConnection get(JobID jobId) {
		return jobManagerConnections.getKeyA(jobId);
	}

	@Nullable
	@Override
	public JobManagerConnection get(ResourceID resourceId) {
		return jobManagerConnections.getKeyB(resourceId);
	}

	@Override
	public Collection<JobManagerConnection> values() {
		return jobManagerConnections.values();
	}
}
