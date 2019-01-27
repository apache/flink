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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Container for multiple {@link JobManagerConnection} registered under their respective job id.
 */
public class JobManagerTable {
	private final Map<JobID, JobManagerConnection> jobIDIndices;

	private final Map<ResourceID, JobManagerConnection> resourceIDIndices;

	public JobManagerTable() {
		// The JobManagerTable will be updated in the RPC main thread, and read in
		// the task thread when committing and querying aggregated accumulators, thus
		// ConcurrentHashMap is required here.
		jobIDIndices = new ConcurrentHashMap<>(4);
		resourceIDIndices = new ConcurrentHashMap<>(4);
	}

	public Collection<JobManagerConnection> getAllJobManagerConnections() {
		return jobIDIndices.values();
	}

	public boolean contains(JobID jobId) {
		return jobIDIndices.containsKey(jobId);
	}

	public boolean add(JobManagerConnection jobManagerConnection) {
		JobManagerConnection previousJMC = jobIDIndices.put(jobManagerConnection.getJobID(), jobManagerConnection);
		if (previousJMC != null) {
			jobIDIndices.put(jobManagerConnection.getJobID(), previousJMC);
			return false;
		} else {
			resourceIDIndices.put(jobManagerConnection.getResourceID(), jobManagerConnection);
			return true;
		}
	}

	public JobManagerConnection remove(JobID jobId) {
		JobManagerConnection jobManagerConnection = jobIDIndices.remove(jobId);
		if (jobManagerConnection != null) {
			resourceIDIndices.remove(jobManagerConnection.getResourceID());
		}
		return jobManagerConnection;
	}

	public JobManagerConnection get(JobID jobId) {
		return jobIDIndices.get(jobId);
	}

	public boolean contains(ResourceID resourceID) {
		return resourceIDIndices.containsKey(resourceID);
	}

	public JobManagerConnection remove(ResourceID resourceID) {
		JobManagerConnection jobManagerConnection = resourceIDIndices.remove(resourceID);
		if (jobManagerConnection != null) {
			jobIDIndices.remove(jobManagerConnection.getJobID());
		}
		return jobManagerConnection;
	}

	public JobManagerConnection get(ResourceID resourceID) {
		return resourceIDIndices.get(resourceID);
	}
}
