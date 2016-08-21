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
package org.apache.flink.client.program;

import org.apache.flink.api.common.JobClient;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobExecutionException;

import java.util.Map;

/**
 * A detached job client which lazily initiates the cluster connection.
 */
public class JobClientLazy implements JobClient {

	/** The ClusterClient used for JobClient retrieval */
	private final ClusterClient clusterClient;

	/** The JobID to eventually retrieve the JobClient for */
	private final JobID jobID;

	/** The retrieved client */
	private JobClientEager retrievedJobClient;

	public JobClientLazy(JobID jobID, ClusterClient clusterClient) {
		this.clusterClient = clusterClient;
		this.jobID = jobID;
	}

	private JobClientEager getJobClient() throws JobExecutionException {
		if (retrievedJobClient == null) {
			retrievedJobClient = clusterClient.retrieveJob(jobID);
		}
		return retrievedJobClient;
	}

	@Override
	public JobID getJobID() {
		return jobID;
	}

	@Override
	public boolean hasFinished() throws Exception {
		return getJobClient().hasFinished();
	}

	@Override
	public JobExecutionResult waitForResult() throws Exception {
		return getJobClient().waitForResult();
	}

	@Override
	public Map<String, Object> getAccumulators() throws Exception {
		return getJobClient().getAccumulators();
	}

	@Override
	public void cancel() throws Exception {
		getJobClient().cancel();
	}

	@Override
	public void stop() throws Exception {
		getJobClient().stop();
	}

	@Override
	public void addFinalizer(Runnable finalizer) throws Exception {
		getJobClient().addFinalizer(finalizer);
	}

	@Override
	public void shutdown() {
		if (retrievedJobClient != null) {
			retrievedJobClient.shutdown();
		}
	}

}
