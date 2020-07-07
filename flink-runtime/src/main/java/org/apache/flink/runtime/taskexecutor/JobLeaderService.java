/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.rpc.RpcService;

/**
 * This service has the responsibility to monitor the job leaders (the job manager which is leader
 * for a given job) for all registered jobs. Upon gaining leadership for a job and detection by the
 * job leader service, the service tries to establish a connection to the job leader. After
 * successfully establishing a connection, the job leader listener is notified about the new job
 * leader and its connection. In case that a job leader loses leadership, the job leader listener
 * is notified as well.
 */
public interface JobLeaderService {

	/**
	 * Start the job leader service with the given services.
	 *
	 * @param initialOwnerAddress to be used for establishing connections (source address)
	 * @param initialRpcService to be used to create rpc connections
	 * @param initialHighAvailabilityServices to create leader retrieval services for the different jobs
	 * @param initialJobLeaderListener listening for job leader changes
	 */
	void start(
		String initialOwnerAddress,
		RpcService initialRpcService,
		HighAvailabilityServices initialHighAvailabilityServices,
		JobLeaderListener initialJobLeaderListener);

	/**
	 * Stop the job leader services. This implies stopping all leader retrieval services for the
	 * different jobs and their leader retrieval listeners.
	 *
	 * @throws Exception if an error occurs while stopping the service
	 */
	void stop() throws Exception;

	/**
	 * Remove the given job from being monitored by the job leader service.
	 *
	 * @param jobId identifying the job to remove from monitoring
	 */
	void removeJob(JobID jobId);

	/**
	 * Add the given job to be monitored. This means that the service tries to detect leaders for
	 * this job and then tries to establish a connection to it.
	 *
	 * @param jobId identifying the job to monitor
	 * @param defaultTargetAddress of the job leader
	 * @throws Exception if an error occurs while starting the leader retrieval service
	 */
	void addJob(JobID jobId, String defaultTargetAddress) throws Exception;

	/**
	 * Triggers reconnection to the last known leader of the given job.
	 *
	 * @param jobId specifying the job for which to trigger reconnection
	 */
	void reconnect(JobID jobId);

	/**
	 * Check whether the service monitors the given job.
	 *
	 * @param jobId identifying the job
	 * @return True if the given job is monitored; otherwise false
	 */
	boolean containsJob(JobID jobId);
}
