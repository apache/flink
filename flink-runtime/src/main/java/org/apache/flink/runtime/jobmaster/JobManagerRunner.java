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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.util.AutoCloseableAsync;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for a runner which executes a {@link JobMaster}.
 */
public interface JobManagerRunner extends AutoCloseableAsync {

	/**
	 * Start the execution of the {@link JobMaster}.
	 *
	 * @throws Exception if the JobMaster cannot be started
	 */
	void start() throws Exception;

	/**
	 * Get the {@link JobMasterGateway} of the {@link JobMaster}. The future is
	 * only completed if the JobMaster becomes leader.
	 *
	 * @return Future with the JobMasterGateway once the underlying JobMaster becomes leader
	 */
	CompletableFuture<JobMasterGateway> getJobMasterGateway();

	/**
	 * Get the result future of this runner. The future is completed once the executed
	 * job reaches a globally terminal state.
	 *
	 * @return Future which is completed with the job result
	 */
	CompletableFuture<ArchivedExecutionGraph> getResultFuture();

	/**
	 * Get the job id of the executed job.
	 *
	 * @return job id of the executed job
	 */
	JobID getJobID();
}
