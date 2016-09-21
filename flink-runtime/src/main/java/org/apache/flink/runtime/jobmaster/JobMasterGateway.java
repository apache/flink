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

import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import scala.concurrent.Future;

/**
 * {@link JobMaster} rpc gateway interface
 */
public interface JobMasterGateway extends RpcGateway {

	/**
	 * Submits a job to the job manager. The job is registered at the libraryCacheManager which
	 * creates the job's class loader. The job graph is appended to the corresponding execution
	 * graph and be prepared to run.
	 *
	 * @param isRecovery Flag indicating whether this is a recovery or initial submission
	 * @return Flag indicating whether this job has been accepted
	 */
	Future<Boolean> submitJob(final boolean isRecovery);

	/**
	 * Making this job begins to run.
	 */
	void startJob();

	/**
	 * Suspending job, all the running tasks will be cancelled, and runtime status will be cleared. Should re-submit
	 * the job before restarting it.
	 *
	 * @param cause The reason of why this job been suspended.
	 */
	void suspendJob(final Throwable cause);

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param taskExecutionState New task execution state for a given task
	 * @return Future acknowledge of the task execution state update
	 */
	Future<Acknowledge> updateTaskExecutionState(TaskExecutionState taskExecutionState);

	/**
	 * Triggers the registration of the job master at the resource manager.
	 *
	 * @param address Address of the resource manager
	 */
	void registerAtResourceManager(final String address);
}
