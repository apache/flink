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

package org.apache.flink.runtime.jobClient;

import org.apache.flink.runtime.messages.ExecutionGraphMessages;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.rpc.RpcGateway;

import java.util.UUID;

/**
 * The {@link JobInfoTrackerGateway}'s RPC gateway interface, receive notification from {@link org.apache.flink.runtime.jobmaster.JobMaster}.
 */
public interface JobInfoTrackerGateway extends RpcGateway {

	/**
	 * Receives notification about execution state changed event from {@link org.apache.flink.runtime.jobmaster.JobMaster}
	 *
	 * @param jobMasterLeaderSessionID leaderSessionID of jobMaster which send this notifcation
	 * @param executionStateChanged    the execution state change message
	 */
	void notifyJobExecutionStateChanged(UUID jobMasterLeaderSessionID,
		ExecutionGraphMessages.ExecutionStateChanged executionStateChanged);

	/**
	 * Receives notification about job status changed event from {@link org.apache.flink.runtime.jobmaster.JobMaster}
	 *
	 * @param jobMasterLeaderSessionID leaderSessionID of jobMaster which send this notifcation
	 * @param jobStatusChanged         the job state change message
	 */
	void notifyJobStatusChanged(UUID jobMasterLeaderSessionID,
		ExecutionGraphMessages.JobStatusChanged jobStatusChanged);

	/**
	 * Receives notification about job result from {@link org.apache.flink.runtime.jobmaster.JobMaster}
	 *
	 * @param jobMasterLeaderSessionID leaderSessionID of jobMaster which send this notifcation
	 * @param jobResultMessage         job result
	 */
	void notifyJobResult(UUID jobMasterLeaderSessionID, JobManagerMessages.JobResultMessage jobResultMessage);

}
