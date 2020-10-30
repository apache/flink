/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

/**
 * This interface enables subscribing to failures that are detected from the JobMaster side
 * (e.g., from within the {@link ExecutionGraph}).
 * In contrast, there are also failures that are detected by the TaskManager, which are communicated
 * via {@link JobMasterGateway#updateTaskExecutionState(TaskExecutionState)}.
 */
public interface InternalFailuresListener {

	void notifyTaskFailure(ExecutionAttemptID attemptId, Throwable t, boolean cancelTask, boolean releasePartitions);

	void notifyGlobalFailure(Throwable t);
}
