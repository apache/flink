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

package org.apache.flink.runtime.protocols;

import java.io.IOException;

import org.apache.flink.core.protocols.VersionedProtocol;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.TaskOperationResult;

/**
 * The task submission protocol is implemented by the task manager and allows the job manager
 * to submit and cancel tasks, as well as to query the task manager for cached libraries and submit
 * these if necessary.
 */
public interface TaskOperationProtocol extends VersionedProtocol {

	TaskOperationResult submitTask(TaskDeploymentDescriptor task) throws IOException;

	TaskOperationResult cancelTask(ExecutionAttemptID executionId) throws IOException;

	
}
