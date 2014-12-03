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

package org.apache.flink.runtime.taskmanager;

import akka.actor.ActorRef;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import scala.concurrent.duration.FiniteDuration;

public class TaskInputSplitProvider implements InputSplitProvider {

	private final ActorRef jobManager;
	
	private final JobID jobId;
	
	private final JobVertexID vertexId;

	private final ExecutionAttemptID executionID;

	private final FiniteDuration timeout;
	
	public TaskInputSplitProvider(ActorRef jobManager, JobID jobId, JobVertexID vertexId,
								ExecutionAttemptID executionID, FiniteDuration timeout) {
		this.jobManager = jobManager;
		this.jobId = jobId;
		this.vertexId = vertexId;
		this.executionID = executionID;
		this.timeout = timeout;
	}

	@Override
	public InputSplit getNextInputSplit() {
		try {
			TaskManagerMessages.NextInputSplit nextInputSplit = AkkaUtils.ask(jobManager,
					new JobManagerMessages.RequestNextInputSplit(jobId, vertexId, executionID),
					timeout);

			return nextInputSplit.inputSplit();
		}
		catch (Exception e) {
			throw new RuntimeException("Requesting the next InputSplit failed.", e);
		}
	}
}
