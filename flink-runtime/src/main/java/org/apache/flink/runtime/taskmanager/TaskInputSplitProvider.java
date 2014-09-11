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

import java.io.IOException;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.protocols.InputSplitProviderProtocol;
import scala.concurrent.Await;
import scala.concurrent.Future;

public class TaskInputSplitProvider implements InputSplitProvider {

	private final ActorRef jobmanager;
	
	private final JobID jobId;
	
	private final JobVertexID vertexId;
	
	public TaskInputSplitProvider(ActorRef jobmanager, JobID jobId, JobVertexID vertexId) {
		this.jobmanager = jobmanager;
		this.jobId = jobId;
		this.vertexId = vertexId;
	}

	@Override
	public InputSplit getNextInputSplit() {
		try {
			Future<Object> futureResponse = Patterns.ask(jobmanager, new JobManagerMessages.RequestNextInputSplit
					(jobId, vertexId), AkkaUtils.FUTURE_TIMEOUT());

			TaskManagerMessages.NextInputSplit nextInputSplit = (TaskManagerMessages.NextInputSplit) Await.result
					(futureResponse,AkkaUtils.AWAIT_DURATION());

			return nextInputSplit.inputSplit();
		}
		catch (Exception e) {
			throw new RuntimeException("Requesting the next InputSplit failed.", e);
		}
	}
}
