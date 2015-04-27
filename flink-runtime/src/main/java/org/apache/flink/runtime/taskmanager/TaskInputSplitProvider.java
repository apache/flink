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

import akka.pattern.Patterns;
import akka.util.Timeout;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.InstantiationUtil;

import scala.concurrent.Await;
import scala.concurrent.Future;

public class TaskInputSplitProvider implements InputSplitProvider {

	private final ActorRef jobManager;
	
	private final JobID jobId;
	
	private final JobVertexID vertexId;

	private final ExecutionAttemptID executionID;

	private final ClassLoader usercodeClassLoader;
	
	private final Timeout timeout;
	
	public TaskInputSplitProvider(ActorRef jobManager, JobID jobId, JobVertexID vertexId,
								ExecutionAttemptID executionID, ClassLoader userCodeClassLoader,
								Timeout timeout)
	{
		this.jobManager = jobManager;
		this.jobId = jobId;
		this.vertexId = vertexId;
		this.executionID = executionID;
		this.usercodeClassLoader = userCodeClassLoader;
		this.timeout = timeout;
	}

	@Override
	public InputSplit getNextInputSplit() {
		try {
			final Future<Object> response = Patterns.ask(jobManager,
					new JobManagerMessages.RequestNextInputSplit(jobId, vertexId, executionID),
					timeout);

			final Object result = Await.result(response, timeout.duration());

			if(!(result instanceof JobManagerMessages.NextInputSplit)){
				throw new RuntimeException("RequestNextInputSplit requires a response of type " +
						"NextInputSplit. Instead response is of type " + result.getClass() + ".");
			} else {
				final JobManagerMessages.NextInputSplit nextInputSplit =
						(JobManagerMessages.NextInputSplit) result;

				byte[] serializedData = nextInputSplit.splitData();

				if(serializedData == null) {
					return null;
				} else {
					Object deserialized = InstantiationUtil.deserializeObject(serializedData,
							usercodeClassLoader);
					return (InputSplit) deserialized;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Requesting the next InputSplit failed.", e);
		}
	}
}
