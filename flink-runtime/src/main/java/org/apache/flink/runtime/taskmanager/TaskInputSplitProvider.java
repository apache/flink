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

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.util.Preconditions;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * Implementation using {@link ActorGateway} to forward the messages.
 */
public class TaskInputSplitProvider implements InputSplitProvider {

	private final ActorGateway jobManager;
	
	private final JobID jobID;
	
	private final JobVertexID vertexID;

	private final ExecutionAttemptID executionID;

	private final FiniteDuration timeout;


	public TaskInputSplitProvider(
		ActorGateway jobManager,
		JobID jobID,
		JobVertexID vertexID,
		ExecutionAttemptID executionID,
		FiniteDuration timeout) {

		this.jobManager = Preconditions.checkNotNull(jobManager);
		this.jobID = Preconditions.checkNotNull(jobID);
		this.vertexID = Preconditions.checkNotNull(vertexID);
		this.executionID = Preconditions.checkNotNull(executionID);
		this.timeout = Preconditions.checkNotNull(timeout);
	}

	@Override
	public InputSplit getNextInputSplit(ClassLoader userCodeClassLoader) throws InputSplitProviderException {
		Preconditions.checkNotNull(userCodeClassLoader);

		final Future<Object> response = jobManager.ask(
			new JobManagerMessages.RequestNextInputSplit(jobID, vertexID, executionID),
			timeout);

		final Object result;

		try {
			result = Await.result(response, timeout);
		} catch (Exception e) {
			throw new InputSplitProviderException("Did not receive next input split from JobManager.", e);
		}

		if(result instanceof JobManagerMessages.NextInputSplit){
			final JobManagerMessages.NextInputSplit nextInputSplit =
				(JobManagerMessages.NextInputSplit) result;

			byte[] serializedData = nextInputSplit.splitData();

			if(serializedData == null) {
				return null;
			} else {
				final Object deserialized;

				try {
					deserialized = InstantiationUtil.deserializeObject(serializedData,
						userCodeClassLoader);
				} catch (Exception e) {
					throw new InputSplitProviderException("Could not deserialize the serialized input split.", e);
				}

				return (InputSplit) deserialized;
			}
		} else {
			throw new InputSplitProviderException("RequestNextInputSplit requires a response of type " +
				"NextInputSplit. Instead response is of type " + result.getClass() + '.');
		}

	}
}
