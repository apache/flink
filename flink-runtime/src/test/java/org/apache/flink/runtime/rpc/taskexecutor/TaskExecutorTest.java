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

package org.apache.flink.runtime.rpc.taskexecutor;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.util.DirectExecutorService;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.net.URL;
import java.util.Collections;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class TaskExecutorTest extends TestLogger {

	/**
	 * Tests that we can deploy and cancel a task on the TaskExecutor without exceptions
	 */
	@Test
	public void testTaskExecution() throws Exception {
		RpcService testingRpcService = mock(RpcService.class);
		DirectExecutorService directExecutorService = new DirectExecutorService();
		TaskExecutor taskExecutor = new TaskExecutor(testingRpcService, directExecutorService);

		TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(
			new JobID(),
			"Test job",
			new JobVertexID(),
			new ExecutionAttemptID(),
			new SerializedValue<ExecutionConfig>(null),
			"Test task",
			0,
			1,
			0,
			new Configuration(),
			new Configuration(),
			"Invokable",
			Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
			Collections.<InputGateDeploymentDescriptor>emptyList(),
			Collections.<BlobKey>emptyList(),
			Collections.<URL>emptyList(),
			0
		);

		Acknowledge ack = taskExecutor.executeTask(tdd);

		ack = taskExecutor.cancelTask(tdd.getExecutionId());
	}

	/**
	 * Tests that cancelling a non-existing task will return an exception
	 */
	@Test(expected=Exception.class)
	public void testWrongTaskCancellation() throws Exception {
		RpcService testingRpcService = mock(RpcService.class);
		DirectExecutorService directExecutorService = null;
		TaskExecutor taskExecutor = new TaskExecutor(testingRpcService, directExecutorService);

		taskExecutor.cancelTask(new ExecutionAttemptID());

		fail("The cancellation should have thrown an exception.");
	}
}
