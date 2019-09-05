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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

class SubmissionTrackingTaskManagerGateway extends SimpleAckingTaskManagerGateway {

	private final BlockingQueue<TaskDeploymentDescriptor> taskDeploymentDescriptors = new LinkedBlockingDeque<>();

	private boolean failSubmission;

	public void setFailSubmission(final boolean failSubmission) {
		this.failSubmission = failSubmission;
	}

	@Override
	public CompletableFuture<Acknowledge> submitTask(final TaskDeploymentDescriptor tdd, final Time timeout) {
		super.submitTask(tdd, timeout);

		taskDeploymentDescriptors.add(tdd);

		if (failSubmission) {
			return FutureUtils.completedExceptionally(new RuntimeException("Task submission failed."));
		} else {
			return CompletableFuture.completedFuture(Acknowledge.get());
		}
	}

	public List<ExecutionVertexID> getDeployedExecutionVertices(int num, long timeoutMs) {
		final List<ExecutionVertexID> deployedVertices = new ArrayList<>();
		for (int i = 0; i < num; i++) {
			try {
				final TaskDeploymentDescriptor taskDeploymentDescriptor = taskDeploymentDescriptors.poll(timeoutMs, TimeUnit.MILLISECONDS);
				checkState(taskDeploymentDescriptor != null, "Expected %s tasks to be submitted within %s ms, got %s", num, timeoutMs, i);
				deployedVertices.add(getExecutionVertexId(taskDeploymentDescriptor));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		return deployedVertices;
	}

	private ExecutionVertexID getExecutionVertexId(final TaskDeploymentDescriptor deploymentDescriptor) {
		final TaskInformation taskInformation = deserializeTaskInformation(deploymentDescriptor);
		final JobVertexID jobVertexId = taskInformation.getJobVertexId();
		final int subtaskIndex = deploymentDescriptor.getSubtaskIndex();
		return new ExecutionVertexID(jobVertexId, subtaskIndex);
	}

	private TaskInformation deserializeTaskInformation(final TaskDeploymentDescriptor taskDeploymentDescriptor) {
		try {
			return taskDeploymentDescriptor
				.getSerializedTaskInformation()
				.deserializeValue(ClassLoader.getSystemClassLoader());
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
}
