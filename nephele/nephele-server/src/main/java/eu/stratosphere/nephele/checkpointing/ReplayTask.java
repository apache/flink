/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.checkpointing;

import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.profiling.TaskManagerProfiler;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.taskmanager.Task;
import eu.stratosphere.nephele.taskmanager.TaskManager;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TaskContext;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTaskContext;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.template.InputSplitProvider;

public final class ReplayTask implements Task {

	private final ExecutionVertexID vertexID;

	private final CheckpointEnvironment environment;

	private final Task encapsulatedTask;

	public ReplayTask(final ExecutionVertexID vertexID, final Environment environment,
			final TaskManager taskManager) {

		this.vertexID = vertexID;
		this.environment = CheckpointEnvironment.createFromEnvironment(environment);

		this.encapsulatedTask = null;
	}

	public ReplayTask(final Task encapsulatedTask) {

		this.vertexID = encapsulatedTask.getVertexID();
		this.environment = CheckpointEnvironment.createFromEnvironment(encapsulatedTask.getEnvironment());

		this.encapsulatedTask = encapsulatedTask;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.environment.getJobID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ExecutionVertexID getVertexID() {

		return this.vertexID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Environment getEnvironment() {

		return this.environment;
	}

	@Override
	public void markAsFailed() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isTerminated() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void startExecution() {
		// TODO Auto-generated method stub

		System.out.println("Checkpoint replay task started");
	}

	@Override
	public void cancelExecution() {
		// TODO Auto-generated method stub

	}

	@Override
	public void killExecution() {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerMemoryManager(final MemoryManager memoryManager) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerIOManager(final IOManager ioManager) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputSplitProvider(final InputSplitProvider inputSplitProvider) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerProfiler(final TaskManagerProfiler taskManagerProfiler, final Configuration jobConfiguration) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterMemoryManager(final MemoryManager memoryManager) {

		if (this.encapsulatedTask != null) {
			this.encapsulatedTask.unregisterMemoryManager(memoryManager);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterProfiler(final TaskManagerProfiler taskManagerProfiler) {

		if (this.encapsulatedTask != null) {
			this.encapsulatedTask.unregisterProfiler(taskManagerProfiler);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TaskContext createTaskContext(final TransferEnvelopeDispatcher transferEnvelopeDispatcher,
			final Map<ExecutionVertexID, RuntimeTaskContext> tasksWithUndecidedCheckpoints) {

		return new ReplayTaskContext();
	}

}
