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

package eu.stratosphere.nephele.streaming.listeners;

import java.util.ArrayDeque;
import java.util.Queue;

import eu.stratosphere.nephele.execution.Mapper;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.StreamingCommunicationThread;
import eu.stratosphere.nephele.streaming.actions.AbstractAction;
import eu.stratosphere.nephele.streaming.chaining.StreamChainCoordinator;
import eu.stratosphere.nephele.streaming.types.AbstractStreamingData;
import eu.stratosphere.nephele.types.Record;

public final class StreamListenerContext {

	public static final String CONTEXT_CONFIGURATION_KEY = "streaming.listener.context";

	private static enum TaskType {
		INPUT, REGULAR, OUTPUT
	};

	private final Queue<AbstractAction> pendingActions = new ArrayDeque<AbstractAction>();

	private final JobID jobID;

	private final ExecutionVertexID vertexID;

	private final StreamingCommunicationThread communicationThread;

	private final StreamChainCoordinator chainCoordinator;

	private final TaskType taskType;

	private final int aggregationInterval;

	private final int taggingInterval;

	private StreamListenerContext(final JobID jobID, final ExecutionVertexID vertexID,
			final StreamingCommunicationThread communicationThread, final StreamChainCoordinator chainCoordinator,
			final TaskType taskType, final int aggregationInterval, final int taggingInterval) {

		if (jobID == null) {
			throw new IllegalArgumentException("Parameter jobID must not be null");
		}

		if (vertexID == null) {
			throw new IllegalArgumentException("Parameter vertexID must not be null");
		}

		if (communicationThread == null) {
			throw new IllegalArgumentException("Parameter communicationThread must not be null");
		}

		if (taskType == null) {
			throw new IllegalArgumentException("Parameter taskType must not be null");
		}

		if (aggregationInterval <= 0) {
			throw new IllegalArgumentException("Parameter aggregationInterval must be greater than zero");
		}

		if (taggingInterval <= 0 && taskType == TaskType.INPUT) {
			throw new IllegalArgumentException("Parameter taggingInterval must be greater than zero");
		}

		this.jobID = jobID;
		this.vertexID = vertexID;
		this.communicationThread = communicationThread;
		this.chainCoordinator = chainCoordinator;
		this.taskType = taskType;
		this.aggregationInterval = aggregationInterval;
		this.taggingInterval = taggingInterval;
	}

	public static StreamListenerContext createForInputTask(final JobID jobID, final ExecutionVertexID vertexID,
			final StreamingCommunicationThread communicationThread, final StreamChainCoordinator chainCoordinator,
			final int aggregationInterval, final int taggingInterval) {

		return new StreamListenerContext(jobID, vertexID, communicationThread, chainCoordinator, TaskType.INPUT,
			aggregationInterval, taggingInterval);
	}

	public static StreamListenerContext createForRegularTask(final JobID jobID, final ExecutionVertexID vertexID,
			final StreamingCommunicationThread communicationThread, final StreamChainCoordinator chainCoordinator,
			final int aggregationInterval) {

		return new StreamListenerContext(jobID, vertexID, communicationThread, chainCoordinator, TaskType.REGULAR,
			aggregationInterval, -1);
	}

	public static StreamListenerContext createForOutputTask(final JobID jobID, final ExecutionVertexID vertexID,
			final StreamingCommunicationThread communicationThread, final StreamChainCoordinator chainCoordinator,
			final int aggregationInterval) {

		return new StreamListenerContext(jobID, vertexID, communicationThread, chainCoordinator, TaskType.OUTPUT,
			aggregationInterval, -1);
	}

	boolean isInputVertex() {

		return (this.taskType == TaskType.INPUT);
	}

	boolean isOutputVertex() {

		return (this.taskType == TaskType.OUTPUT);
	}

	boolean isRegularVertex() {

		return (this.taskType == TaskType.REGULAR);
	}

	JobID getJobID() {

		return this.jobID;
	}

	ExecutionVertexID getVertexID() {

		return this.vertexID;
	}

	int getTaggingInterval() {

		return this.taggingInterval;
	}

	int getAggregationInterval() {

		return this.aggregationInterval;
	}

	void sendDataAsynchronously(final AbstractStreamingData data) throws InterruptedException {

		this.communicationThread.sendDataAsynchronously(data);
	}

	public void queuePendingAction(final AbstractAction action) {

		synchronized (this.pendingActions) {
			this.pendingActions.add(action);
		}
	}

	Queue<AbstractAction> getPendingActionsQueue() {

		return this.pendingActions;
	}

	void registerMapper(final Mapper<? extends Record, ? extends Record> mapper,
			final RecordReader<? extends Record> reader, final RecordWriter<? extends Record> writer) {

		this.chainCoordinator.registerMapper(this.vertexID, mapper, reader, writer);
	}
}
