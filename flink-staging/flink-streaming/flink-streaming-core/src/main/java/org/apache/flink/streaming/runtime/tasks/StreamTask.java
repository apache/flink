/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.BarrierTransceiver;
import org.apache.flink.runtime.jobgraph.tasks.OperatorStateCarrier;
import org.apache.flink.runtime.messages.CheckpointingMessages;
import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.ChainableStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.CoReaderIterator;
import org.apache.flink.streaming.runtime.io.IndexedReaderIterator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;

public class StreamTask<IN, OUT> extends AbstractInvokable implements StreamTaskContext<OUT>,
		BarrierTransceiver, OperatorStateCarrier {

	private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	private static int numTasks;

	protected StreamConfig configuration;
	protected int instanceID;
	private static int numVertices = 0;

	private InputHandler<IN> inputHandler;
	protected OutputHandler<OUT> outputHandler;
	private StreamOperator<IN, OUT> streamOperator;
	protected volatile boolean isRunning = false;

	private StreamingRuntimeContext context;
	private Map<String, OperatorState<?>> states;

	protected ClassLoader userClassLoader;

	private EventListener<TaskEvent> superstepListener;

	public StreamTask() {
		streamOperator = null;
		numTasks = newTask();
		instanceID = numTasks;
		superstepListener = new SuperstepEventListener();
	}

	protected static int newTask() {
		numVertices++;
		return numVertices;
	}

	@Override
	public void registerInputOutput() {
		initialize();
		setInputsOutputs();
		setOperator();
	}

	protected void initialize() {
		this.userClassLoader = getUserCodeClassLoader();
		this.configuration = new StreamConfig(getTaskConfiguration());
		this.states = new HashMap<String, OperatorState<?>>();
		this.context = createRuntimeContext(getEnvironment().getTaskName(), this.states);
	}

	@Override
	public void broadcastBarrierFromSource(long id) {
		// Only called at input vertices
		if (LOG.isDebugEnabled()) {
			LOG.debug("Received barrier from jobmanager: " + id);
		}
		actOnBarrier(id);
	}

	/**
	 * This method is called to confirm that a barrier has been fully processed.
	 * It sends an acknowledgment to the jobmanager. In the current version if
	 * there is user state it also checkpoints the state to the jobmanager.
	 */
	@Override
	public void confirmBarrier(long barrierID) throws IOException {

		if (configuration.getStateMonitoring() && !states.isEmpty()) {
			getEnvironment().getJobManager().tell(
					new CheckpointingMessages.StateBarrierAck(getEnvironment().getJobID(), getEnvironment()
							.getJobVertexId(), context.getIndexOfThisSubtask(), barrierID,
							new LocalStateHandle(states)), ActorRef.noSender());
		} else {
			getEnvironment().getJobManager().tell(
					new CheckpointingMessages.BarrierAck(getEnvironment().getJobID(), getEnvironment().getJobVertexId(),
							context.getIndexOfThisSubtask(), barrierID), ActorRef.noSender());
		}

	}

	public void setInputsOutputs() {
		inputHandler = new InputHandler<IN>(this);
		outputHandler = new OutputHandler<OUT>(this);
	}

	protected void setOperator() {
		streamOperator = configuration.getStreamOperator(userClassLoader);
		streamOperator.setup(this);
	}

	public String getName() {
		return getEnvironment().getTaskName();
	}

	public int getInstanceID() {
		return instanceID;
	}

	public StreamingRuntimeContext createRuntimeContext(String taskName,
			Map<String, OperatorState<?>> states) {
		Environment env = getEnvironment();
		return new StreamingRuntimeContext(taskName, env, getUserCodeClassLoader(),
				getExecutionConfig(), states);
	}

	@Override
	public void invoke() throws Exception {
		this.isRunning = true;

		boolean operatorOpen = false;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Task {} invoked with instance id {}", getName(), getInstanceID());
		}

		try {
			streamOperator.setRuntimeContext(context);

			operatorOpen = true;
			openOperator();

			streamOperator.run();

			closeOperator();
			operatorOpen = false;

			if (LOG.isDebugEnabled()) {
				LOG.debug("Task {} invoke finished instance id {}", getName(), getInstanceID());
			}

		} catch (Exception e) {

			if (operatorOpen) {
				try {
					closeOperator();
				} catch (Throwable t) {
				}
			}

			if (LOG.isErrorEnabled()) {
				LOG.error("StreamOperator failed due to: {}", StringUtils.stringifyException(e));
			}
			throw e;
		} finally {
			this.isRunning = false;
			// Cleanup
			outputHandler.flushOutputs();
			clearBuffers();
		}

	}

	protected void openOperator() throws Exception {
		streamOperator.open(getTaskConfiguration());

		for (ChainableStreamOperator<?, ?> operator : outputHandler.chainedOperators) {
			operator.setRuntimeContext(context);
			operator.open(getTaskConfiguration());
		}
	}

	protected void closeOperator() throws Exception {
		streamOperator.close();

		for (ChainableStreamOperator<?, ?> operator : outputHandler.chainedOperators) {
			operator.close();
		}
	}

	protected void clearBuffers() throws IOException {
		if (outputHandler != null) {
			outputHandler.clearWriters();
		}
		if (inputHandler != null) {
			inputHandler.clearReaders();
		}
	}

	@Override
	public void cancel() {
		if (streamOperator != null) {
			streamOperator.cancel();
		}
	}

	@Override
	public StreamConfig getConfig() {
		return configuration;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> MutableObjectIterator<X> getInput(int index) {
		if (index == 0) {
			return (MutableObjectIterator<X>) inputHandler.getInputIter();
		} else {
			throw new IllegalArgumentException("There is only 1 input");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> IndexedReaderIterator<X> getIndexedInput(int index) {
		if (index == 0) {
			return (IndexedReaderIterator<X>) inputHandler.getInputIter();
		} else {
			throw new IllegalArgumentException("There is only 1 input");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> StreamRecordSerializer<X> getInputSerializer(int index) {
		if (index == 0) {
			return (StreamRecordSerializer<X>) inputHandler.getInputSerializer();
		} else {
			throw new IllegalArgumentException("There is only 1 input");
		}
	}

	@Override
	public Collector<OUT> getOutputCollector() {
		return outputHandler.getCollector();
	}

	@Override
	public <X, Y> CoReaderIterator<X, Y> getCoReader() {
		throw new IllegalArgumentException("CoReader not available");
	}

	public EventListener<TaskEvent> getSuperstepListener() {
		return this.superstepListener;
	}

	/**
	 * Method to be called when a barrier is received from all the input
	 * channels. It should broadcast the barrier to the output operators,
	 * checkpoint the state and send an ack.
	 * 
	 * @param id
	 */
	private synchronized void actOnBarrier(long id) {
		if (isRunning) {
			try {
				outputHandler.broadcastBarrier(id);
				confirmBarrier(id);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Superstep " + id + " processed: " + StreamTask.this);
				}
			} catch (Exception e) {
				// Only throw any exception if the vertex is still running
				if (isRunning) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	@Override
	public String toString() {
		return configuration.getOperatorName() + " (" + context.getIndexOfThisSubtask() + ")";
	}

	/**
	 * Re-injects the user states into the map
	 */
	@Override
	public void injectState(StateHandle stateHandle) {
		this.states.putAll(stateHandle.getState(userClassLoader));
	}

	private class SuperstepEventListener implements EventListener<TaskEvent> {

		@Override
		public void onEvent(TaskEvent event) {
			actOnBarrier(((StreamingSuperstep) event).getId());
		}

	}
}
