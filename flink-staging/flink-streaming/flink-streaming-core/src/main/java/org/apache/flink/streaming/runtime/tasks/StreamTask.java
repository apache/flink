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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.functors.NotNullPredicate;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCommittingOperator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointedOperator;
import org.apache.flink.runtime.jobgraph.tasks.OperatorStateCarrier;
import org.apache.flink.runtime.state.LocalStateHandle;
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


public class StreamTask<IN, OUT> extends AbstractInvokable implements StreamTaskContext<OUT>,
		OperatorStateCarrier<LocalStateHandle>, CheckpointedOperator, CheckpointCommittingOperator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	private final Object checkpointLock = new Object();
	
	private static int numTasks;

	protected StreamConfig configuration;
	protected int instanceID;
	private static int numVertices = 0;

	private InputHandler<IN> inputHandler;
	protected OutputHandler<OUT> outputHandler;
	private StreamOperator<IN, OUT> streamOperator;
	private boolean chained;
	protected volatile boolean isRunning = false;

	private StreamingRuntimeContext context;

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
		this.context = createRuntimeContext(getEnvironment().getTaskName());
	}

	public void setInputsOutputs() {
		inputHandler = new InputHandler<IN>(this);
		outputHandler = new OutputHandler<OUT>(this);
		chained = !outputHandler.getChainedOperators().isEmpty();
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

	public StreamingRuntimeContext createRuntimeContext(String taskName) {
		Environment env = getEnvironment();
		return new StreamingRuntimeContext(taskName, env, getUserCodeClassLoader(),
				getExecutionConfig());
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

		for (ChainableStreamOperator<?, ?> operator : outputHandler.getChainedOperators()) {
			operator.setRuntimeContext(context);
			operator.open(getTaskConfiguration());
		}
	}

	protected void closeOperator() throws Exception {
		streamOperator.close();

		for (ChainableStreamOperator<?, ?> operator : outputHandler.getChainedOperators()) {
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

	// ------------------------------------------------------------------------
	//  Checkpoint and Restore
	// ------------------------------------------------------------------------

	/**
	 * Re-injects the user states into the map. Also set the state on the functions.
	 */
	@Override
	public void setInitialState(LocalStateHandle stateHandle) throws Exception {
		// here, we later resolve the state handle into the actual state by
		// loading the state described by the handle from the backup store
		Serializable state = stateHandle.getState();

		if (chained) {
			@SuppressWarnings("unchecked")
			List<Serializable> chainedStates = (List<Serializable>) state;

			Serializable headState = chainedStates.get(0);
			if (headState != null) {
				streamOperator.restoreInitialState(headState);
			}

			for (int i = 1; i < chainedStates.size(); i++) {
				Serializable chainedState = chainedStates.get(i);
				if (chainedState != null) {
					outputHandler.getChainedOperators().get(i - 1).restoreInitialState(chainedState);
				}
			}

		} else {
			streamOperator.restoreInitialState(state);
		}
	}

	/**
	 * This method is either called directly by the checkpoint coordinator, or called
	 * when all incoming channels have reported a barrier
	 * 
	 * @param checkpointId
	 * @param timestamp
	 * @throws Exception
	 */
	@Override
	public void triggerCheckpoint(long checkpointId, long timestamp) throws Exception {
		
		synchronized (checkpointLock) {
			if (isRunning) {
				try {
					LOG.info("Starting checkpoint {} on task {}", checkpointId, getName());
					
					// first draw the state that should go into checkpoint
					LocalStateHandle state;
					try {
						Serializable userState = streamOperator.getStateSnapshotFromFunction(
								checkpointId, timestamp);
						
						if (chained) {
							// We construct a list of states for chained tasks
							List<Serializable> chainedStates = new ArrayList<Serializable>();

							chainedStates.add(userState);

							for (StreamOperator<?, ?> chainedOperator : outputHandler.getChainedOperators()) {
								chainedStates.add(chainedOperator.getStateSnapshotFromFunction(
										checkpointId, timestamp));
							}

							userState = CollectionUtils.exists(chainedStates,
									NotNullPredicate.INSTANCE) ? (Serializable) chainedStates
									: null;
						}
						
						state = userState == null ? null : new LocalStateHandle(userState);
					}
					catch (Exception e) {
						throw new Exception("Error while drawing snapshot of the user state.", e);
					}
			
					// now emit the checkpoint barriers
					outputHandler.broadcastBarrier(checkpointId, timestamp);
					
					// now confirm the checkpoint
					if (state == null) {
						getEnvironment().acknowledgeCheckpoint(checkpointId);
					} else {
						getEnvironment().acknowledgeCheckpoint(checkpointId, state);
					}
				}
				catch (Exception e) {
					if (isRunning) {
						throw e;
					}
				}
			}
		}
	}

	@Override
	public void confirmCheckpoint(long checkpointId, long timestamp) throws Exception {
		// we do nothing here so far. this should call commit on the source function, for example
		synchronized (checkpointLock) {
			streamOperator.confirmCheckpointCompleted(checkpointId, timestamp);
			if (chained) {
				for (StreamOperator<?, ?> op : outputHandler.getChainedOperators()) {
					op.confirmCheckpointCompleted(checkpointId, timestamp);
				}
			}
		}
	}
	
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return getEnvironment().getTaskNameWithSubtasks();
	}

	// ------------------------------------------------------------------------

	private class SuperstepEventListener implements EventListener<TaskEvent> {

		@Override
		public void onEvent(TaskEvent event) {
			try {
				StreamingSuperstep sStep = (StreamingSuperstep) event;
				triggerCheckpoint(sStep.getId(), sStep.getTimestamp());
			}
			catch (Exception e) {
				throw new RuntimeException("Error triggering a checkpoint as the result of receiving checkpoint barrier", e);
			}
		}
	}
}
