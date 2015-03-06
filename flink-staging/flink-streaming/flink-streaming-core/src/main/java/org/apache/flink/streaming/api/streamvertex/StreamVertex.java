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

package org.apache.flink.streaming.api.streamvertex;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.BarrierTransceiver;
import org.apache.flink.runtime.jobgraph.tasks.OperatorStateCarrier;
import org.apache.flink.runtime.jobmanager.BarrierAck;
import org.apache.flink.runtime.jobmanager.StateBarrierAck;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.invokable.ChainableInvokable;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.streaming.io.IndexedReaderIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;

public class StreamVertex<IN, OUT> extends AbstractInvokable implements StreamTaskContext<OUT>,
		BarrierTransceiver, OperatorStateCarrier {

	private static final Logger LOG = LoggerFactory.getLogger(StreamVertex.class);

	private static int numTasks;

	protected StreamConfig configuration;
	protected int instanceID;
	private static int numVertices = 0;

	private InputHandler<IN> inputHandler;
	protected OutputHandler<OUT> outputHandler;
	private StreamInvokable<IN, OUT> userInvokable;

	private StreamingRuntimeContext context;
	private Map<String, OperatorState<?>> states;

	protected ClassLoader userClassLoader;

	private EventListener<TaskEvent> superstepListener;
	
	private boolean onRecovery;

	public StreamVertex() {
		userInvokable = null;
		numTasks = newVertex();
		instanceID = numTasks;
		superstepListener = new SuperstepEventListener();
	}

	protected static int newVertex() {
		numVertices++;
		return numVertices;
	}

	@Override
	public void registerInputOutput() {
		initialize();
		setInputsOutputs();
		setInvokable();
	}

	protected void initialize() {
		this.userClassLoader = getUserCodeClassLoader();
		this.configuration = new StreamConfig(getTaskConfiguration());
		if(!onRecovery)
		{
			this.states = configuration.getOperatorStates(userClassLoader);
		}
		this.context = createRuntimeContext(getEnvironment().getTaskName(), this.states);
	}

	protected <T> void invokeUserFunction(StreamInvokable<?, T> userInvokable) throws Exception {
		userInvokable.setRuntimeContext(context);
		userInvokable.open(getTaskConfiguration());

		for (ChainableInvokable<?, ?> invokable : outputHandler.chainedInvokables) {
			invokable.setRuntimeContext(context);
			invokable.open(getTaskConfiguration());
		}

		userInvokable.invoke();
		userInvokable.close();

		for (ChainableInvokable<?, ?> invokable : outputHandler.chainedInvokables) {
			invokable.close();
		}

	}

	@Override
	public void broadcastBarrier(long id) {
		//Only called at input vertices
		if (LOG.isDebugEnabled()) {
			LOG.debug("Received barrier from jobmanager: " + id);
		}
		actOnBarrier(id);
	}

	@Override
	public void confirmBarrier(long barrierID) {
		
		if(configuration.getStateMonitoring() && states != null)
		{
			getEnvironment().getJobManager().tell(
					new StateBarrierAck(getEnvironment().getJobID(), 
							getEnvironment().getJobVertexId(), context.getIndexOfThisSubtask(), 
							barrierID, states), ActorRef.noSender());
		}
		else
		{
			getEnvironment().getJobManager().tell(
					new BarrierAck(getEnvironment().getJobID(), getEnvironment().getJobVertexId(),
							context.getIndexOfThisSubtask(), barrierID), ActorRef.noSender());	
		}
		
	}

	public void setInputsOutputs() {
		inputHandler = new InputHandler<IN>(this);
		outputHandler = new OutputHandler<OUT>(this);
	}

	protected void setInvokable() {
		userInvokable = configuration.getUserInvokable(userClassLoader);
		userInvokable.setup(this);
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

		if (LOG.isDebugEnabled()) {
			LOG.debug("Task {} invoked with instance id {}", getName(), getInstanceID());
		}

		try {
			userInvokable.setRuntimeContext(context);
			userInvokable.open(getTaskConfiguration());

			for (ChainableInvokable<?, ?> invokable : outputHandler.chainedInvokables) {
				invokable.setRuntimeContext(context);
				invokable.open(getTaskConfiguration());
			}

			userInvokable.invoke();

			userInvokable.close();

			for (ChainableInvokable<?, ?> invokable : outputHandler.chainedInvokables) {
				invokable.close();
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Task {} invoke finished instance id {}", getName(), getInstanceID());
			}

		} catch (Exception e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("StreamInvokable failed due to: {}", StringUtils.stringifyException(e));
			}
			throw e;
		} finally {
			// Cleanup
			outputHandler.flushOutputs();
			clearBuffers();
		}

	}

	protected void clearBuffers() {
		if (outputHandler != null) {
			outputHandler.clearWriters();
		}
		if (inputHandler != null) {
			inputHandler.clearReaders();
		}
	}

	@Override
	public void cancel() {
		if (userInvokable != null) {
			userInvokable.cancel();
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

	private void actOnBarrier(long id) {
		try {
			outputHandler.broadcastBarrier(id);
			//TODO checkpoint state here
			confirmBarrier(id);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Superstep " + id + " processed: " + StreamVertex.this);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return configuration.getOperatorName() + " (" + context.getIndexOfThisSubtask() + ")";
	}

	@Override
	public void injectStates(Map<String,OperatorState<?>> states) {
		onRecovery = true;
		this.states.putAll(states);
	}
	

	private class SuperstepEventListener implements EventListener<TaskEvent> {

		@Override
		public void onEvent(TaskEvent event) {
			actOnBarrier(((StreamingSuperstep) event).getId());
		}

	}
}
