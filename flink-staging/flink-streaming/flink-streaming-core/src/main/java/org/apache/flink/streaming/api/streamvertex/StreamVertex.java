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

import java.util.Map;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.invokable.ChainableInvokable;
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.CoReaderIterator;
import org.apache.flink.streaming.state.OperatorState;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public class StreamVertex<IN, OUT> extends AbstractInvokable implements StreamTaskContext<OUT> {

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

	public StreamVertex() {
		userInvokable = null;
		numTasks = newVertex();
		instanceID = numTasks;
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
		this.states = configuration.getOperatorStates(userClassLoader);
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
		return new StreamingRuntimeContext(taskName, env, getUserCodeClassLoader(), states);
	}

	@Override
	public void invoke() throws Exception {
		outputHandler.invokeUserFunction("TASK", userInvokable);
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
}
