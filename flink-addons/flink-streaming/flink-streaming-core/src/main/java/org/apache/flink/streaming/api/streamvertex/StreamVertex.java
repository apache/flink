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
import org.apache.flink.streaming.api.invokable.StreamInvokable;
import org.apache.flink.streaming.state.OperatorState;

public class StreamVertex<IN, OUT> extends AbstractInvokable {

	private static int numTasks;

	protected StreamConfig configuration;
	protected int instanceID;
	protected String name;
	private static int numVertices = 0;
	protected boolean isMutable;
	protected Object function;
	protected String functionName;

	private InputHandler<IN> inputHandler;
	private OutputHandler<OUT> outputHandler;
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
		this.name = configuration.getVertexName();
		this.isMutable = configuration.getMutability();
		this.functionName = configuration.getFunctionName();
		this.function = configuration.getFunction(userClassLoader);
		this.states = configuration.getOperatorStates(userClassLoader);
		this.context = createRuntimeContext(name, this.states);
	}

	protected <T> void invokeUserFunction(StreamInvokable<?, T> userInvokable) throws Exception {
		userInvokable.setRuntimeContext(context);
		userInvokable.open(getTaskConfiguration());
		userInvokable.invoke();
		userInvokable.close();
	}

	public void setInputsOutputs() {
		inputHandler = new InputHandler<IN>(this);
		outputHandler = new OutputHandler<OUT>(this);
	}

	protected void setInvokable() {
		userInvokable = configuration.getUserInvokable(userClassLoader);
		userInvokable.initialize(outputHandler.getCollector(), inputHandler.getInputIter(),
				inputHandler.getInputSerializer(), isMutable);
	}

	public String getName() {
		return name;
	}

	public int getInstanceID() {
		return instanceID;
	}

	public StreamingRuntimeContext createRuntimeContext(String taskName,
			Map<String, OperatorState<?>> states) {
		Environment env = getEnvironment();
		return new StreamingRuntimeContext(taskName, env.getCurrentNumberOfSubtasks(),
				env.getIndexInSubtaskGroup(), getUserCodeClassLoader(), states, env.getCopyTask());
	}

	@Override
	public void invoke() throws Exception {
		outputHandler.invokeUserFunction("TASK", userInvokable);
	}
}
