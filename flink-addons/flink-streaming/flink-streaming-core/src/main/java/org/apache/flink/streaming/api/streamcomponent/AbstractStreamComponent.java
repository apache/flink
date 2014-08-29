/**
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

package org.apache.flink.streaming.api.streamcomponent;

import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.invokable.StreamInvokable;

public abstract class AbstractStreamComponent extends AbstractInvokable {

	protected StreamConfig configuration;
	protected int instanceID;
	protected String name;
	private static int numComponents = 0;
	protected boolean isMutable;
	protected Object function;
	protected String functionName;

	protected static int newComponent() {
		numComponents++;
		return numComponents;
	}

	@Override
	public void registerInputOutput() {
		initialize();
		setInputsOutputs();
		setInvokable();
	}
	
	protected void initialize() {
		this.configuration = new StreamConfig(getTaskConfiguration());
		this.name = configuration.getComponentName();
		this.isMutable = configuration.getMutability();
		this.functionName = configuration.getFunctionName();
		this.function = configuration.getFunction();
	}

	protected <T> void invokeUserFunction(StreamInvokable<T> userInvokable) throws Exception {
		userInvokable.open(getTaskConfiguration());
		userInvokable.invoke();
		userInvokable.close();
	}

	protected abstract void setInputsOutputs();

	protected abstract void setInvokable();

	public String getName() {
		return name;
	}

	public int getInstanceID() {
		return instanceID;
	}
}
