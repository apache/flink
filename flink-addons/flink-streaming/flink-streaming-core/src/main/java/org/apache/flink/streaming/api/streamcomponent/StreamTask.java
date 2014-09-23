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

package org.apache.flink.streaming.api.streamcomponent;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.invokable.StreamOperatorInvokable;

public class StreamTask<IN extends Tuple, OUT extends Tuple> extends AbstractStreamComponent {

	private InputHandler<IN> inputHandler;
	private OutputHandler<OUT> outputHandler;

	private StreamOperatorInvokable<IN, OUT> userInvokable;
	
	private static int numTasks;

	public StreamTask() {
		userInvokable = null;
		numTasks = newComponent();
		instanceID = numTasks;
	}

	@Override
	public void setInputsOutputs() {
		inputHandler = new InputHandler<IN>(this);
		outputHandler = new OutputHandler<OUT>(this);
	}

	@Override
	protected void setInvokable() {
		userInvokable = configuration.getUserInvokable();
		userInvokable.initialize(outputHandler.getCollector(), inputHandler.getInputIter(),
				inputHandler.getInputSerializer(), isMutable);
	}

	@Override
	public void invoke() throws Exception {
		outputHandler.invokeUserFunction("TASK", userInvokable);
	}
}
