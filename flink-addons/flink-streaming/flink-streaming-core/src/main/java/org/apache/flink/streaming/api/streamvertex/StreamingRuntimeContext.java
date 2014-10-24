/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.streamvertex;

import java.util.Map;
import java.util.concurrent.FutureTask;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.state.OperatorState;

/**
 * Implementation of the {@link RuntimeContext}, created by runtime stream UDF
 * operators.
 */
public class StreamingRuntimeContext extends RuntimeUDFContext {

	private final Map<String, OperatorState<?>> operatorStates;

	public StreamingRuntimeContext(String name, int numParallelSubtasks, int subtaskIndex,
			ClassLoader userCodeClassLoader, Map<String, OperatorState<?>> operatorStates) {
		super(name, numParallelSubtasks, subtaskIndex, userCodeClassLoader);
		this.operatorStates = operatorStates;
	}

	public StreamingRuntimeContext(String name, int numParallelSubtasks, int subtaskIndex,
			ClassLoader userCodeClassLoader, Map<String, OperatorState<?>> operatorStates,
			Map<String, FutureTask<Path>> cpTasks) {
		super(name, numParallelSubtasks, subtaskIndex, userCodeClassLoader, cpTasks);
		this.operatorStates = operatorStates;
	}

	/**
	 * Returns the operator state registered by the given name for the operator.
	 * 
	 * 
	 * @param name
	 *            Name of the operator state to be returned.
	 * @return The operator state.
	 */
	public OperatorState<?> getState(String name) {
		if (operatorStates == null) {
			throw new RuntimeException("No state has been registered for this operator.");
		} else {
			OperatorState<?> state = operatorStates.get(name);
			if (state != null) {
				return state;
			} else {
				throw new RuntimeException("No state has been registered for the name: " + name);
			}
		}

	}

}
