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

package org.apache.flink.runtime.operators.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.AbstractRuntimeUDFContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.broadcast.BroadcastVariableMaterialization;
import org.apache.flink.runtime.broadcast.InitializationTypeConflictException;

import com.google.common.base.Preconditions;

/**
 * A standalone implementation of the {@link RuntimeContext}, created by runtime UDF operators.
 */
public class DistributedRuntimeUDFContext extends AbstractRuntimeUDFContext {

	private final HashMap<String, BroadcastVariableMaterialization<?, ?>> broadcastVars = new HashMap<String, BroadcastVariableMaterialization<?, ?>>();
	
	
	public DistributedRuntimeUDFContext(String name, int numParallelSubtasks, int subtaskIndex, ClassLoader userCodeClassLoader) {
		super(name, numParallelSubtasks, subtaskIndex, userCodeClassLoader);
	}
	
	public DistributedRuntimeUDFContext(String name, int numParallelSubtasks, int subtaskIndex, ClassLoader userCodeClassLoader, Map<String, FutureTask<Path>> cpTasks) {
		super(name, numParallelSubtasks, subtaskIndex, userCodeClassLoader, cpTasks);
	}
	

	@Override
	public <T> List<T> getBroadcastVariable(String name) {
		Preconditions.checkNotNull(name);
		
		// check if we have an initialized version
		@SuppressWarnings("unchecked")
		BroadcastVariableMaterialization<T, ?> variable = (BroadcastVariableMaterialization<T, ?>) this.broadcastVars.get(name);
		if (variable != null) {
			try {
				return variable.getVariable();
			}
			catch (InitializationTypeConflictException e) {
				throw new RuntimeException("The broadcast variable '" + name + "' has been initialized by a prior call to a " + e.getType());
			}
		}
		else {
			throw new IllegalArgumentException("The broadcast variable with name '" + name + "' has not been set.");
		}
	}
	
	@Override
	public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
		if (name == null) {
			throw new NullPointerException("Thw broadcast variable name must not be null.");
		}
		if (initializer == null) {
			throw new NullPointerException("Thw broadcast variable initializer must not be null.");
		}
		
		// check if we have an initialized version
		@SuppressWarnings("unchecked")
		BroadcastVariableMaterialization<T, C> variable = (BroadcastVariableMaterialization<T, C>) this.broadcastVars.get(name);
		if (variable != null) {
			return variable.getVariable(initializer);
		}
		else {
			throw new IllegalArgumentException("The broadcast variable with name '" + name + "' has not been set.");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void setBroadcastVariable(String name, BroadcastVariableMaterialization<?, ?> value) {
		this.broadcastVars.put(name, value);
	}
	
	public void clearBroadcastVariable(String name) {
		this.broadcastVars.remove(name);
	}
	
	public void clearAllBroadcastVariables() {
		this.broadcastVars.clear();
	}
}
