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

package org.apache.flink.api.common.functions.util;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.PriorityQueue;

/**
 * A standalone implementation of the {@link RuntimeContext}, created by runtime UDF operators.
 */
public class RuntimeUDFContext extends AbstractRuntimeUDFContext {

	private final HashMap<String, Object> initializedBroadcastVars = new HashMap<String, Object>();
	
	private final HashMap<String, List<?>> uninitializedBroadcastVars = new HashMap<String, List<?>>();

	public RuntimeUDFContext(String name, int numParallelSubtasks, int subtaskIndex, ClassLoader userCodeClassLoader,
							ExecutionConfig executionConfig, Map<String, Future<Path>> cpTasks, Map<String, Accumulator<?,?>> accumulators) {
		super(name, numParallelSubtasks, subtaskIndex, userCodeClassLoader, executionConfig, accumulators, cpTasks);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <RT> List<RT> getBroadcastVariable(String name) {
		
		// check if we have an initialized version
		Object o = this.initializedBroadcastVars.get(name);
		if (o != null) {
			if (o instanceof List) {
				return (List<RT>) o;
			}
			else {
				throw new IllegalStateException("The broadcast variable with name '" + name + 
						"' is not a List. A different call must have requested this variable with a BroadcastVariableInitializer.");
			}
		}
		else {
			List<?> uninitialized = this.uninitializedBroadcastVars.remove(name);
			if (uninitialized != null) {
				this.initializedBroadcastVars.put(name, uninitialized);
				return (List<RT>) uninitialized;
			}
			else {
				throw new IllegalArgumentException("The broadcast variable with name '" + name + "' has not been set.");
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
		
		// check if we have an initialized version
		Object o = this.initializedBroadcastVars.get(name);
		if (o != null) {
			return (C) o;
		}
		else {
			List<T> uninitialized = (List<T>) this.uninitializedBroadcastVars.remove(name);
			if (uninitialized != null) {
				C result = initializer.initializeBroadcastVariable(uninitialized);
				this.initializedBroadcastVars.put(name, result);
				return result;
			}
			else {
				throw new IllegalArgumentException("The broadcast variable with name '" + name + "' has not been set.");
			}
		}
	}

	/**
	 *
	 * Wrap a java.util.PriorityQueue for standalone RuntimeContext.
	 */
	@Override
	public <T> PriorityQueue<T> getPriorityQueue(final TypeInformation<T> typeInformation, final int k, final boolean order) throws Exception {
		return new PriorityQueue<T>() {

			private java.util.PriorityQueue<T> javaHeap = new java.util.PriorityQueue<T>(k, new Comparator<T>() {

				TypeComparator<T> typeComparator = createComparator(typeInformation, order, null);

				@Override
				public int compare(T first, T second) {
					return typeComparator.compare(first, second);
				}
			});

			@Override
			public void insert(T element) throws IOException {
				this.javaHeap.add(element);
			}

			@Override
			public T next() throws IOException {
				return this.javaHeap.poll();
			}

			@Override
			public T next(T reuse) throws IOException {
				return this.javaHeap.poll();
			}

			@Override
			public int size() {
				return this.javaHeap.size();
			}

			@Override
			public void close() throws IOException {
				this.javaHeap.clear();
			}
		};
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void setBroadcastVariable(String name, List<?> value) {
		this.uninitializedBroadcastVars.put(name, value);
		this.initializedBroadcastVars.remove(name);
	}
	
	public void clearBroadcastVariable(String name) {
		this.uninitializedBroadcastVars.remove(name);
		this.initializedBroadcastVars.remove(name);
	}
	
	public void clearAllBroadcastVariables() {
		this.uninitializedBroadcastVars.clear();
		this.initializedBroadcastVars.clear();
	}
}
