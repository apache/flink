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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Base interface for bundle processing.
 *
 * <p>finishBundle method take a bundle of elements and transform them into zero, one, or more elements.
 * Typical applications can be combining a bundle of elements to a single value or aggregating on a bundle of elements.
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the returned elements.
 */
public abstract class BundleFunction<K, V, IN, OUT> implements Function {
	private static final long serialVersionUID = -6672219582127325882L;

	protected transient ExecutionContext ctx;

	public void open(ExecutionContext ctx) throws Exception {
		this.ctx = ctx;
	}

	public void close() throws Exception {}

	public void endInput(Collector<OUT> out) throws Exception {}

	protected RuntimeContext getRuntimeContext() {
		if (this.ctx != null) {
			return this.ctx.getRuntimeContext();
		} else {
			throw new IllegalStateException("The stream exec runtime context has not been initialized.");
		}
	}

	/**
	 * Adds the given input to the given value, returning the new accumulator value.
	 * @param value the existing value, maybe null
	 * @param input the given input, not null
	 */
	public abstract V addInput(@Nullable V value, IN input);

	/**
	 * Merges two values, returning an value with the merged value.
	 * This is happened when local bundle restoring from state after parallelism is changed,
	 * maybe some same key-value pairs is located in the same task.
	 *
	 * @param value1 the existing value, maybe null
	 * @param value2 the merged value, not null
	 */
	public V mergeValue(@Nullable V value1, V value2) {
		throw new RuntimeException("Local bundle function should implement merge() method.");
	}

	/**
	 * Called when a bundle is finished. Transform a bundle to zero, one, or more output elements.
	 */
	public abstract void finishBundle(Map<K, V> buffer, Collector<OUT> out) throws Exception;
}
