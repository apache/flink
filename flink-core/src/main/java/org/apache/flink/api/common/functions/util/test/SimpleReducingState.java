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

package org.apache.flink.api.common.functions.util.test;

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;

/**
 * A simple {@link ReducingState} for testing.
 */
public class SimpleReducingState<T> implements ReducingState<T> {

	private ReducingStateDescriptor<T> descriptor;
	private T value;

	public SimpleReducingState(ReducingStateDescriptor<T> descriptor) {
		this.descriptor = descriptor;
	}

	@Override
	public T get() throws Exception {
		return value;
	}

	@Override
	public void add(T value) throws Exception {
		this.value = descriptor.getReduceFunction().reduce(this.value, value);
	}

	@Override
	public void clear() {
		value = null;
	}
}
