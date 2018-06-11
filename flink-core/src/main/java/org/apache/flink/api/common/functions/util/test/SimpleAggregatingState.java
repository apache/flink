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

import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;

/**
 * A simple {@link AggregatingState} for testing.
 */
public class SimpleAggregatingState<IN, ACC, OUT> implements AggregatingState<IN, OUT> {

	private AggregatingStateDescriptor<IN, ACC, OUT> descriptor;
	private ACC accumulator;

	public SimpleAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> descriptor) {
		this.descriptor = descriptor;
		this.accumulator = this.descriptor.getAggregateFunction().createAccumulator();
	}

	@Override
	public OUT get() throws Exception {
		return descriptor.getAggregateFunction().getResult(accumulator);
	}

	@Override
	public void add(IN value) throws Exception {
		this.accumulator = descriptor.getAggregateFunction().add(value, accumulator);
	}

	@Override
	public void clear() {
		accumulator = null;
	}
}
