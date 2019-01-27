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

package org.apache.flink.api.common.operators.base.utils;

import org.apache.flink.api.common.accumulators.AbstractAccumulatorRegistry;
import org.apache.flink.api.common.accumulators.Accumulator;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A fake accumulator registry implementation for test cases.
 */
public class TestAccumulatorRegistry extends AbstractAccumulatorRegistry {

	@Override
	public <V, A extends Serializable> void addPreAggregatedAccumulator(String name, Accumulator<V, A> accumulator) {

	}

	@Override
	public Map<String, Accumulator<?, ?>> getPreAggregatedAccumulators() {
		return null;
	}

	@Override
	public void commitPreAggregatedAccumulator(String name) {

	}

	@Override
	public <V, A extends Serializable> CompletableFuture<Accumulator<V, A>> queryPreAggregatedAccumulator(String name) {
		return new CompletableFuture<>();
	}
}
