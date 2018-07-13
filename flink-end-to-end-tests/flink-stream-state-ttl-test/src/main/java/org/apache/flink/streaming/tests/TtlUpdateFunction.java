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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateTtlConfiguration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.tests.verify.TtlStateVerifier;
import org.apache.flink.streaming.tests.verify.TtlUpdateContext;
import org.apache.flink.util.Collector;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.stream.Collectors;

class TtlUpdateFunction
	extends RichFlatMapFunction<TtlStateUpdate, TtlUpdateContext> implements CheckpointedFunction {
	@Nonnull
	private final StateTtlConfiguration ttlConfig;
	private Map<String, State> states;

	TtlUpdateFunction(@Nonnull StateTtlConfiguration ttlConfig) {
		this.ttlConfig = ttlConfig;
	}

	@Override
	public void flatMap(TtlStateUpdate value, Collector<TtlUpdateContext> out) throws Exception {
		for (TtlStateVerifier<?, ?> verifier : TtlStateVerifier.VERIFIERS) {
			State state = states.get(verifier.getId());
			Object valueBeforeUpdate = verifier.get(state);
			Object update = value.getUpdate(verifier.getId());
			verifier.update(state, update);
			Object updatedValue = verifier.get(state);
			TtlUpdateContext updateContext =
				new TtlUpdateContext<>(value.getKey(), verifier.getId(), valueBeforeUpdate, update, updatedValue);
			out.collect(updateContext);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) {

	}

	@Override
	public void initializeState(FunctionInitializationContext context) {
		states = TtlStateVerifier.VERIFIERS.stream()
			.collect(Collectors.toMap(TtlStateVerifier::getId, v -> v.createState(context, ttlConfig)));
	}
}
