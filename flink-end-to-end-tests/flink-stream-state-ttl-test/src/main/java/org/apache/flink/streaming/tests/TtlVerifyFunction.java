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
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.tests.verify.TtlStateVerifier;
import org.apache.flink.streaming.tests.verify.TtlUpdateContext;
import org.apache.flink.streaming.tests.verify.TtlValue;
import org.apache.flink.streaming.tests.verify.TtlVerificationContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class TtlVerifyFunction
	extends RichFlatMapFunction<TtlUpdateContext, String> implements CheckpointedFunction {
	private static final Logger LOG = LoggerFactory.getLogger(TtlVerifyFunction.class);

	@Nonnull
	private final Time precision;

	@Nonnull
	private transient Map<String, ListState<TtlValue<?>>> prevValueStates = new HashMap<>();

	TtlVerifyFunction(@Nonnull Time precision) {
		this.precision = precision;
	}

	@Override
	public void flatMap(TtlUpdateContext value, Collector<String> out) throws Exception {
		TtlStateVerifier<?, ?> verifier = TtlStateVerifier.VERIFIERS_BY_NAME.get(value.getVerifierId());
		List<TtlValue<?>> prevValues = StreamSupport.stream(prevValueStates.get(verifier.getId()).get().spliterator(), false).collect(Collectors.toList());
		TtlVerificationContext<?, ?> verificationContext = new TtlVerificationContext<>(prevValues, value);
		try {
			if (!verifier.verify(verificationContext, precision)) {
				out.collect(verificationContext.toString());
			}
		} catch (Throwable t) {
			LOG.error("Verification unexpected failure" + verificationContext, t);
		}
		prevValueStates.get(verifier.getId()).add(value.getUpdateWithTs());
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) {

	}

	@Override
	public void initializeState(FunctionInitializationContext context) {
		prevValueStates = TtlStateVerifier.VERIFIERS.stream()
			.collect(Collectors.toMap(TtlStateVerifier::getId, v -> {
				Preconditions.checkNotNull(v);
				TypeSerializer<TtlValue<?>> typeSerializer = new TtlValue.Serializer(v.getUpdateSerializer());
				ListStateDescriptor<TtlValue<?>> stateDesc = new ListStateDescriptor<>(
					"TtlPrevValueState_" + v.getId(), typeSerializer);
				KeyedStateStore store = context.getKeyedStateStore();
				return store.getListState(stateDesc);
			}));
	}
}
