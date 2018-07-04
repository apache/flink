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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Test suite for {@link TtlAggregatingState}. */
public class TtlAggregatingStateTest
	extends TtlMergingStateBase.TtlIntegerMergingStateBase<TtlAggregatingState<?, String, Integer, Long, String>, Integer, String> {
	private static final long DEFAULT_ACCUMULATOR = 3L;

	@Override
	void initTestValues() {
		updater = v -> ttlState.add(v);
		getter = () -> ttlState.get();
		originalGetter = () -> ttlState.original.get();

		updateEmpty = 5;
		updateUnexpired = 7;
		updateExpired = 6;

		getUpdateEmpty = "8";
		getUnexpired = "15";
		getUpdateExpired = "9";
	}

	@Override
	TtlAggregatingState<?, String, Integer, Long, String> createState() {
		AggregatingStateDescriptor<Integer, Long, String> aggregatingStateDes =
			new AggregatingStateDescriptor<>("TtlTestAggregatingState", AGGREGATE, LongSerializer.INSTANCE);
		return (TtlAggregatingState<?, String, Integer, Long, String>) wrapMockState(aggregatingStateDes);
	}

	@Override
	String getMergeResult(
		List<Tuple2<String, Integer>> unexpiredUpdatesToMerge,
		List<Tuple2<String, Integer>> finalUpdatesToMerge) {
		Set<String> namespaces = new HashSet<>();
		unexpiredUpdatesToMerge.forEach(t -> namespaces.add(t.f0));
		finalUpdatesToMerge.forEach(t -> namespaces.add(t.f0));
		return Integer.toString(getIntegerMergeResult(unexpiredUpdatesToMerge, finalUpdatesToMerge) +
			namespaces.size() * (int) DEFAULT_ACCUMULATOR);
	}

	private static final AggregateFunction<Integer, Long, String> AGGREGATE =
		new AggregateFunction<Integer, Long, String>() {
			@Override
			public Long createAccumulator() {
				return DEFAULT_ACCUMULATOR;
			}

			@Override
			public Long add(Integer value, Long accumulator) {
				return accumulator + value;
			}

			@Override
			public String getResult(Long accumulator) {
				return accumulator.toString();
			}

			@Override
			public Long merge(Long a, Long b) {
				return a + b;
			}
		};
}
