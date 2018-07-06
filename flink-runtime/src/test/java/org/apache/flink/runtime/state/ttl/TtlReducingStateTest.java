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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

/** Test suite for {@link TtlReducingState}. */
public class TtlReducingStateTest
	extends TtlMergingStateBase.TtlIntegerMergingStateBase<TtlReducingState<?, String, Integer>, Integer, Integer> {
	@Override
	void initTestValues() {
		updater = v -> ttlState.add(v);
		getter = () -> ttlState.get();
		originalGetter = () -> ttlState.original.get();

		updateEmpty = 5;
		updateUnexpired = 7;
		updateExpired = 6;

		getUpdateEmpty = 5;
		getUnexpired = 12;
		getUpdateExpired = 6;
	}

	@Override
	TtlReducingState<?, String, Integer> createState() {
		ReducingStateDescriptor<Integer> aggregatingStateDes =
			new ReducingStateDescriptor<>("TtlTestReducingState", REDUCE, IntSerializer.INSTANCE);
		return (TtlReducingState<?, String, Integer>) wrapMockState(aggregatingStateDes);
	}

	@Override
	Integer getMergeResult(
		List<Tuple2<String, Integer>> unexpiredUpdatesToMerge,
		List<Tuple2<String, Integer>> finalUpdatesToMerge) {
		return getIntegerMergeResult(unexpiredUpdatesToMerge, finalUpdatesToMerge);
	}

	private static final ReduceFunction<Integer> REDUCE = (v1, v2) -> {
		if (v1 == null && v2 == null) {
			return null;
		} else if (v1 == null) {
			return v2;
		} else if (v2 == null) {
			return v1;
		} else {
			return v1 + v2;
		}
	};
}
