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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.runtime.state.internal.InternalFoldingState;

/** Test suite for {@link TtlFoldingState}. */
public class TtlFoldingStateTest extends TtlStateTestBase<TtlFoldingState<?, String, Long, String>, Long, String> {
	@Override
	TtlFoldingState<?, String, Long, String> createState() {
		FoldFunction<Long, TtlValue<String>> ttlFoldFunction = new TtlFoldFunction<>(FOLD, ttlConfig, timeProvider, "1");
		return new TtlFoldingState<>(
			new MockInternalFoldingState<>(ttlFoldFunction), ttlConfig, timeProvider, null);
	}

	@Override
	void initTestValues() {
		updater = v -> ttlState.add(v);
		getter = () -> ttlState.get();
		originalGetter = () -> ttlState.original.get();

		updateEmpty = 5L;
		updateUnexpired = 7L;
		updateExpired = 6L;

		getUpdateEmpty = "6";
		getUnexpired = "13";
		getUpdateExpired = "7";
	}

	private static class MockInternalFoldingState<K, N, T, ACC>
		extends MockInternalKvState<K, N, ACC> implements InternalFoldingState<K, N, T, ACC> {
		private final FoldFunction<T, ACC> foldFunction;

		private MockInternalFoldingState(FoldFunction<T, ACC> foldFunction) {
			this.foldFunction = foldFunction;
		}

		@Override
		public ACC get() {
			return getInternal();
		}

		@Override
		public void add(T value) throws Exception {
			updateInternal(foldFunction.fold(get(), value));
		}
	}

	private static final FoldFunction<Long, String> FOLD = (acc, val) -> {
		long lacc = acc == null ? 0 : Long.parseLong(acc);
		return Long.toString(val == null ? lacc : lacc + val);
	};
}
