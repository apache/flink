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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.internal.InternalListState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/** Test suite for {@link TtlListState}. */
public class TtlListStateTest
	extends TtlMergingStateBase<TtlListState<?, String, Integer>, List<Integer>, Iterable<Integer>> {
	@Override
	TtlListState<?, String, Integer> createState() {
		return new TtlListState<>(new MockInternalListState<>(), ttlConfig, timeProvider, null);
	}

	@Override
	void initTestValues() {
		updater = v -> ttlState.addAll(v);
		getter = () -> StreamSupport.stream(ttlState.get().spliterator(), false).collect(Collectors.toList());
		originalGetter = () -> ttlState.original.get();

		emptyValue = Collections.emptyList();

		updateEmpty = Arrays.asList(5, 7, 10);
		updateUnexpired = Arrays.asList(8, 9, 11);
		updateExpired = Arrays.asList(1, 4);

		getUpdateEmpty = updateEmpty;
		getUnexpired = updateUnexpired;
		getUpdateExpired = updateExpired;
	}

	@Override
	List<Integer> generateRandomUpdate() {
		int size = RANDOM.nextInt(5);
		return IntStream.range(0, size).mapToObj(i -> RANDOM.nextInt(100)).collect(Collectors.toList());
	}

	@Override
	Iterable<Integer> getMergeResult(
		List<Tuple2<String, List<Integer>>> unexpiredUpdatesToMerge,
		List<Tuple2<String, List<Integer>>> finalUpdatesToMerge) {
		List<Integer> result = new ArrayList<>();
		finalUpdatesToMerge.forEach(t -> result.addAll(t.f1));
		return result;
	}

	private static class MockInternalListState<K, N, T>
		extends MockInternalMergingState<K, N, T, List<T>, Iterable<T>>
		implements InternalListState<K, N, T> {

		MockInternalListState() {
			super(ArrayList::new);
		}

		@Override
		public void update(List<T> elements) {
			updateInternal(elements);
		}

		@Override
		public void addAll(List<T> elements) {
			getInternal().addAll(elements);
		}

		@Override
		List<T> mergeState(List<T> acc, List<T> nAcc) {
			acc = new ArrayList<>(acc);
			acc.addAll(nAcc);
			return acc;
		}

		@Override
		public Iterable<T> get() {
			return getInternal();
		}

		@Override
		public void add(T element) {
			getInternal().add(element);
		}

		@Override
		public void clear() {
			getInternal().clear();
		}
	}
}
