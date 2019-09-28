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

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/** Base test suite for {@link TtlListState}. */
abstract class TtlListStateTestContextBase<T>
	extends TtlMergingStateTestContext<TtlListState<?, String, T>, List<T>, Iterable<T>> {
	private final TypeSerializer<T> serializer;

	TtlListStateTestContextBase(TypeSerializer<T> serializer) {
		this.serializer = serializer;
	}

	@Override
	public void update(List<T> value) throws Exception {
		ttlState.addAll(value);
	}

	@Override
	public Iterable<T> get() throws Exception {
		return StreamSupport.stream(ttlState.get().spliterator(), false).collect(Collectors.toList());
	}

	@Override
	public Object getOriginal() throws Exception {
		return ttlState.original.get() == null ? emptyValue : ttlState.original.get();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <US extends State, SV> StateDescriptor<US, SV> createStateDescriptor() {
		return (StateDescriptor<US, SV>) new ListStateDescriptor<>(getName(), serializer);
	}

	@Override
	List<T> generateRandomUpdate() {
		int size = RANDOM.nextInt(5);
		return IntStream.range(0, size).mapToObj(this::generateRandomElement).collect(Collectors.toList());
	}

	abstract T generateRandomElement(int i);

	@Override
	Iterable<T> getMergeResult(
		List<Tuple2<String, List<T>>> unexpiredUpdatesToMerge,
		List<Tuple2<String, List<T>>> finalUpdatesToMerge) {
		List<T> result = new ArrayList<>();
		finalUpdatesToMerge.forEach(t -> result.addAll(t.f1));
		return result;
	}

}

