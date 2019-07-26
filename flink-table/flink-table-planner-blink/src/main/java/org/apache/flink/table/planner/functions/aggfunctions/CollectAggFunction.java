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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregate function for COLLECT.
 * @param <T> type of collect element.
 */
public class CollectAggFunction<T>
	extends AggregateFunction<Map<T, Integer>, CollectAggFunction.CollectAccumulator<T>> {

	private static final long serialVersionUID = -5860934997657147836L;

	private final TypeInformation<T> elementType;

	public CollectAggFunction(TypeInformation<T> elementType) {
		this.elementType = elementType;
	}

	/** The initial accumulator for Collect aggregate function. */
	public static class CollectAccumulator<T> {
		public MapView<T, Integer> map = null;

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			CollectAccumulator<?> that = (CollectAccumulator<?>) o;
			return Objects.equals(map, that.map);
		}
	}

	public CollectAccumulator<T> createAccumulator() {
		CollectAccumulator<T> acc = new CollectAccumulator<>();
		acc.map = new MapView<>(elementType, Types.INT);
		return acc;
	}

	public void resetAccumulator(CollectAccumulator<T> accumulator) {
		accumulator.map.clear();
	}

	public void accumulate(CollectAccumulator<T> accumulator, T value) throws Exception {
		if (value != null) {
			Integer count = accumulator.map.get(value);
			if (count != null) {
				accumulator.map.put(value, count + 1);
			} else {
				accumulator.map.put(value, 1);
			}
		}
	}

	public void retract(CollectAccumulator<T> accumulator, T value) throws Exception {
		if (value != null) {
			Integer count = accumulator.map.get(value);
			if (count != null) {
				if (count == 1) {
					accumulator.map.remove(value);
				} else {
					accumulator.map.put(value, count - 1);
				}
			} else {
				accumulator.map.put(value, -1);
			}
		}
	}

	public void merge(CollectAccumulator<T> accumulator, Iterable<CollectAccumulator<T>> others) throws Exception {
		for (CollectAccumulator<T> other : others) {
			for (Map.Entry<T, Integer> entry : other.map.entries()) {
				T key = entry.getKey();
				Integer newCount = entry.getValue();
				Integer oldCount = accumulator.map.get(key);
				if (oldCount == null) {
					accumulator.map.put(key, newCount);
				} else {
					accumulator.map.put(key, oldCount + newCount);
				}
			}
		}
	}

	@Override
	public Map<T, Integer> getValue(CollectAccumulator<T> accumulator) {
		Map<T, Integer> result = new HashMap<>();
		try {
			for (Map.Entry<T, Integer> entry : accumulator.map.entries()) {
				result.put(entry.getKey(), entry.getValue());
			}
			return result;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TypeInformation<Map<T, Integer>> getResultType() {
		return new MapTypeInfo<>(elementType, Types.INT);
	}
}
