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

package org.apache.flink.graph.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.LongValue;

import static org.apache.flink.api.java.typeutils.ValueTypeInfo.LONG_VALUE_TYPE_INFO;

public class GraphUtils {

	/**
	 * Count the number of elements in a DataSet.
	 *
	 * @param input DataSet of elements to be counted
	 * @param <T> element type
	 * @return count
	 */
	public static <T> DataSet<LongValue> count(DataSet<T> input) {
		return input
			.map(new MapTo<T, LongValue>(new LongValue(1)))
				.returns(LONG_VALUE_TYPE_INFO)
			.reduce(new AddLongValue());
	}

	/**
	 * Map each element to a value.
	 *
	 * @param <I> input type
	 * @param <O> output type
	 */
	public static class MapTo<I, O>
	implements MapFunction<I, O> {
		private final O value;

		/**
		 * Map each element to the given object.
		 *
		 * @param value the object to emit for each element
		 */
		public MapTo(O value) {
			this.value = value;
		}

		@Override
		public O map(I o) throws Exception {
			return value;
		}
	}

	/**
	 * Add {@link LongValue} elements.
	 */
	public static class AddLongValue
	implements ReduceFunction<LongValue> {
		@Override
		public LongValue reduce(LongValue value1, LongValue value2)
				throws Exception {
			value1.setValue(value1.getValue() + value2.getValue());
			return value1;
		}
	}
}
