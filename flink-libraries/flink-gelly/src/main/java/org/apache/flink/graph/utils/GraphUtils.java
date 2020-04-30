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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.types.LongValue;

import static org.apache.flink.api.java.typeutils.ValueTypeInfo.LONG_VALUE_TYPE_INFO;

/**
 * {@link Graph} utilities.
 */
public class GraphUtils {

	private GraphUtils() {}

	/**
	 * Count the number of elements in a DataSet.
	 *
	 * @param input DataSet of elements to be counted
	 * @param <T> element type
	 * @return count
	 */
	public static <T> DataSet<LongValue> count(DataSet<T> input) {
		return input
			.map(new MapTo<>(new LongValue(1)))
				.returns(LONG_VALUE_TYPE_INFO)
				.name("Emit 1")
			.reduce(new AddLongValue())
				.name("Sum");
	}

	/**
	 * The identity mapper returns the input as output.
	 *
	 * @param <T> element type
	 */
	@ForwardedFields("*")
	public static final class IdentityMapper<T>
	implements MapFunction<T, T> {
		public T map(T value) {
			return value;
		}
	}

	/**
	 * The identity mapper returns the input as output.
	 *
	 * <p>This does not forward fields and is used to break an operator chain.
	 *
	 * @param <T> element type
	 */
	public static final class NonForwardingIdentityMapper<T>
	implements MapFunction<T, T> {
		public T map(T value) {
			return value;
		}
	}

	/**
	 * Map each element to a value.
	 *
	 * @param <I> input type
	 * @param <O> output type
	 */
	public static class MapTo<I, O>
	implements MapFunction<I, O>, ResultTypeQueryable<O>, TranslateFunction<I, O> {
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
		public O map(I input) throws Exception {
			return value;
		}

		@Override
		public O translate(I input, O reuse)
				throws Exception {
			return value;
		}

		@Override
		public TypeInformation<O> getProducedType() {
			return (TypeInformation<O>) TypeExtractor.createTypeInfo(value.getClass());
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
