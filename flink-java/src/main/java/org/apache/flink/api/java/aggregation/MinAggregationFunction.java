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

package org.apache.flink.api.java.aggregation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.ResettableValue;

/**
 * Implementations of {@link AggregationFunction} for min operation.
 * @param <T> aggregating type
 */
@Internal
public abstract class MinAggregationFunction<T extends Comparable<T>> extends AggregationFunction<T> {
	private static final long serialVersionUID = 1L;

	@Override
	public String toString() {
		return "MIN";
	}

	// --------------------------------------------------------------------------------------------

	private static final class ImmutableMinAgg<U extends Comparable<U>> extends MinAggregationFunction<U> {
		private static final long serialVersionUID = 1L;

		private U value;

		@Override
		public void initializeAggregate() {
			value = null;
		}

		@Override
		public void aggregate(U val) {
			if (value != null) {
				int cmp = value.compareTo(val);
				value = (cmp < 0) ? value : val;
			} else {
				value = val;
			}
		}

		@Override
		public U getAggregate() {
			return value;
		}
	}

	// --------------------------------------------------------------------------------------------

	private static final class MutableMinAgg<U extends Comparable<U> & ResettableValue<U> & CopyableValue<U>> extends MinAggregationFunction<U> {
		private static final long serialVersionUID = 1L;

		private U value;

		@Override
		public void initializeAggregate() {
			value = null;
		}

		@Override
		public void aggregate(U val) {
			if (value != null) {
				int cmp = value.compareTo(val);
				if (cmp > 0) {
					value.setValue(val);
				}
			} else {
				value = val.copy();
			}
		}

		@Override
		public U getAggregate() {
			return value;
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Factory for {@link MinAggregationFunction}.
	 */
	public static final class MinAggregationFunctionFactory implements AggregationFunctionFactory {
		private static final long serialVersionUID = 1L;

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public <T> AggregationFunction<T> createAggregationFunction(Class<T> type) {
			if (Comparable.class.isAssignableFrom(type)) {
				if (ResettableValue.class.isAssignableFrom(type) && CopyableValue.class.isAssignableFrom(type)) {
					return (AggregationFunction<T>) new MutableMinAgg();
				} else {
					return (AggregationFunction<T>) new ImmutableMinAgg();
				}
			} else {
				throw new UnsupportedAggregationTypeException("The type " + type.getName() +
					" is not supported for minimum aggregation. " +
					"Minimum aggregatable types must implement the Comparable interface.");
			}
		}
	}
}
