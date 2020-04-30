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

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * A reduce operator that takes 3-tuples (groupKey, sortKey, value), and applies the sorted group reduce
 * operation only on the unwrapped values.
 */
@Internal
public class PlanUnwrappingSortedReduceGroupOperator<IN, OUT, K1, K2> extends GroupReduceOperatorBase<Tuple3<K1, K2, IN>, OUT, GroupReduceFunction<Tuple3<K1, K2, IN>, OUT>> {

	public PlanUnwrappingSortedReduceGroupOperator(
		GroupReduceFunction<IN, OUT> udf,
		Keys.SelectorFunctionKeys<IN, K1> groupingKey,
		Keys.SelectorFunctionKeys<IN, K2> sortingKey,
		String name,
		TypeInformation<OUT> outType,
		TypeInformation<Tuple3<K1, K2, IN>>
		typeInfoWithKey, boolean combinable) {
		super(
			combinable ?
				new TupleUnwrappingGroupCombinableGroupReducer<IN, OUT, K1, K2>(udf) :
				new TupleUnwrappingNonCombinableGroupReducer<IN, OUT, K1, K2>(udf),
			new UnaryOperatorInformation<>(typeInfoWithKey, outType), groupingKey.computeLogicalKeyPositions(), name);

		super.setCombinable(combinable);
	}

	// --------------------------------------------------------------------------------------------

	private static final class TupleUnwrappingGroupCombinableGroupReducer<IN, OUT, K1, K2> extends WrappingFunction<GroupReduceFunction<IN, OUT>>
		implements GroupReduceFunction<Tuple3<K1, K2, IN>, OUT>, GroupCombineFunction<Tuple3<K1, K2, IN>, Tuple3<K1, K2, IN>> {

		private static final long serialVersionUID = 1L;

		private Tuple3UnwrappingIterator<IN, K1, K2> iter;
		private Tuple3WrappingCollector<IN, K1, K2> coll;

		private TupleUnwrappingGroupCombinableGroupReducer(GroupReduceFunction<IN, OUT> wrapped) {
			super(wrapped);

			if (!GroupCombineFunction.class.isAssignableFrom(wrappedFunction.getClass())) {
				throw new IllegalArgumentException("Wrapped reduce function does not implement the GroupCombineFunction interface.");
			}

			this.iter = new Tuple3UnwrappingIterator<>();
			this.coll = new Tuple3WrappingCollector<>(this.iter);
		}

		@Override
		public void reduce(Iterable<Tuple3<K1, K2, IN>> values, Collector<OUT> out) throws Exception {
			iter.set(values.iterator());
			this.wrappedFunction.reduce(iter, out);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void combine(Iterable<Tuple3<K1, K2, IN>> values, Collector<Tuple3<K1, K2, IN>> out) throws Exception {
			iter.set(values.iterator());
			coll.set(out);
			((GroupCombineFunction<IN, IN>) this.wrappedFunction).combine(iter, coll);
		}

		@Override
		public String toString() {
			return this.wrappedFunction.toString();
		}
	}

	private static final class TupleUnwrappingNonCombinableGroupReducer<IN, OUT, K1, K2> extends WrappingFunction<GroupReduceFunction<IN, OUT>>
		implements GroupReduceFunction<Tuple3<K1, K2, IN>, OUT> {

		private static final long serialVersionUID = 1L;

		private final Tuple3UnwrappingIterator<IN, K1, K2> iter;

		private TupleUnwrappingNonCombinableGroupReducer(GroupReduceFunction<IN, OUT> wrapped) {
			super(wrapped);
			this.iter = new Tuple3UnwrappingIterator<>();
		}

		@Override
		public void reduce(Iterable<Tuple3<K1, K2, IN>> values, Collector<OUT> out) throws Exception {
			iter.set(values.iterator());
			this.wrappedFunction.reduce(iter, out);
		}

		@Override
		public String toString() {
			return this.wrappedFunction.toString();
		}
	}
}
