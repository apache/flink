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
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.GroupCombineOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * A reduce operator that takes 3-tuples (groupKey, sortKey, value), and applies the sorted partial group reduce
 * operation only on the unwrapped values.
 */
@Internal
public class PlanUnwrappingSortedGroupCombineOperator<IN, OUT, K1, K2> extends GroupCombineOperatorBase<Tuple3<K1, K2, IN>, OUT, GroupCombineFunction<Tuple3<K1, K2, IN>, OUT>> {

	public PlanUnwrappingSortedGroupCombineOperator(GroupCombineFunction<IN, OUT> udf, Keys.SelectorFunctionKeys<IN, K1> groupingKey, Keys.SelectorFunctionKeys<IN, K2> sortingKey, String name,
													TypeInformation<OUT> outType, TypeInformation<Tuple3<K1, K2, IN>> typeInfoWithKey) {
		super(new TupleUnwrappingGroupReducer<IN, OUT, K1, K2>(udf),
				new UnaryOperatorInformation<Tuple3<K1, K2, IN>, OUT>(typeInfoWithKey, outType),
				groupingKey.computeLogicalKeyPositions(),
				name);

	}

	private static final class TupleUnwrappingGroupReducer<IN, OUT, K1, K2> extends WrappingFunction<GroupCombineFunction<IN, OUT>>
			implements GroupCombineFunction<Tuple3<K1, K2, IN>, OUT> {

		private static final long serialVersionUID = 1L;

		private final Tuple3UnwrappingIterator<IN, K1, K2> iter;

		private TupleUnwrappingGroupReducer(GroupCombineFunction<IN, OUT> wrapped) {
			super(wrapped);
			this.iter = new Tuple3UnwrappingIterator<IN, K1, K2>();
		}

		@Override
		public void combine(Iterable<Tuple3<K1, K2, IN>> values, Collector<OUT> out) throws Exception {
			iter.set(values.iterator());
			this.wrappedFunction.combine(iter, out);
		}

		@Override
		public String toString() {
			return this.wrappedFunction.toString();
		}
	}
}
