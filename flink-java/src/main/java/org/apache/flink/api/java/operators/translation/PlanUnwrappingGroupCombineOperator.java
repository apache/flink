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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * A group combine operator that takes 2-tuples (key-value pairs), and applies the group combine operation only
 * on the unwrapped values.
 */
@Internal
public class PlanUnwrappingGroupCombineOperator<IN, OUT, K> extends GroupCombineOperatorBase<Tuple2<K, IN>, OUT, GroupCombineFunction<Tuple2<K, IN>, OUT>> {

	public PlanUnwrappingGroupCombineOperator(GroupCombineFunction<IN, OUT> udf, Keys.SelectorFunctionKeys<IN, K> key, String name,
												TypeInformation<OUT> outType, TypeInformation<Tuple2<K, IN>> typeInfoWithKey) {
		super(new TupleUnwrappingGroupCombiner<IN, OUT, K>(udf),
				new UnaryOperatorInformation<Tuple2<K, IN>, OUT>(typeInfoWithKey, outType), key.computeLogicalKeyPositions(), name);

	}

	// --------------------------------------------------------------------------------------------

	private static final class TupleUnwrappingGroupCombiner<IN, OUT, K> extends WrappingFunction<GroupCombineFunction<IN, OUT>>
		implements GroupCombineFunction<Tuple2<K, IN>, OUT> {

		private static final long serialVersionUID = 1L;

		private final TupleUnwrappingIterator<IN, K> iter;

		private TupleUnwrappingGroupCombiner(GroupCombineFunction<IN, OUT> wrapped) {
			super(wrapped);
			this.iter = new TupleUnwrappingIterator<IN, K>();
		}

		@Override
		public void combine(Iterable<Tuple2<K, IN>> values, Collector<OUT> out) throws Exception {
			iter.set(values.iterator());
			this.wrappedFunction.combine(iter, out);
		}

		@Override
		public String toString() {
			return this.wrappedFunction.toString();
		}
	}
}
