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
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.base.CoGroupOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * A co group operator that applies the operation only on the unwrapped values on the right.
 */
@Internal
public class PlanRightUnwrappingCoGroupOperator<I1, I2, OUT, K>
		extends CoGroupOperatorBase<I1, Tuple2<K, I2>, OUT, CoGroupFunction<I1, Tuple2<K, I2>, OUT>> {

	public PlanRightUnwrappingCoGroupOperator(
			CoGroupFunction<I1, I2, OUT> udf,
			int[] key1,
			Keys.SelectorFunctionKeys<I2, K> key2,
			String name,
			TypeInformation<OUT> resultType,
			TypeInformation<I1> typeInfo1,
			TypeInformation<Tuple2<K, I2>> typeInfoWithKey2) {

		super(
				new TupleRightUnwrappingCoGrouper<I1, I2, OUT, K>(udf),
				new BinaryOperatorInformation<I1, Tuple2<K, I2>, OUT>(
						typeInfo1,
						typeInfoWithKey2,
						resultType),
				key1,
				key2.computeLogicalKeyPositions(),
				name);
	}

	private static final class TupleRightUnwrappingCoGrouper<I1, I2, OUT, K>
			extends WrappingFunction<CoGroupFunction<I1, I2, OUT>>
			implements CoGroupFunction<I1, Tuple2<K, I2>, OUT> {

		private static final long serialVersionUID = 1L;

		private final TupleUnwrappingIterator<I2, K> iter2;

		private TupleRightUnwrappingCoGrouper(CoGroupFunction<I1, I2, OUT> wrapped) {
			super(wrapped);

			this.iter2 = new TupleUnwrappingIterator<I2, K>();
		}

		@Override
		public void coGroup(
				Iterable<I1> records1,
				Iterable<Tuple2<K, I2>> records2,
				Collector<OUT> out) throws Exception {

			iter2.set(records2.iterator());
			this.wrappedFunction.coGroup(records1, iter2, out);
		}
	}
}
