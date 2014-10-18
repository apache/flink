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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class PlanLeftUnwrappingJoinOperator<I1, I2, OUT, K>
		extends JoinOperatorBase<Tuple2<K, I1>, I2, OUT, FlatJoinFunction<Tuple2<K, I1>, I2, OUT>> {

	public PlanLeftUnwrappingJoinOperator(
			FlatJoinFunction<I1, I2, OUT> udf,
			Keys.SelectorFunctionKeys<I1, K> key1,
			int[] key2, String name,
			TypeInformation<OUT> resultType,
			TypeInformation<Tuple2<K, I1>> typeInfoWithKey1,
			TypeInformation<I2> typeInfo2) {
		super(
				new TupleLeftUnwrappingJoiner<I1, I2, OUT, K>(udf),
				new BinaryOperatorInformation<Tuple2<K, I1>, I2, OUT>(
						typeInfoWithKey1,
						typeInfo2,
						resultType),
				key1.computeLogicalKeyPositions(), key2, name);
	}

	public static final class TupleLeftUnwrappingJoiner<I1, I2, OUT, K>
			extends WrappingFunction<FlatJoinFunction<I1, I2, OUT>>
			implements FlatJoinFunction<Tuple2<K, I1>, I2, OUT> {

		private static final long serialVersionUID = 1L;

		private TupleLeftUnwrappingJoiner(FlatJoinFunction<I1, I2, OUT> wrapped) {
			super(wrapped);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void join (Tuple2<K, I1> value1, I2 value2, Collector<OUT> collector) throws Exception {
			wrappedFunction.join ((I1)(value1.getField(1)), value2, collector);
		}
	}
}
