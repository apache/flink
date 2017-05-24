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

package org.apache.flink.graph.library.linkanalysis;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.DoubleValue;

class Functions {

	private Functions() {}

	/**
	 * Sum vertices' scores.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0")
	protected static final class SumScore<T>
		implements ReduceFunction<Tuple2<T, DoubleValue>> {
		@Override
		public Tuple2<T, DoubleValue> reduce(Tuple2<T, DoubleValue> left, Tuple2<T, DoubleValue> right)
			throws Exception {
			left.f1.setValue(left.f1.getValue() + right.f1.getValue());
			return left;
		}
	}
}
