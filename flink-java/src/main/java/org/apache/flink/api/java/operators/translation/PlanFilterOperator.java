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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.FilterOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.util.Collector;

/**
 * @see FilterOperatorBase
 * @param <T>
 */
@Internal
@ForwardedFields("*")
public class PlanFilterOperator<T> extends FilterOperatorBase<T, FlatMapFunction<T, T>> {

	public PlanFilterOperator(FilterFunction<T> udf, String name, TypeInformation<T> type) {
		super(new FlatMapFilter<T>(udf), new UnaryOperatorInformation<T, T>(type, type), name);
	}

	/**
	 * @see FlatMapFunction
	 * @param <T>
	 */
	public static final class FlatMapFilter<T> extends WrappingFunction<FilterFunction<T>>
		implements FlatMapFunction<T, T> {

		private static final long serialVersionUID = 1L;

		private FlatMapFilter(FilterFunction<T> wrapped) {
			super(wrapped);
		}

		@Override
		public final void flatMap(T value, Collector<T> out) throws Exception {
			if (this.wrappedFunction.filter(value)) {
				out.collect(value);
			}
		}
	}
}
