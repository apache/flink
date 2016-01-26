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

package org.apache.flink.api.java.operators;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.operators.Keys.SelectorFunctionKeys;
import org.apache.flink.api.java.operators.translation.PlanUnwrappingReduceGroupOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;

/**
 * This operator represents the application of a "distinct" function on a data set, and the
 * result data set produced by the function.
 *
 * @param <T> The type of the data set made distinct by the operator.
 */
public class DistinctOperator<T> extends SingleInputOperator<T, T, DistinctOperator<T>> {

	private final Keys<T> keys;

	private final String distinctLocationName;

	public DistinctOperator(DataSet<T> input, Keys<T> keys, String distinctLocationName) {
		super(input, input.getType());

		this.distinctLocationName = distinctLocationName;

		// if keys is null distinction is done on all fields
		if (keys == null) {
			keys = new Keys.ExpressionKeys<>(input.getType());
		}

		this.keys = keys;
	}

	@Override
	protected org.apache.flink.api.common.operators.base.GroupReduceOperatorBase<?, T, ?> translateToDataFlow(Operator<T> input) {

		final RichGroupReduceFunction<T, T> function = new DistinctFunction<>();

		String name = getName() != null ? getName() : "Distinct at " + distinctLocationName;

		if (keys instanceof Keys.ExpressionKeys) {

			int[] logicalKeyPositions = keys.computeLogicalKeyPositions();
			UnaryOperatorInformation<T, T> operatorInfo = new UnaryOperatorInformation<>(getInputType(), getResultType());
			GroupReduceOperatorBase<T, T, GroupReduceFunction<T, T>> po =
					new GroupReduceOperatorBase<T, T, GroupReduceFunction<T, T>>(function, operatorInfo, logicalKeyPositions, name);

			po.setCombinable(true);
			po.setInput(input);
			po.setParallelism(getParallelism());

			// make sure that distinct preserves the partitioning for the fields on which they operate
			if (getType().isTupleType()) {
				SingleInputSemanticProperties sProps = new SingleInputSemanticProperties();

				for (int field : keys.computeLogicalKeyPositions()) {
					sProps.addForwardedField(field, field);
				}

				po.setSemanticProperties(sProps);
			}

			return po;
		}
		else if (keys instanceof SelectorFunctionKeys) {

			@SuppressWarnings("unchecked")
			SelectorFunctionKeys<T, ?> selectorKeys = (SelectorFunctionKeys<T, ?>) keys;

			PlanUnwrappingReduceGroupOperator<T, T, ?> po = translateSelectorFunctionDistinct(
							selectorKeys, function, getResultType(), name, input);

			po.setParallelism(this.getParallelism());

			return po;
		}
		else {
			throw new UnsupportedOperationException("Unrecognized key type.");
		}
	}

	// --------------------------------------------------------------------------------------------

	private static <IN, OUT, K> PlanUnwrappingReduceGroupOperator<IN, OUT, K> translateSelectorFunctionDistinct(
			SelectorFunctionKeys<IN, ?> rawKeys,
			RichGroupReduceFunction<IN, OUT> function,
			TypeInformation<OUT> outputType,
			String name,
			Operator<IN> input)
	{
		@SuppressWarnings("unchecked")
		final SelectorFunctionKeys<IN, K> keys = (SelectorFunctionKeys<IN, K>) rawKeys;
		
		TypeInformation<Tuple2<K, IN>> typeInfoWithKey = SelectorFunctionKeys.createTypeWithKey(keys);
		Operator<Tuple2<K, IN>> keyedInput = SelectorFunctionKeys.appendKeyExtractor(input, keys);
		
		PlanUnwrappingReduceGroupOperator<IN, OUT, K> reducer =
				new PlanUnwrappingReduceGroupOperator<>(function, keys, name, outputType, typeInfoWithKey, true);
		reducer.setInput(keyedInput);

		return reducer;
	}
	
	@RichGroupReduceFunction.Combinable
	public static final class DistinctFunction<T> extends RichGroupReduceFunction<T, T> {

		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<T> values, Collector<T> out) {
			out.collect(values.iterator().next());
		}
	}
}
