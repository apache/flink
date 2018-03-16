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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Keys.SelectorFunctionKeys;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.operators.translation.PlanUnwrappingReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * This operator represents the application of a "reduce" function on a data set, and the
 * result data set produced by the function.
 *
 * @param <IN> The type of the data set reduced by the operator.
 *
 * @see org.apache.flink.api.common.functions.ReduceFunction
 */
@Public
public class ReduceOperator<IN> extends SingleInputUdfOperator<IN, IN, ReduceOperator<IN>> {

	private final ReduceFunction<IN> function;

	private final Grouping<IN> grouper;

	private final String defaultName;

	// should be null in case of an all reduce
	private CombineHint hint;

	/**
	 * This is the case for a reduce-all case (in contrast to the reduce-per-group case).
	 *
	 * @param input
	 * @param function
	 */
	public ReduceOperator(DataSet<IN> input, ReduceFunction<IN> function, String defaultName) {
		super(input, input.getType());

		this.function = function;
		this.grouper = null;
		this.defaultName = defaultName;
		this.hint = null;
	}

	public ReduceOperator(Grouping<IN> input, ReduceFunction<IN> function, String defaultName) {
		super(input.getInputDataSet(), input.getInputDataSet().getType());

		this.function = function;
		this.grouper = input;
		this.defaultName = defaultName;
		this.hint = CombineHint.OPTIMIZER_CHOOSES;

		UdfOperatorUtils.analyzeSingleInputUdf(this, ReduceFunction.class, defaultName, function, grouper.keys);
	}

	@Override
	protected ReduceFunction<IN> getFunction() {
		return function;
	}

	@Override
	@Internal
	public SingleInputSemanticProperties getSemanticProperties() {

		SingleInputSemanticProperties props = super.getSemanticProperties();

		// offset semantic information by extracted key fields
		if (props != null &&
				this.grouper != null &&
				this.grouper.keys instanceof SelectorFunctionKeys) {

			int offset = ((SelectorFunctionKeys<?, ?>) this.grouper.keys).getKeyType().getTotalFields();
			if (this.grouper instanceof SortedGrouping) {
				offset += ((SortedGrouping<?>) this.grouper).getSortSelectionFunctionKey().getKeyType().getTotalFields();
			}
			props = SemanticPropUtil.addSourceFieldOffset(props, this.getInputType().getTotalFields(), offset);
		}

		return props;
	}

	@Override
	protected org.apache.flink.api.common.operators.SingleInputOperator<?, IN, ?> translateToDataFlow(Operator<IN> input) {

		String name = getName() != null ? getName() : "Reduce at " + defaultName;

		// distinguish between grouped reduce and non-grouped reduce
		if (grouper == null) {
			// non grouped reduce
			UnaryOperatorInformation<IN, IN> operatorInfo = new UnaryOperatorInformation<>(getInputType(), getInputType());
			ReduceOperatorBase<IN, ReduceFunction<IN>> po =
					new ReduceOperatorBase<>(function, operatorInfo, new int[0], name);

			po.setInput(input);
			// the parallelism for a non grouped reduce can only be 1
			po.setParallelism(1);

			return po;
		}

		if (grouper.getKeys() instanceof SelectorFunctionKeys) {

			// reduce with key selector function
			@SuppressWarnings("unchecked")
			SelectorFunctionKeys<IN, ?> selectorKeys = (SelectorFunctionKeys<IN, ?>) grouper.getKeys();

			org.apache.flink.api.common.operators.SingleInputOperator<?, IN, ?> po =
				translateSelectorFunctionReducer(selectorKeys, function, getInputType(), name, input, getParallelism(), hint);
			((PlanUnwrappingReduceOperator<?, ?>) po.getInput()).setCustomPartitioner(grouper.getCustomPartitioner());

			return po;
		}
		else if (grouper.getKeys() instanceof Keys.ExpressionKeys) {

			// reduce with field positions
			int[] logicalKeyPositions = grouper.getKeys().computeLogicalKeyPositions();
			UnaryOperatorInformation<IN, IN> operatorInfo = new UnaryOperatorInformation<>(getInputType(), getInputType());
			ReduceOperatorBase<IN, ReduceFunction<IN>> po =
					new ReduceOperatorBase<>(function, operatorInfo, logicalKeyPositions, name);

			po.setCustomPartitioner(grouper.getCustomPartitioner());

			po.setInput(input);
			po.setParallelism(getParallelism());
			po.setCombineHint(hint);

			return po;
		}
		else {
			throw new UnsupportedOperationException("Unrecognized key type.");
		}
	}

	/**
	 * Sets the strategy to use for the combine phase of the reduce.
	 *
	 * <p>If this method is not called, then the default hint will be used.
	 * ({@link org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint#OPTIMIZER_CHOOSES})
	 *
	 * @param strategy The hint to use.
	 * @return The ReduceOperator object, for function call chaining.
	 */
	@PublicEvolving
	public ReduceOperator<IN> setCombineHint(CombineHint strategy) {
		this.hint = strategy;
		return this;
	}

	// --------------------------------------------------------------------------------------------

	private static <T, K> org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?> translateSelectorFunctionReducer(
		SelectorFunctionKeys<T, ?> rawKeys,
		ReduceFunction<T> function,
		TypeInformation<T> inputType,
		String name,
		Operator<T> input,
		int parallelism,
		CombineHint hint) {
		@SuppressWarnings("unchecked")
		final SelectorFunctionKeys<T, K> keys = (SelectorFunctionKeys<T, K>) rawKeys;

		TypeInformation<Tuple2<K, T>> typeInfoWithKey = KeyFunctions.createTypeWithKey(keys);
		Operator<Tuple2<K, T>> keyedInput = KeyFunctions.appendKeyExtractor(input, keys);

		PlanUnwrappingReduceOperator<T, K> reducer = new PlanUnwrappingReduceOperator<>(function, keys, name, inputType, typeInfoWithKey);
		reducer.setInput(keyedInput);
		reducer.setParallelism(parallelism);
		reducer.setCombineHint(hint);

		return KeyFunctions.appendKeyRemover(reducer, keys);
	}
}
