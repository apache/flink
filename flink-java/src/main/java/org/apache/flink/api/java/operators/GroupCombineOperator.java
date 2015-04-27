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

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.GroupCombineOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.operators.translation.KeyExtractingMapper;
import org.apache.flink.api.java.operators.translation.PlanUnwrappingGroupCombineOperator;
import org.apache.flink.api.java.operators.translation.PlanUnwrappingSortedGroupCombineOperator;
import org.apache.flink.api.java.operators.translation.TwoKeyExtractingMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

/**
 * This operator behaves like the GroupReduceOperator with Combine but only runs the Combine part which reduces all data
 * locally in their partitions. The combine part can return an arbitrary data type. This is useful to pre-combine values 
 * into an intermediate representation before applying a proper reduce operation.
 *
 * @param <IN> The type of the data set consumed by the operator.
 * @param <OUT> The type of the data set created by the operator.
 */
public class GroupCombineOperator<IN, OUT> extends SingleInputUdfOperator<IN, OUT, GroupCombineOperator<IN, OUT>> {

	private final GroupCombineFunction<IN, OUT> function;

	private final Grouping<IN> grouper;

	private final String defaultName;

	/**
	 * Constructor for a non-grouped reduce (all reduce).
	 *
	 * @param input The input data set to the groupReduce function.
	 * @param resultType The type information for the resulting type.
	 * @param function The user-defined GroupReduce function.
	 * @param defaultName The operator's name.
	 */
	public GroupCombineOperator(DataSet<IN> input, TypeInformation<OUT> resultType, GroupCombineFunction<IN, OUT> function, String defaultName) {
		super(input, resultType);
		this.function = function;
		this.grouper = null;
		this.defaultName = defaultName;
	}

	/**
	 * Constructor for a grouped reduce.
	 *
	 * @param input The grouped input to be processed group-wise by the groupReduce function.
	 * @param function The user-defined GroupReduce function.
	 */
	public GroupCombineOperator(Grouping<IN> input, TypeInformation<OUT> resultType, GroupCombineFunction<IN, OUT> function, String defaultName) {
		super(input != null ? input.getDataSet() : null, resultType);

		this.function = function;
		this.grouper = input;
		this.defaultName = defaultName;
	}

	@Override
	protected GroupCombineFunction<IN, OUT> getFunction() {
		return function;
	}

	@Override
	public SingleInputSemanticProperties getSemanticProperties() {

		SingleInputSemanticProperties props = super.getSemanticProperties();

		// offset semantic information by extracted key fields
		if(props != null &&
				this.grouper != null &&
				this.grouper.keys instanceof Keys.SelectorFunctionKeys) {

			int offset = ((Keys.SelectorFunctionKeys) this.grouper.keys).getKeyType().getTotalFields();
			if(this.grouper instanceof SortedGrouping) {
				offset += ((SortedGrouping) this.grouper).getSortSelectionFunctionKey().getKeyType().getTotalFields();
			}

			props = SemanticPropUtil.addSourceFieldOffset(props, this.getInputType().getTotalFields(), offset);
		}

		return props;
	}

	// --------------------------------------------------------------------------------------------
	//  Translation
	// --------------------------------------------------------------------------------------------

	@Override
	protected GroupCombineOperatorBase<?, OUT, ?> translateToDataFlow(Operator<IN> input) {

		String name = getName() != null ? getName() : "GroupCombine at " + defaultName;

		// distinguish between grouped reduce and non-grouped reduce
		if (grouper == null) {
			// non grouped reduce
			UnaryOperatorInformation<IN, OUT> operatorInfo = new UnaryOperatorInformation<IN, OUT>(getInputType(), getResultType());
			GroupCombineOperatorBase<IN, OUT, GroupCombineFunction<IN, OUT>> po =
					new GroupCombineOperatorBase<IN, OUT, GroupCombineFunction<IN, OUT>>(function, operatorInfo, new int[0], name);

			po.setInput(input);
			// the parallelism for a non grouped reduce can only be 1
			po.setParallelism(1);
			return po;
		}

		if (grouper.getKeys() instanceof Keys.SelectorFunctionKeys) {

			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<IN, ?> selectorKeys = (Keys.SelectorFunctionKeys<IN, ?>) grouper.getKeys();

			if (grouper instanceof SortedGrouping) {
				SortedGrouping<IN> sortedGrouper = (SortedGrouping<IN>) grouper;
				Keys.SelectorFunctionKeys<IN, ?> sortKeys = sortedGrouper.getSortSelectionFunctionKey();

				PlanUnwrappingSortedGroupCombineOperator<IN, OUT, ?, ?> po = translateSelectorFunctionSortedReducer(
						selectorKeys, sortKeys, function, getInputType(), getResultType(), name, input);

				// set group order
				int[] sortKeyPositions = sortedGrouper.getGroupSortKeyPositions();
				Order[] sortOrders = sortedGrouper.getGroupSortOrders();

				Ordering o = new Ordering();
				for(int i=0; i < sortKeyPositions.length; i++) {
					o.appendOrdering(sortKeyPositions[i], null, sortOrders[i]);
				}
				po.setGroupOrder(o);

				po.setParallelism(this.getParallelism());
				return po;
			} else {
				PlanUnwrappingGroupCombineOperator<IN, OUT, ?> po = translateSelectorFunctionReducer(
						selectorKeys, function, getInputType(), getResultType(), name, input);

				po.setParallelism(this.getParallelism());
				return po;
			}
		}
		else if (grouper.getKeys() instanceof Keys.ExpressionKeys) {

			int[] logicalKeyPositions = grouper.getKeys().computeLogicalKeyPositions();
			UnaryOperatorInformation<IN, OUT> operatorInfo = new UnaryOperatorInformation<IN, OUT>(getInputType(), getResultType());
			GroupCombineOperatorBase<IN, OUT, GroupCombineFunction<IN, OUT>> po =
					new GroupCombineOperatorBase<IN, OUT, GroupCombineFunction<IN, OUT>>(function, operatorInfo, logicalKeyPositions, name);

			po.setInput(input);
			po.setParallelism(getParallelism());

			// set group order
			if (grouper instanceof SortedGrouping) {
				SortedGrouping<IN> sortedGrouper = (SortedGrouping<IN>) grouper;

				int[] sortKeyPositions = sortedGrouper.getGroupSortKeyPositions();
				Order[] sortOrders = sortedGrouper.getGroupSortOrders();

				Ordering o = new Ordering();
				for(int i=0; i < sortKeyPositions.length; i++) {
					o.appendOrdering(sortKeyPositions[i], null, sortOrders[i]);
				}
				po.setGroupOrder(o);
			}

			return po;
		}
		else {
			throw new UnsupportedOperationException("Unrecognized key type.");
		}
	}


	// --------------------------------------------------------------------------------------------

	private static <IN, OUT, K> PlanUnwrappingGroupCombineOperator<IN, OUT, K> translateSelectorFunctionReducer(
			Keys.SelectorFunctionKeys<IN, ?> rawKeys, GroupCombineFunction<IN, OUT> function,
			TypeInformation<IN> inputType, TypeInformation<OUT> outputType, String name, Operator<IN> input)
	{
		@SuppressWarnings("unchecked")
		final Keys.SelectorFunctionKeys<IN, K> keys = (Keys.SelectorFunctionKeys<IN, K>) rawKeys;

		TypeInformation<Tuple2<K, IN>> typeInfoWithKey = new TupleTypeInfo<Tuple2<K, IN>>(keys.getKeyType(), inputType);

		KeyExtractingMapper<IN, K> extractor = new KeyExtractingMapper<IN, K>(keys.getKeyExtractor());

		PlanUnwrappingGroupCombineOperator<IN, OUT, K> reducer = new PlanUnwrappingGroupCombineOperator<IN, OUT, K>(function, keys, name, outputType, typeInfoWithKey);

		MapOperatorBase<IN, Tuple2<K, IN>, MapFunction<IN, Tuple2<K, IN>>> mapper = new MapOperatorBase<IN, Tuple2<K, IN>, MapFunction<IN, Tuple2<K, IN>>>(extractor, new UnaryOperatorInformation<IN, Tuple2<K, IN>>(inputType, typeInfoWithKey), "Key Extractor");

		reducer.setInput(mapper);
		mapper.setInput(input);

		// set the mapper's parallelism to the input parallelism to make sure it is chained
		mapper.setParallelism(input.getParallelism());

		return reducer;
	}

	private static <IN, OUT, K1, K2> PlanUnwrappingSortedGroupCombineOperator<IN, OUT, K1, K2> translateSelectorFunctionSortedReducer(
			Keys.SelectorFunctionKeys<IN, ?> rawGroupingKey, Keys.SelectorFunctionKeys<IN, ?> rawSortingKey, GroupCombineFunction<IN, OUT> function,
			TypeInformation<IN> inputType, TypeInformation<OUT> outputType, String name, Operator<IN> input)
	{
		@SuppressWarnings("unchecked")
		final Keys.SelectorFunctionKeys<IN, K1> groupingKey = (Keys.SelectorFunctionKeys<IN, K1>) rawGroupingKey;

		@SuppressWarnings("unchecked")
		final Keys.SelectorFunctionKeys<IN, K2> sortingKey = (Keys.SelectorFunctionKeys<IN, K2>) rawSortingKey;

		TypeInformation<Tuple3<K1, K2, IN>> typeInfoWithKey = new TupleTypeInfo<Tuple3<K1, K2, IN>>(groupingKey.getKeyType(), sortingKey.getKeyType(), inputType);

		TwoKeyExtractingMapper<IN, K1, K2> extractor = new TwoKeyExtractingMapper<IN, K1, K2>(groupingKey.getKeyExtractor(), sortingKey.getKeyExtractor());

		PlanUnwrappingSortedGroupCombineOperator<IN, OUT, K1, K2> reducer = new PlanUnwrappingSortedGroupCombineOperator<IN, OUT, K1, K2>(function, groupingKey, sortingKey, name, outputType, typeInfoWithKey);

		MapOperatorBase<IN, Tuple3<K1, K2, IN>, MapFunction<IN, Tuple3<K1, K2, IN>>> mapper = new MapOperatorBase<IN, Tuple3<K1, K2, IN>, MapFunction<IN, Tuple3<K1, K2, IN>>>(extractor, new UnaryOperatorInformation<IN, Tuple3<K1, K2, IN>>(inputType, typeInfoWithKey), "Key Extractor");

		reducer.setInput(mapper);
		mapper.setInput(input);

		// set the mapper's parallelism to the input parallelism to make sure it is chained
		mapper.setParallelism(input.getParallelism());

		return reducer;
	}
}
