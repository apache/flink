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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.SemanticPropUtil;
import org.apache.flink.api.java.operators.translation.KeyExtractingMapper;
import org.apache.flink.api.java.operators.translation.KeyRemovingMapper;
import org.apache.flink.api.java.operators.translation.PlanUnwrappingReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.DataSet;

/**
 * This operator represents the application of a "reduce" function on a data set, and the
 * result data set produced by the function.
 * 
 * @param <IN> The type of the data set reduced by the operator.
 * 
 * @see org.apache.flink.api.common.functions.ReduceFunction
 */
public class ReduceOperator<IN> extends SingleInputUdfOperator<IN, IN, ReduceOperator<IN>> {
	
	private final ReduceFunction<IN> function;
	
	private final Grouping<IN> grouper;
	
	private final String defaultName;
	
	/**
	 * 
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
	}
	
	
	public ReduceOperator(Grouping<IN> input, ReduceFunction<IN> function, String defaultName) {
		super(input.getDataSet(), input.getDataSet().getType());
		
		this.function = function;
		this.grouper = input;
		this.defaultName = defaultName;

		UdfOperatorUtils.analyzeSingleInputUdf(this, ReduceFunction.class, defaultName, function, grouper.keys);
	}
	
	@Override
	protected ReduceFunction<IN> getFunction() {
		return function;
	}

	@Override
	public SingleInputSemanticProperties getSemanticProperties() {

		SingleInputSemanticProperties props = super.getSemanticProperties();

		// offset semantic information by extracted key fields
		if(props != null &&
				this.grouper != null &&
				this.grouper.keys instanceof Keys.SelectorFunctionKeys) {

			int offset = ((Keys.SelectorFunctionKeys<?,?>) this.grouper.keys).getKeyType().getTotalFields();
			if(this.grouper instanceof SortedGrouping) {
				offset += ((SortedGrouping<?>) this.grouper).getSortSelectionFunctionKey().getKeyType().getTotalFields();
			}
			props = SemanticPropUtil.addSourceFieldOffset(props, this.getInputType().getTotalFields(), offset);
		}

		return props;
	}

	@Override
	protected org.apache.flink.api.common.operators.SingleInputOperator<?, IN, ?> translateToDataFlow(Operator<IN> input) {
		
		String name = getName() != null ? getName() : "Reduce at "+defaultName;
		
		// distinguish between grouped reduce and non-grouped reduce
		if (grouper == null) {
			// non grouped reduce
			UnaryOperatorInformation<IN, IN> operatorInfo = new UnaryOperatorInformation<IN, IN>(getInputType(), getInputType());
			ReduceOperatorBase<IN, ReduceFunction<IN>> po =
					new ReduceOperatorBase<IN, ReduceFunction<IN>>(function, operatorInfo, new int[0], name);
			
			po.setInput(input);
			// the parallelism for a non grouped reduce can only be 1
			po.setParallelism(1);
			
			return po;
		}
		
		if (grouper.getKeys() instanceof Keys.SelectorFunctionKeys) {
			
			// reduce with key selector function
			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<IN, ?> selectorKeys = (Keys.SelectorFunctionKeys<IN, ?>) grouper.getKeys();
			
			MapOperatorBase<?, IN, ?> po = translateSelectorFunctionReducer(selectorKeys, function, getInputType(), name, input, getParallelism());
			((PlanUnwrappingReduceOperator<?, ?>) po.getInput()).setCustomPartitioner(grouper.getCustomPartitioner());
			
			return po;
		}
		else if (grouper.getKeys() instanceof Keys.ExpressionKeys) {
			
			// reduce with field positions
			int[] logicalKeyPositions = grouper.getKeys().computeLogicalKeyPositions();
			UnaryOperatorInformation<IN, IN> operatorInfo = new UnaryOperatorInformation<IN, IN>(getInputType(), getInputType());
			ReduceOperatorBase<IN, ReduceFunction<IN>> po =
					new ReduceOperatorBase<IN, ReduceFunction<IN>>(function, operatorInfo, logicalKeyPositions, name);
			
			po.setCustomPartitioner(grouper.getCustomPartitioner());
			
			po.setInput(input);
			po.setParallelism(getParallelism());
			
			return po;
		}
		else {
			throw new UnsupportedOperationException("Unrecognized key type.");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static <T, K> MapOperatorBase<Tuple2<K, T>, T, ?> translateSelectorFunctionReducer(Keys.SelectorFunctionKeys<T, ?> rawKeys,
			ReduceFunction<T> function, TypeInformation<T> inputType, String name, Operator<T> input, int parallelism)
	{
		@SuppressWarnings("unchecked")
		final Keys.SelectorFunctionKeys<T, K> keys = (Keys.SelectorFunctionKeys<T, K>) rawKeys;
		
		TypeInformation<Tuple2<K, T>> typeInfoWithKey = new TupleTypeInfo<Tuple2<K, T>>(keys.getKeyType(), inputType);
		
		KeyExtractingMapper<T, K> extractor = new KeyExtractingMapper<T, K>(keys.getKeyExtractor());
		
		PlanUnwrappingReduceOperator<T, K> reducer = new PlanUnwrappingReduceOperator<T, K>(function, keys, name, inputType, typeInfoWithKey);
		
		MapOperatorBase<T, Tuple2<K, T>, MapFunction<T, Tuple2<K, T>>> keyExtractingMap = new MapOperatorBase<T, Tuple2<K, T>, MapFunction<T, Tuple2<K, T>>>(extractor, new UnaryOperatorInformation<T, Tuple2<K, T>>(inputType, typeInfoWithKey), "Key Extractor");
		MapOperatorBase<Tuple2<K, T>, T, MapFunction<Tuple2<K, T>, T>> keyRemovingMap = new MapOperatorBase<Tuple2<K, T>, T, MapFunction<Tuple2<K, T>, T>>(new KeyRemovingMapper<T, K>(), new UnaryOperatorInformation<Tuple2<K, T>, T>(typeInfoWithKey, inputType), "Key Extractor");

		keyExtractingMap.setInput(input);
		reducer.setInput(keyExtractingMap);
		keyRemovingMap.setInput(reducer);
		
		// set parallelism
		keyExtractingMap.setParallelism(input.getParallelism());
		reducer.setParallelism(parallelism);
		keyRemovingMap.setParallelism(parallelism);
		
		return keyRemovingMap;
	}
}
