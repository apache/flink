/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.operators;

import eu.stratosphere.api.common.functions.GenericMap;
import eu.stratosphere.api.common.functions.GenericReduce;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.UnaryOperatorInformation;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.api.common.operators.base.ReduceOperatorBase;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.operators.translation.KeyExtractingMapper;
import eu.stratosphere.api.java.operators.translation.KeyRemovingMapper;
import eu.stratosphere.api.java.operators.translation.PlanUnwrappingReduceOperator;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.types.TypeInformation;

/**
 * This operator represents the application of a "reduce" function on a data set, and the
 * result data set produced by the function.
 * 
 * @param <IN> The type of the data set reduced by the operator.
 * 
 * @see ReduceFunction
 */
public class ReduceOperator<IN> extends SingleInputUdfOperator<IN, IN, ReduceOperator<IN>> {
	
	private final ReduceFunction<IN> function;
	
	private final Grouping<IN> grouper;
	
	/**
	 * 
	 * This is the case for a reduce-all case (in contrast to the reduce-per-group case).
	 * 
	 * @param input
	 * @param function
	 */
	public ReduceOperator(DataSet<IN> input, ReduceFunction<IN> function) {
		super(input, input.getType());
		
		this.function = function;
		this.grouper = null;
		
		extractSemanticAnnotationsFromUdf(function.getClass());
	}

	public ReduceOperator(DataSet<IN> input, ReduceFunction<IN> function, IN initialValue) {
		super(input, input.getType());

		this.function = function;
		this.function.setInitialValue(initialValue);
		this.grouper = null;

		extractSemanticAnnotationsFromUdf(function.getClass());
	}
	
	
	public ReduceOperator(Grouping<IN> input, ReduceFunction<IN> function) {
		super(input.getDataSet(), input.getDataSet().getType());
		
		this.function = function;
		this.grouper = input;
		
		extractSemanticAnnotationsFromUdf(function.getClass());
	}

	@Override
	protected eu.stratosphere.api.common.operators.SingleInputOperator<?, IN, ?> translateToDataFlow(Operator<IN> input) {
		
		String name = getName() != null ? getName() : function.getClass().getName();
		
		// distinguish between grouped reduce and non-grouped reduce
		if (grouper == null) {
			// non grouped reduce
			UnaryOperatorInformation<IN, IN> operatorInfo = new UnaryOperatorInformation<IN, IN>(getInputType(), getInputType());
			ReduceOperatorBase<IN, GenericReduce<IN>> po =
					new ReduceOperatorBase<IN, GenericReduce<IN>>(function, operatorInfo, new int[0], name);
			// set input
			po.setInput(input);
			
			// the degree of parallelism for a non grouped reduce can only be 1
			po.setDegreeOfParallelism(1);
			
			return po;
		}
		
		if (grouper.getKeys() instanceof Keys.SelectorFunctionKeys) {
			
			// reduce with key selector function
			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<IN, ?> selectorKeys = (Keys.SelectorFunctionKeys<IN, ?>) grouper.getKeys();
			
			MapOperatorBase<?, IN, ?> po = translateSelectorFunctionReducer(selectorKeys, function, getInputType(), name, input, this.getParallelism());
			return po;
		}
		else if (grouper.getKeys() instanceof Keys.FieldPositionKeys ||
				grouper.getKeys() instanceof Keys.ExpressionKeys) {
			
			// reduce with field positions
			int[] logicalKeyPositions = grouper.getKeys().computeLogicalKeyPositions();
			UnaryOperatorInformation<IN, IN> operatorInfo = new UnaryOperatorInformation<IN, IN>(getInputType(), getInputType());
			ReduceOperatorBase<IN, GenericReduce<IN>> po =
					new ReduceOperatorBase<IN, GenericReduce<IN>>(function, operatorInfo, logicalKeyPositions, name);
			
			// set input
			po.setInput(input);
			// set dop
			po.setDegreeOfParallelism(this.getParallelism());
			
			return po;
		}
		else {
			throw new UnsupportedOperationException("Unrecognized key type.");
		}
		
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static <T, K> MapOperatorBase<Tuple2<K, T>, T, ?> translateSelectorFunctionReducer(Keys.SelectorFunctionKeys<T, ?> rawKeys,
			ReduceFunction<T> function, TypeInformation<T> inputType, String name, Operator<T> input, int dop)
	{
		@SuppressWarnings("unchecked")
		final Keys.SelectorFunctionKeys<T, K> keys = (Keys.SelectorFunctionKeys<T, K>) rawKeys;
		
		TypeInformation<Tuple2<K, T>> typeInfoWithKey = new TupleTypeInfo<Tuple2<K, T>>(keys.getKeyType(), inputType);
		
		KeyExtractingMapper<T, K> extractor = new KeyExtractingMapper<T, K>(keys.getKeyExtractor());
		
		PlanUnwrappingReduceOperator<T, K> reducer = new PlanUnwrappingReduceOperator<T, K>(function, keys, name, inputType, typeInfoWithKey);
		
		MapOperatorBase<T, Tuple2<K, T>, GenericMap<T, Tuple2<K, T>>> keyExtractingMap = new MapOperatorBase<T, Tuple2<K, T>, GenericMap<T, Tuple2<K, T>>>(extractor, new UnaryOperatorInformation<T, Tuple2<K, T>>(inputType, typeInfoWithKey), "Key Extractor");
		MapOperatorBase<Tuple2<K, T>, T, GenericMap<Tuple2<K, T>, T>> keyRemovingMap = new MapOperatorBase<Tuple2<K, T>, T, GenericMap<Tuple2<K, T>, T>>(new KeyRemovingMapper<T, K>(), new UnaryOperatorInformation<Tuple2<K, T>, T>(typeInfoWithKey, inputType), "Key Extractor");

		keyExtractingMap.setInput(input);
		reducer.setInput(keyExtractingMap);
		keyRemovingMap.setInput(reducer);
		
		// set dop
		keyExtractingMap.setDegreeOfParallelism(input.getDegreeOfParallelism());
		reducer.setDegreeOfParallelism(dop);
		keyRemovingMap.setDegreeOfParallelism(dop);
		
		return keyRemovingMap;
	}
}
