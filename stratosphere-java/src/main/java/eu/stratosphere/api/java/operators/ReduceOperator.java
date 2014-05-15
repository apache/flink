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

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.operators.translation.KeyExtractingMapper;
import eu.stratosphere.api.java.operators.translation.KeyRemovingMapper;
import eu.stratosphere.api.java.operators.translation.PlanMapOperator;
import eu.stratosphere.api.java.operators.translation.PlanReduceOperator;
import eu.stratosphere.api.java.operators.translation.PlanUnwrappingReduceOperator;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeInformation;

/**
 *
 * @param <IN> The type of the data set reduced by the operator.
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
		
		if (function == null) {
			throw new NullPointerException("Reduce function must not be null.");
		}
		
		this.function = function;
		this.grouper = null;
	}
	
	
	public ReduceOperator(Grouping<IN> input, ReduceFunction<IN> function) {
		super(input.getDataSet(), input.getDataSet().getType());
		
		if (function == null) {
			throw new NullPointerException("Reduce function must not be null.");
		}
		
		this.function = function;
		this.grouper = input;
	}

	@Override
	protected eu.stratosphere.api.common.operators.SingleInputOperator<?> translateToDataFlow(Operator input) {
		String name = getName() != null ? getName() : function.getClass().getName();
		
		// distinguish between grouped reduce and non-grouped reduce
		if (grouper == null) {
			// non grouped reduce
			PlanReduceOperator<IN> po = new PlanReduceOperator<IN>(function, new int[0], name, getInputType());
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
			
			PlanMapOperator<?, IN> po = translateSelectorFunctionReducer(selectorKeys, function, getInputType(), name, input, this.getParallelism());			
			return po;
		}
		else if (grouper.getKeys() instanceof Keys.FieldPositionKeys) {
			
			// reduce with field positions
			int[] logicalKeyPositions = grouper.getKeys().computeLogicalKeyPositions();
			PlanReduceOperator<IN> po = new PlanReduceOperator<IN>(function, logicalKeyPositions, name, getInputType());
			
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
	
	private static <T, K> PlanMapOperator<Tuple2<K, T>, T> translateSelectorFunctionReducer(Keys.SelectorFunctionKeys<T, ?> rawKeys,
			ReduceFunction<T> function, TypeInformation<T> inputType, String name, Operator input, int dop)
	{
		@SuppressWarnings("unchecked")
		final Keys.SelectorFunctionKeys<T, K> keys = (Keys.SelectorFunctionKeys<T, K>) rawKeys;
		
		TypeInformation<Tuple2<K, T>> typeInfoWithKey = new TupleTypeInfo<Tuple2<K, T>>(keys.getKeyType(), inputType);
		
		KeyExtractingMapper<T, K> extractor = new KeyExtractingMapper<T, K>(keys.getKeyExtractor());
		
		PlanUnwrappingReduceOperator<T, K> reducer = new PlanUnwrappingReduceOperator<T, K>(function, keys, name, inputType, typeInfoWithKey);
		
		PlanMapOperator<T, Tuple2<K, T>> keyExtractingMap = new PlanMapOperator<T, Tuple2<K, T>>(extractor, "Key Extractor", inputType, typeInfoWithKey);
		PlanMapOperator<Tuple2<K, T>, T> keyRemovingMap = new PlanMapOperator<Tuple2<K, T>, T>(new KeyRemovingMapper<T, K>(), "Key Remover", typeInfoWithKey, inputType);

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
