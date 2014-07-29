/**
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

import org.apache.flink.api.common.functions.Mappable;
import org.apache.flink.api.common.functions.Reducible;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.java.operators.translation.KeyExtractingMapper;
import org.apache.flink.api.java.operators.translation.KeyRemovingMapper;
import org.apache.flink.api.java.operators.translation.PlanUnwrappingReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.types.TypeInformation;

import org.apache.flink.api.java.DataSet;

/**
 * This operator represents the application of a "reduce" function on a data set, and the
 * result data set produced by the function.
 * 
 * @param <IN> The type of the data set reduced by the operator.
 * 
 * @see Reducible
 */
public class ReduceOperator<IN> extends SingleInputUdfOperator<IN, IN, ReduceOperator<IN>> {
	
	private final Reducible<IN> function;
	
	private final Grouping<IN> grouper;
	
	/**
	 * 
	 * This is the case for a reduce-all case (in contrast to the reduce-per-group case).
	 * 
	 * @param input
	 * @param function
	 */
	public ReduceOperator(DataSet<IN> input, Reducible<IN> function) {
		super(input, input.getType());
		
		this.function = function;
		this.grouper = null;
		
		extractSemanticAnnotationsFromUdf(function.getClass());
	}
	
	
	public ReduceOperator(Grouping<IN> input, Reducible<IN> function) {
		super(input.getDataSet(), input.getDataSet().getType());
		
		this.function = function;
		this.grouper = input;
		
		extractSemanticAnnotationsFromUdf(function.getClass());
	}

	@Override
	protected org.apache.flink.api.common.operators.SingleInputOperator<?, IN, ?> translateToDataFlow(Operator<IN> input) {
		
		String name = getName() != null ? getName() : function.getClass().getName();
		
		// distinguish between grouped reduce and non-grouped reduce
		if (grouper == null) {
			// non grouped reduce
			UnaryOperatorInformation<IN, IN> operatorInfo = new UnaryOperatorInformation<IN, IN>(getInputType(), getInputType());
			ReduceOperatorBase<IN, Reducible<IN>> po =
					new ReduceOperatorBase<IN, Reducible<IN>>(function, operatorInfo, new int[0], name);
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
			ReduceOperatorBase<IN, Reducible<IN>> po =
					new ReduceOperatorBase<IN, Reducible<IN>>(function, operatorInfo, logicalKeyPositions, name);
			
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
			Reducible<T> function, TypeInformation<T> inputType, String name, Operator<T> input, int dop)
	{
		@SuppressWarnings("unchecked")
		final Keys.SelectorFunctionKeys<T, K> keys = (Keys.SelectorFunctionKeys<T, K>) rawKeys;
		
		TypeInformation<Tuple2<K, T>> typeInfoWithKey = new TupleTypeInfo<Tuple2<K, T>>(keys.getKeyType(), inputType);
		
		KeyExtractingMapper<T, K> extractor = new KeyExtractingMapper<T, K>(keys.getKeyExtractor());
		
		PlanUnwrappingReduceOperator<T, K> reducer = new PlanUnwrappingReduceOperator<T, K>(function, keys, name, inputType, typeInfoWithKey);
		
		MapOperatorBase<T, Tuple2<K, T>, Mappable<T, Tuple2<K, T>>> keyExtractingMap = new MapOperatorBase<T, Tuple2<K, T>, Mappable<T, Tuple2<K, T>>>(extractor, new UnaryOperatorInformation<T, Tuple2<K, T>>(inputType, typeInfoWithKey), "Key Extractor");
		MapOperatorBase<Tuple2<K, T>, T, Mappable<Tuple2<K, T>, T>> keyRemovingMap = new MapOperatorBase<Tuple2<K, T>, T, Mappable<Tuple2<K, T>, T>>(new KeyRemovingMapper<T, K>(), new UnaryOperatorInformation<Tuple2<K, T>, T>(typeInfoWithKey, inputType), "Key Extractor");

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
