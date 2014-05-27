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

import java.util.Iterator;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.functions.GenericGroupReduce;
import eu.stratosphere.api.common.functions.GenericMap;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.UnaryOperatorInformation;
import eu.stratosphere.api.common.operators.base.GroupReduceOperatorBase;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.operators.translation.KeyExtractingMapper;
import eu.stratosphere.api.java.operators.translation.PlanUnwrappingReduceGroupOperator;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.types.TypeInformation;
import eu.stratosphere.util.Collector;

/**
 * This operator represents the application of a "distinct" function on a data set, and the
 * result data set produced by the function.
 * 
 * @param <T> The type of the data set made distinct by the operator.
 */
public class DistinctOperator<T> extends SingleInputOperator<T, T, DistinctOperator<T>> {
	
	private final Keys<T> keys;
	
	public DistinctOperator(DataSet<T> input, Keys<T> keys) {
		super(input, input.getType());
		
		// if keys is null distinction is done on all tuple fields
		if (keys == null) {
			if (input.getType().isTupleType()) {
				
				TupleTypeInfo<?> tupleType = (TupleTypeInfo<?>) input.getType();
				int[] allFields = new int[tupleType.getArity()];
				for(int i = 0; i < tupleType.getArity(); i++) {
					allFields[i] = i;
				}
				keys = new Keys.FieldPositionKeys<T>(allFields, input.getType(), true);
			}
			else {
				throw new InvalidProgramException("Distinction on all fields is only possible on tuple data types.");
			}
		}
		
		
		// FieldPositionKeys can only be applied on Tuples
		if (keys instanceof Keys.FieldPositionKeys && !input.getType().isTupleType()) {
			throw new InvalidProgramException("Distinction on field positions is only possible on tuple data types.");
		}
		
		this.keys = keys;
	}

	@Override
	protected eu.stratosphere.api.common.operators.base.GroupReduceOperatorBase<?, T, ?> translateToDataFlow(Operator<T> input) {
		
		GroupReduceFunction<T, T> function = new DistinctFunction<T>();
		String name = function.getClass().getName();
		
		if (keys instanceof Keys.FieldPositionKeys) {

			int[] logicalKeyPositions = keys.computeLogicalKeyPositions();
			UnaryOperatorInformation<T, T> operatorInfo = new UnaryOperatorInformation<T, T>(getInputType(), getResultType());
			GroupReduceOperatorBase<T, T, GenericGroupReduce<T, T>> po =
					new GroupReduceOperatorBase<T, T, GenericGroupReduce<T, T>>(function, operatorInfo, logicalKeyPositions, name);

			po.setCombinable(true);
			po.setInput(input);
			po.setDegreeOfParallelism(this.getParallelism());
			
			return po;
		}
		else if (keys instanceof Keys.SelectorFunctionKeys) {
		
			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<T, ?> selectorKeys = (Keys.SelectorFunctionKeys<T, ?>) keys;
			
			PlanUnwrappingReduceGroupOperator<T, T, ?> po = translateSelectorFunctionDistinct(
							selectorKeys, function, getInputType(), getResultType(), name, input, true);
			
			po.setDegreeOfParallelism(this.getParallelism());
			
			return po;
		}
		else {
			throw new UnsupportedOperationException("Unrecognized key type.");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static <IN, OUT, K> PlanUnwrappingReduceGroupOperator<IN, OUT, K> translateSelectorFunctionDistinct(
			Keys.SelectorFunctionKeys<IN, ?> rawKeys, GroupReduceFunction<IN, OUT> function,
			TypeInformation<IN> inputType, TypeInformation<OUT> outputType, String name, Operator<IN> input,
			boolean combinable)
	{
		@SuppressWarnings("unchecked")
		final Keys.SelectorFunctionKeys<IN, K> keys = (Keys.SelectorFunctionKeys<IN, K>) rawKeys;
		
		TypeInformation<Tuple2<K, IN>> typeInfoWithKey = new TupleTypeInfo<Tuple2<K, IN>>(keys.getKeyType(), inputType);
		
		KeyExtractingMapper<IN, K> extractor = new KeyExtractingMapper<IN, K>(keys.getKeyExtractor());
		
		PlanUnwrappingReduceGroupOperator<IN, OUT, K> reducer = new PlanUnwrappingReduceGroupOperator<IN, OUT, K>(function, keys, name, outputType, typeInfoWithKey, combinable);
		
		MapOperatorBase<IN, Tuple2<K, IN>, GenericMap<IN, Tuple2<K, IN>>> mapper = new MapOperatorBase<IN, Tuple2<K, IN>, GenericMap<IN, Tuple2<K, IN>>>(extractor, new UnaryOperatorInformation<IN, Tuple2<K, IN>>(inputType, typeInfoWithKey), "Key Extractor");

		reducer.setInput(mapper);
		mapper.setInput(input);
		
		// set the mapper's parallelism to the input parallelism to make sure it is chained
		mapper.setDegreeOfParallelism(input.getDegreeOfParallelism());
		
		return reducer;
	}
	
	public static final class DistinctFunction<T> extends GroupReduceFunction<T, T> {

		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterator<T> values, Collector<T> out)
				throws Exception {
			out.collect(values.next());
		}
	}
}
