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

import eu.stratosphere.api.common.functions.GenericCombine;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction.Combinable;
import eu.stratosphere.api.java.operators.translation.KeyExtractingMapper;
import eu.stratosphere.api.java.operators.translation.PlanGroupReduceOperator;
import eu.stratosphere.api.java.operators.translation.PlanMapOperator;
import eu.stratosphere.api.java.operators.translation.PlanUnwrappingReduceGroupOperator;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.TypeInformation;

/**
 *
 * @param <IN> The type of the data set consumed by the operator.
 * @param <OUT> The type of the data set created by the operator.
 */
public class ReduceGroupOperator<IN, OUT> extends SingleInputUdfOperator<IN, OUT, ReduceGroupOperator<IN, OUT>> {
	
	private final GroupReduceFunction<IN, OUT> function;
	
	private final Grouping<IN> grouper;
	
	private boolean combinable;
	
	
	/**
	 * Constructor for a non-grouped reduce (all reduce).
	 * 
	 * @param input
	 * @param function
	 */
	public ReduceGroupOperator(DataSet<IN> input, GroupReduceFunction<IN, OUT> function) {
		super(input, TypeExtractor.getGroupReduceReturnTypes(function, input.getType()));
		
		if (function == null) {
			throw new NullPointerException("GroupReduce function must not be null.");
		}
		
		this.function = function;
		this.grouper = null;
		checkCombinability();
	}
	
	/**
	 * Constructor for a grouped reduce.
	 * 
	 * @param input
	 * @param function
	 */
	public ReduceGroupOperator(Grouping<IN> input, GroupReduceFunction<IN, OUT> function) {
		super(input != null ? input.getDataSet() : null, TypeExtractor.getGroupReduceReturnTypes(function, input.getDataSet().getType()));
		
		if (function == null) {
			throw new NullPointerException("GroupReduce function must not be null.");
		}
		
		this.function = function;
		this.grouper = input;
		checkCombinability();
	}
	
	private void checkCombinability() {
		if (function instanceof GenericCombine && function.getClass().getAnnotation(Combinable.class) != null) {
			this.combinable = true;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------
	
	public boolean isCombinable() {
		return combinable;
	}
	
	public void setCombinable(boolean combinable) {
		// sanity check that the function is a subclass of the combine interface
		if (combinable && !(function instanceof GenericCombine)) {
			throw new IllegalArgumentException("The function does not implement the combine interface.");
		}
		
		this.combinable = combinable;
	}
	
	@Override
	protected Operator translateToDataFlow(Operator input) {
		
		String name = getName() != null ? getName() : function.getClass().getName();
		
		// distinguish between grouped reduce and non-grouped reduce
		if (grouper == null) {
			// non grouped reduce
			PlanGroupReduceOperator<IN, OUT> po = 
					new PlanGroupReduceOperator<IN, OUT>(function, new int[0], name, getInputType(), getResultType());
			po.setCombinable(combinable);
			
			po.setInput(input);
			// the degree of parallelism for a non grouped reduce can only be 1
			po.setDegreeOfParallelism(1);
			return po;
		}
	
		if (grouper.getKeys() instanceof Keys.SelectorFunctionKeys) {
		
			@SuppressWarnings("unchecked")
			Keys.SelectorFunctionKeys<IN, ?> selectorKeys = (Keys.SelectorFunctionKeys<IN, ?>) grouper.getKeys();
			
			PlanUnwrappingReduceGroupOperator<IN, OUT, ?> po = translateSelectorFunctionReducer(
							selectorKeys, function, getInputType(), getResultType(), name, input, isCombinable());
			po.setCombinable(combinable);
			
			// set dop
			po.setDegreeOfParallelism(this.getParallelism());
			
			return po;
		}
		else if (grouper.getKeys() instanceof Keys.FieldPositionKeys) {

			int[] logicalKeyPositions = grouper.getKeys().computeLogicalKeyPositions();
			PlanGroupReduceOperator<IN, OUT> po = 
					new PlanGroupReduceOperator<IN, OUT>(function, logicalKeyPositions, name, getInputType(), getResultType());
			po.setCombinable(combinable);
			
			// set input
			po.setInput(input);
			// set dop
			po.setDegreeOfParallelism(this.getParallelism());
			
			// set group order
			if(grouper instanceof SortedGrouping) {
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
	
	private static <IN, OUT, K> PlanUnwrappingReduceGroupOperator<IN, OUT, K> translateSelectorFunctionReducer(
			Keys.SelectorFunctionKeys<IN, ?> rawKeys, GroupReduceFunction<IN, OUT> function,
			TypeInformation<IN> inputType, TypeInformation<OUT> outputType, String name, Operator input,
			boolean combinable)
	{
		@SuppressWarnings("unchecked")
		final Keys.SelectorFunctionKeys<IN, K> keys = (Keys.SelectorFunctionKeys<IN, K>) rawKeys;
		
		TypeInformation<Tuple2<K, IN>> typeInfoWithKey = new TupleTypeInfo<Tuple2<K, IN>>(keys.getKeyType(), inputType);
		
		KeyExtractingMapper<IN, K> extractor = new KeyExtractingMapper<IN, K>(keys.getKeyExtractor());
		
		PlanUnwrappingReduceGroupOperator<IN, OUT, K> reducer = new PlanUnwrappingReduceGroupOperator<IN, OUT, K>(function, keys, name, inputType, outputType, typeInfoWithKey, combinable);
		
		PlanMapOperator<IN, Tuple2<K, IN>> mapper = new PlanMapOperator<IN, Tuple2<K, IN>>(extractor, "Key Extractor", inputType, typeInfoWithKey);

		reducer.setInput(mapper);
		mapper.setInput(input);
		
		// set the mapper's parallelism to the input parallelism to make sure it is chained
		mapper.setDegreeOfParallelism(input.getDegreeOfParallelism());
		
		return reducer;
	}
}
