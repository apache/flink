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

import org.apache.flink.api.common.functions.FlatCombinable;
import org.apache.flink.api.common.functions.GroupReducible;
import org.apache.flink.api.common.functions.Mappable;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.java.functions.GroupReduceFunction;
import org.apache.flink.api.java.operators.translation.KeyExtractingMapper;
import org.apache.flink.api.java.operators.translation.PlanUnwrappingReduceGroupOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.types.TypeInformation;

import org.apache.flink.api.java.DataSet;


/**
 * This operator represents the application of a "reduceGroup" function on a data set, and the
 * result data set produced by the function.
 * 
 * @param <IN> The type of the data set consumed by the operator.
 * @param <OUT> The type of the data set created by the operator.
 */
public class GroupReduceOperator<IN, OUT> extends SingleInputUdfOperator<IN, OUT, GroupReduceOperator<IN, OUT>> {

	private final GroupReducible<IN, OUT> function;

	private final Grouping<IN> grouper;

	// reduceFunction is a GroupReduceFunction
	private boolean richFunction;

	private boolean combinable;

	/**
	 * Constructor for a non-grouped reduce (all reduce).
	 * 
	 * @param input The input data set to the groupReduce function.
	 * @param function The user-defined GroupReduce function.
	 */
	public GroupReduceOperator(DataSet<IN> input, GroupReducible<IN, OUT> function) {
		super(input, TypeExtractor.getGroupReduceReturnTypes(function, input.getType()));
		
		this.function = function;
		this.grouper = null;

		checkCombinability();
	}
	
	/**
	 * Constructor for a grouped reduce.
	 * 
	 * @param input The grouped input to be processed group-wise by the groupReduce function.
	 * @param function The user-defined GroupReduce function.
	 */
	public GroupReduceOperator(Grouping<IN> input, GroupReducible<IN, OUT> function) {
		super(input != null ? input.getDataSet() : null, TypeExtractor.getGroupReduceReturnTypes(function, input.getDataSet().getType()));
		
		this.function = function;
		this.grouper = input;

		checkCombinability();

		extractSemanticAnnotationsFromUdf(function.getClass());
	}

	private void checkCombinability() {
		if (function instanceof FlatCombinable &&
				function.getClass().getAnnotation(GroupReduceFunction.Combinable.class) != null) {
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
		if (combinable && !(function instanceof FlatCombinable)) {
			throw new IllegalArgumentException("The function does not implement the combine interface.");
		}
		
		this.combinable = combinable;
	}
	
	@Override
	protected org.apache.flink.api.common.operators.base.GroupReduceOperatorBase<?, OUT, ?> translateToDataFlow(Operator<IN> input) {

		String name = getName() != null ? getName() : function.getClass().getName();
		
		// distinguish between grouped reduce and non-grouped reduce
		if (grouper == null) {
			// non grouped reduce
			UnaryOperatorInformation<IN, OUT> operatorInfo = new UnaryOperatorInformation<IN, OUT>(getInputType(), getResultType());
			GroupReduceOperatorBase<IN, OUT, GroupReducible<IN, OUT>> po =
					new GroupReduceOperatorBase<IN, OUT, GroupReducible<IN, OUT>>(function, operatorInfo, new int[0], name);

			po.setCombinable(combinable);
			// set input
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
			
			po.setDegreeOfParallelism(this.getParallelism());
			
			return po;
		}
		else if (grouper.getKeys() instanceof Keys.FieldPositionKeys) {

			int[] logicalKeyPositions = grouper.getKeys().computeLogicalKeyPositions();
			UnaryOperatorInformation<IN, OUT> operatorInfo = new UnaryOperatorInformation<IN, OUT>(getInputType(), getResultType());
			GroupReduceOperatorBase<IN, OUT, GroupReducible<IN, OUT>> po =
					new GroupReduceOperatorBase<IN, OUT, GroupReducible<IN, OUT>>(function, operatorInfo, logicalKeyPositions, name);

			po.setCombinable(combinable);
			po.setInput(input);
			po.setDegreeOfParallelism(this.getParallelism());
			
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
	
	private static <IN, OUT, K> PlanUnwrappingReduceGroupOperator<IN, OUT, K> translateSelectorFunctionReducer(
			Keys.SelectorFunctionKeys<IN, ?> rawKeys, GroupReducible<IN, OUT> function,
			TypeInformation<IN> inputType, TypeInformation<OUT> outputType, String name, Operator<IN> input,
			boolean combinable)
	{
		@SuppressWarnings("unchecked")
		final Keys.SelectorFunctionKeys<IN, K> keys = (Keys.SelectorFunctionKeys<IN, K>) rawKeys;
		
		TypeInformation<Tuple2<K, IN>> typeInfoWithKey = new TupleTypeInfo<Tuple2<K, IN>>(keys.getKeyType(), inputType);
		
		KeyExtractingMapper<IN, K> extractor = new KeyExtractingMapper<IN, K>(keys.getKeyExtractor());
		
		PlanUnwrappingReduceGroupOperator<IN, OUT, K> reducer = new PlanUnwrappingReduceGroupOperator<IN, OUT, K>(function, keys, name, outputType, typeInfoWithKey, combinable);
		
		MapOperatorBase<IN, Tuple2<K, IN>, Mappable<IN, Tuple2<K, IN>>> mapper = new MapOperatorBase<IN, Tuple2<K, IN>, Mappable<IN, Tuple2<K, IN>>>(extractor, new UnaryOperatorInformation<IN, Tuple2<K, IN>>(inputType, typeInfoWithKey), "Key Extractor");

		reducer.setInput(mapper);
		mapper.setInput(input);
		
		// set the mapper's parallelism to the input parallelism to make sure it is chained
		mapper.setDegreeOfParallelism(input.getDegreeOfParallelism());
		
		return reducer;
	}



}
