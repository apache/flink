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

package org.apache.flink.api.java.aggregation;

import static org.apache.flink.api.java.aggregation.Aggregations.key;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.AggregationFunction.ResultTypeBehavior;
import org.apache.flink.api.java.operators.AggregationOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;

import com.google.common.primitives.Ints;

/**
 * Factory method container to construct an
 * {@link AggregationOperator} from a {@link DataSet} or
 * {@link UnsortedGrouping}.
 * 
 * <p>The factory performs the following tasks:
 * 
 * <ol>
 *  <li>Extract key fields from a grouped DataSet.
 * 	<li>Compute the aggregation result type.
 *  <li>Create the result tuple.
 * </ol>
 *
 * <p>Note: Each task is implemented in a member class in order to
 * facilitate testing. 
 */
public class AggregationOperatorFactory {
	
	private static final AggregationOperatorFactory INSTANCE = new AggregationOperatorFactory();
	private AggregationFunctionPreprocessor aggregationFunctionPreprocessor = new AggregationFunctionPreprocessor();
	private ResultTypeFactory resultTypeFactory = new ResultTypeFactory();
	
	
	/**
	 * Construct an {@link AggregationOperator} that implements the
	 * aggregation functions listed in {@code functions} on the
	 * (ungrouped) DataSet {@code input}. 
	 * @param input	An (ungrouped) DataSet.
	 * @param functions The aggregation functions that should be computed.
	 * @return An AggregationOperator representing the specified aggregations.
	 */
	public <T, R extends Tuple> AggregationOperator<T, R> aggregate(DataSet<T> input, AggregationFunction<?, ?>[] functions) {
		AggregationOperator<T, R> op = createAggregationOperator(input, new int[0], functions);
		return op;
	}

	/**
	 * Construct an {@link AggregationOperator} that implements the
	 * aggregation functions listed in {@code functions} on the grouped
	 * DataSet {@code input}.
	 * 
	 * <p>If there are no {@link Aggregations.keys} specified in
	 * {@code functions} then a {@code key()} aggregation function is
	 * inserted for each group key.
	 *  
	 * @param input	An grouped DataSet.
	 * @param functions The aggregation functions that should be computed.
	 * @return An AggregationOperator representing the specified aggregations.
	 */
	public <T, R extends Tuple> AggregationOperator<T, R> aggregate(UnsortedGrouping<T> grouping, AggregationFunction<?, ?>[] functions) {
		DataSet<T> input = grouping.getDataSet();
		int[] groupKeys = grouping.getKeys().computeLogicalKeyPositions();
		AggregationOperator<T, R> op = createAggregationOperator(input, groupKeys, functions);
		return op;
	}
	
	// TODO if sum and/or count are present, use these to compute average
	<T, R extends Tuple> AggregationOperator<T, R> createAggregationOperator(DataSet<T> input, int[] groupKeys, AggregationFunction<?, ?>[] functions) {
		AggregationFunction<?, ?>[] intermediateFunctions = aggregationFunctionPreprocessor.createIntermediateFunctions(functions, groupKeys);
		int[] intermediateGroupKeys = aggregationFunctionPreprocessor.createIntermediateGroupKeys(intermediateFunctions);
		TypeInformation<R> resultType = resultTypeFactory.createAggregationResultType(input.getType(), functions);
		TypeInformation<Tuple> intermediateType = resultTypeFactory.createAggregationResultType(input.getType(), intermediateFunctions);
		AggregationOperator<T, R> op = new AggregationOperator<T, R>(input, resultType, intermediateType, intermediateGroupKeys, functions, intermediateFunctions);
		return op;
	}

	static class AggregationFunctionPreprocessor {

		public AggregationFunction<?, ?>[] createIntermediateFunctions(AggregationFunction<?, ?>[] functions, int[] groupKeys) {
			List<AggregationFunction<?, ?>> intermediates = new ArrayList<AggregationFunction<?,?>>();
			int outputPosition = 0;
			for (AggregationFunction<?, ?> function : functions) {
				function.setOutputPosition(outputPosition);
				outputPosition += 1;
				if (groupKeys.length == 0
						&& function instanceof KeySelectionAggregationFunction) {
					throw new IllegalArgumentException("Key selection aggregation function can only be used on grouped DataSets.");
				}
				if (function instanceof CompositeAggregationFunction) {
					CompositeAggregationFunction<?, ?> composite = (CompositeAggregationFunction<?, ?>) function;
					List<AggregationFunction<?, ?>> compositeIntermediates = composite.getIntermediateAggregationFunctions();
					intermediates.addAll(compositeIntermediates);
				} else {
					intermediates.add(function);
				}
			}
			for (int groupKey : groupKeys) {
				AggregationFunction<?, ?> key = key(groupKey);
				if ( ! intermediates.contains(key) ) {
					intermediates.add(key);
				}
			}
			int intermediatePosition = 0;
			for (AggregationFunction<?, ?> function : intermediates) {
				function.setIntermediatePosition(intermediatePosition);
				intermediatePosition += 1;
			}
			AggregationFunction<?, ?>[] result = new AggregationFunction<?, ?>[intermediatePosition];
			intermediates.toArray(result);
			return result;
		}

		public int[] createIntermediateGroupKeys(AggregationFunction<?, ?>[] intermediates) {
			List<Integer> positions = new ArrayList<Integer>();
			for (AggregationFunction<?, ?> function : intermediates) {
				if (function instanceof KeySelectionAggregationFunction) {
					int intermediatePosition = function.getIntermediatePosition();
					positions.add(intermediatePosition);
				}
			}
			int[] result = Ints.toArray(positions);
			return result;
		}
		
		
	}
	
	static class ResultTypeFactory {
	
		<R extends Tuple> TypeInformation<R> createAggregationResultType(TypeInformation<?> inputType, AggregationFunction<?, ?>... functions) {
			
			// assume input is tuple
			Validate.isInstanceOf(TupleTypeInfoBase.class, inputType, "Aggregations are only implemented on tuples.");
			TupleTypeInfoBase<?> inputTypeAsTuple = (TupleTypeInfoBase<?>) inputType;

			// construct output tuple
			int arity =  functions.length;
			Validate.inclusiveBetween(1, Tuple.MAX_ARITY, arity, "Output tuple of aggregation must have between 1 and %s elements; requested tuple has %s elements.", Tuple.MAX_ARITY, functions.length);
			BasicTypeInfo<?>[] types = new BasicTypeInfo[arity];
			for (int i = 0; i < functions.length; ++i) {
				processAggregationFunction(inputTypeAsTuple, types, i, functions[i]);
			}
			TypeInformation<R> resultType = new TupleTypeInfo<R>(types);
			return resultType;
		}

		private <T> void processAggregationFunction(TupleTypeInfoBase<?> inputTypeAsTuple,
				BasicTypeInfo<?>[] types, int i,
				AggregationFunction<T, ?> function) {

			// assume field type is simple
			int fieldPosition = function.getInputPosition();
			TypeInformation<Object> fieldType = inputTypeAsTuple.getTypeAt(fieldPosition);
			Validate.isInstanceOf(BasicTypeInfo.class, fieldType);
			@SuppressWarnings("unchecked")
			BasicTypeInfo<T> basicFieldType = (BasicTypeInfo<T>) fieldType;

			// let the aggregation function know the specific input type
			function.setInputType(basicFieldType);

			// set the result type based on the aggregation function result type behavior
			ResultTypeBehavior resultTypeBehavior = function.getResultTypeBehavior();
			if (resultTypeBehavior == ResultTypeBehavior.FIXED) {
				types[i] = function.getResultType();
			} else if (resultTypeBehavior == ResultTypeBehavior.INPUT) {
				types[i] = basicFieldType;
			} else {
				throw new IllegalStateException("Unknown aggregation function result type behavior: " + resultTypeBehavior);
			}
		}
	}
	
	public static AggregationOperatorFactory getInstance() {
		return INSTANCE;
	}

	ResultTypeFactory getResultTypeFactory() {
		return resultTypeFactory;
	}

	void setResultTypeFactory(ResultTypeFactory resultTypeFactory) {
		this.resultTypeFactory = resultTypeFactory;
	}

	AggregationFunctionPreprocessor getAggregationFunctionPreprocessor() {
		return aggregationFunctionPreprocessor;
	}

	void setAggregationFunctionPreprocessor(
			AggregationFunctionPreprocessor aggregationFunctionPreprocessor) {
		this.aggregationFunctionPreprocessor = aggregationFunctionPreprocessor;
	}
	
}
