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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase.PartitionMethod;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.UnsupportedLambdaExpressionException;
import org.apache.flink.api.java.operators.translation.KeyExtractingMapper;
import org.apache.flink.api.java.operators.translation.KeyRemovingMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class PartitionedDataSet<IN> {
	
	private final DataSet<IN> dataSet;
	
	private final Keys<IN> pKeys;
	private final PartitionMethod pMethod;
	
	public PartitionedDataSet(DataSet<IN> input, PartitionMethod pMethod, Keys<IN> pKeys) {
		this.dataSet = input;
		
		if(pMethod == PartitionMethod.HASH && pKeys == null) {
			throw new IllegalArgumentException("Hash Partitioning requires keys");
		} else if(pMethod == PartitionMethod.RANGE) {
			throw new UnsupportedOperationException("Range Partitioning not yet supported");
		}
		
		if(pKeys instanceof Keys.FieldPositionKeys<?> && !input.getType().isTupleType()) {
			throw new IllegalArgumentException("Hash Partitioning with key fields only possible on Tuple DataSets");
		}
		
		this.pMethod = pMethod;
		this.pKeys = pKeys;
	}
	
	public PartitionedDataSet(DataSet<IN> input, PartitionMethod pMethod) {
		this(input, pMethod, null);
	}
	
	public DataSet<IN> getDataSet() {
		return this.dataSet;
	}
	
	
	/**
	 * Applies a Map transformation on a {@link DataSet}.<br/>
	 * The transformation calls a {@link org.apache.flink.api.java.functions.RichMapFunction} for each element of the DataSet.
	 * Each MapFunction call returns exactly one element.
	 * 
	 * @param mapper The MapFunction that is called for each element of the DataSet.
	 * @return A MapOperator that represents the transformed DataSet.
	 * 
	 * @see org.apache.flink.api.java.functions.RichMapFunction
	 * @see MapOperator
	 * @see DataSet
	 */
	public <R> MapOperator<IN, R> map(MapFunction<IN, R> mapper) {
		if (mapper == null) {
			throw new NullPointerException("Map function must not be null.");
		}
		if (FunctionUtils.isLambdaFunction(mapper)) {
			throw new UnsupportedLambdaExpressionException();
		}
		
		final TypeInformation<R> resultType = TypeExtractor.getMapReturnTypes(mapper, dataSet.getType());
		
		return new MapOperator<IN, R>(this, resultType, mapper);
	}

	/**
	 * Applies a Map-style operation to the entire partition of the data.
	 * The function is called once per parallel partition of the data,
	 * and the entire partition is available through the given Iterator.
	 * The number of elements that each instance of the MapPartition function
	 * sees is non deterministic and depends on the degree of parallelism of the operation.
	 *
	 * This function is intended for operations that cannot transform individual elements,
	 * requires no grouping of elements. To transform individual elements,
	 * the use of {@code map()} and {@code flatMap()} is preferable.
	 *
	 * @param mapPartition The MapPartitionFunction that is called for the full DataSet.
	 * @return A MapPartitionOperator that represents the transformed DataSet.
	 *
	 * @see MapPartitionFunction
	 * @see MapPartitionOperator
	 * @see DataSet
	 */
	public <R> MapPartitionOperator<IN, R> mapPartition(MapPartitionFunction<IN, R> mapPartition ){
		if (mapPartition == null) {
			throw new NullPointerException("MapPartition function must not be null.");
		}
		
		final TypeInformation<R> resultType = TypeExtractor.getMapPartitionReturnTypes(mapPartition, dataSet.getType());
		
		return new MapPartitionOperator<IN, R>(this, resultType, mapPartition);
	}
	
	/**
	 * Applies a FlatMap transformation on a {@link DataSet}.<br/>
	 * The transformation calls a {@link org.apache.flink.api.java.functions.RichFlatMapFunction} for each element of the DataSet.
	 * Each FlatMapFunction call can return any number of elements including none.
	 * 
	 * @param flatMapper The FlatMapFunction that is called for each element of the DataSet. 
	 * @return A FlatMapOperator that represents the transformed DataSet.
	 * 
	 * @see org.apache.flink.api.java.functions.RichFlatMapFunction
	 * @see FlatMapOperator
	 * @see DataSet
	 */
	public <R> FlatMapOperator<IN, R> flatMap(FlatMapFunction<IN, R> flatMapper) {
		if (flatMapper == null) {
			throw new NullPointerException("FlatMap function must not be null.");
		}
		if (FunctionUtils.isLambdaFunction(flatMapper)) {
			throw new UnsupportedLambdaExpressionException();
		}
		
		TypeInformation<R> resultType = TypeExtractor.getFlatMapReturnTypes(flatMapper, dataSet.getType());
		
		return new FlatMapOperator<IN, R>(this, resultType, flatMapper);
	}
	
	/**
	 * Applies a Filter transformation on a {@link DataSet}.<br/>
	 * The transformation calls a {@link org.apache.flink.api.java.functions.RichFilterFunction} for each element of the DataSet
	 * and retains only those element for which the function returns true. Elements for 
	 * which the function returns false are filtered. 
	 * 
	 * @param filter The FilterFunction that is called for each element of the DataSet.
	 * @return A FilterOperator that represents the filtered DataSet.
	 * 
	 * @see org.apache.flink.api.java.functions.RichFilterFunction
	 * @see FilterOperator
	 * @see DataSet
	 */
	public FilterOperator<IN> filter(FilterFunction<IN> filter) {
		if (filter == null) {
			throw new NullPointerException("Filter function must not be null.");
		}
		return new FilterOperator<IN>(this, filter);
	}
	
	
	/*
	 * Translation of partitioning
	 */
		
	protected org.apache.flink.api.common.operators.SingleInputOperator<?, IN, ?> translateToDataFlow(Operator<IN> input, int partitionDop) {
	
		String name = "Partition";
		
		// distinguish between partition types
		if (pMethod == PartitionMethod.REBALANCE) {
			
			UnaryOperatorInformation<IN, IN> operatorInfo = new UnaryOperatorInformation<IN, IN>(dataSet.getType(), dataSet.getType());
			PartitionOperatorBase<IN> noop = new PartitionOperatorBase<IN>(operatorInfo, pMethod, name);
			// set input
			noop.setInput(input);
			// set DOP
			noop.setDegreeOfParallelism(partitionDop);
			
			return noop;
		} 
		else if (pMethod == PartitionMethod.HASH) {
			
			if (pKeys instanceof Keys.FieldPositionKeys) {
				
				int[] logicalKeyPositions = pKeys.computeLogicalKeyPositions();
				UnaryOperatorInformation<IN, IN> operatorInfo = new UnaryOperatorInformation<IN, IN>(dataSet.getType(), dataSet.getType());
				PartitionOperatorBase<IN> noop = new PartitionOperatorBase<IN>(operatorInfo, pMethod, logicalKeyPositions, name);
				// set input
				noop.setInput(input);
				// set DOP
				noop.setDegreeOfParallelism(partitionDop);
				
				return noop;
			} else if (pKeys instanceof Keys.SelectorFunctionKeys) {
				
				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<IN, ?> selectorKeys = (Keys.SelectorFunctionKeys<IN, ?>) pKeys;
				MapOperatorBase<?, IN, ?> po = translateSelectorFunctionReducer(selectorKeys, pMethod, dataSet.getType(), name, input, partitionDop);
				return po;
			}
			else {
				throw new UnsupportedOperationException("Unrecognized key type.");
			}
			
		} 
		else if (pMethod == PartitionMethod.RANGE) {
			throw new UnsupportedOperationException("Range partitioning not yet supported");
		}
		
		return null;
	}
		
	// --------------------------------------------------------------------------------------------
	
	private static <T, K> MapOperatorBase<Tuple2<K, T>, T, ?> translateSelectorFunctionReducer(Keys.SelectorFunctionKeys<T, ?> rawKeys,
			PartitionMethod pMethod, TypeInformation<T> inputType, String name, Operator<T> input, int partitionDop)
	{
		@SuppressWarnings("unchecked")
		final Keys.SelectorFunctionKeys<T, K> keys = (Keys.SelectorFunctionKeys<T, K>) rawKeys;
		
		TypeInformation<Tuple2<K, T>> typeInfoWithKey = new TupleTypeInfo<Tuple2<K, T>>(keys.getKeyType(), inputType);
		UnaryOperatorInformation<Tuple2<K, T>, Tuple2<K, T>> operatorInfo = new UnaryOperatorInformation<Tuple2<K, T>, Tuple2<K, T>>(typeInfoWithKey, typeInfoWithKey);
		
		KeyExtractingMapper<T, K> extractor = new KeyExtractingMapper<T, K>(keys.getKeyExtractor());
		
		MapOperatorBase<T, Tuple2<K, T>, MapFunction<T, Tuple2<K, T>>> keyExtractingMap = new MapOperatorBase<T, Tuple2<K, T>, MapFunction<T, Tuple2<K, T>>>(extractor, new UnaryOperatorInformation<T, Tuple2<K, T>>(inputType, typeInfoWithKey), "Key Extractor");
		PartitionOperatorBase<Tuple2<K, T>> noop = new PartitionOperatorBase<Tuple2<K, T>>(operatorInfo, pMethod, new int[]{0}, name);
		MapOperatorBase<Tuple2<K, T>, T, MapFunction<Tuple2<K, T>, T>> keyRemovingMap = new MapOperatorBase<Tuple2<K, T>, T, MapFunction<Tuple2<K, T>, T>>(new KeyRemovingMapper<T, K>(), new UnaryOperatorInformation<Tuple2<K, T>, T>(typeInfoWithKey, inputType), "Key Extractor");

		keyExtractingMap.setInput(input);
		noop.setInput(keyExtractingMap);
		keyRemovingMap.setInput(noop);
		
		// set dop
		keyExtractingMap.setDegreeOfParallelism(input.getDegreeOfParallelism());
		noop.setDegreeOfParallelism(partitionDop);
		keyRemovingMap.setDegreeOfParallelism(partitionDop);
		
		return keyRemovingMap;
	}

	
}
