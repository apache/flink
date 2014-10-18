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
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase.PartitionMethod;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.translation.KeyExtractingMapper;
import org.apache.flink.api.java.operators.translation.KeyRemovingMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

/**
 * This operator represents a partitioning.
 *
 * @param <T> The type of the data being partitioned.
 */
public class PartitionOperator<T> extends SingleInputUdfOperator<T, T, PartitionOperator<T>> {
	
	private final Keys<T> pKeys;
	private final PartitionMethod pMethod;
	
	public PartitionOperator(DataSet<T> input, PartitionMethod pMethod, Keys<T> pKeys) {
		super(input, input.getType());

		if(pMethod == PartitionMethod.HASH && pKeys == null) {
			throw new IllegalArgumentException("Hash Partitioning requires keys");
		} else if(pMethod == PartitionMethod.RANGE) {
			throw new UnsupportedOperationException("Range Partitioning not yet supported");
		}
		
		if(pKeys instanceof Keys.ExpressionKeys<?> && !(input.getType() instanceof CompositeType) ) {
			throw new IllegalArgumentException("Hash Partitioning with key fields only possible on Composite-type DataSets");
		}
		
		this.pMethod = pMethod;
		this.pKeys = pKeys;
	}
	
	public PartitionOperator(DataSet<T> input, PartitionMethod pMethod) {
		this(input, pMethod, null);
	}
	
	/*
	 * Translation of partitioning
	 */
	protected org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?> translateToDataFlow(Operator<T> input) {
	
		String name = "Partition";
		
		// distinguish between partition types
		if (pMethod == PartitionMethod.REBALANCE) {
			
			UnaryOperatorInformation<T, T> operatorInfo = new UnaryOperatorInformation<T, T>(getType(), getType());
			PartitionOperatorBase<T> noop = new PartitionOperatorBase<T>(operatorInfo, pMethod, name);
			// set input
			noop.setInput(input);
			// set DOP
			noop.setDegreeOfParallelism(getParallelism());
			
			return noop;
		} 
		else if (pMethod == PartitionMethod.HASH) {
			
			if (pKeys instanceof Keys.ExpressionKeys) {
				
				int[] logicalKeyPositions = pKeys.computeLogicalKeyPositions();
				UnaryOperatorInformation<T, T> operatorInfo = new UnaryOperatorInformation<T, T>(getType(), getType());
				PartitionOperatorBase<T> noop = new PartitionOperatorBase<T>(operatorInfo, pMethod, logicalKeyPositions, name);
				// set input
				noop.setInput(input);
				// set DOP
				noop.setDegreeOfParallelism(getParallelism());
				
				return noop;
			} else if (pKeys instanceof Keys.SelectorFunctionKeys) {
				
				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<T, ?> selectorKeys = (Keys.SelectorFunctionKeys<T, ?>) pKeys;
				MapOperatorBase<?, T, ?> po = translateSelectorFunctionReducer(selectorKeys, pMethod, getType(), name, input, getParallelism());
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
