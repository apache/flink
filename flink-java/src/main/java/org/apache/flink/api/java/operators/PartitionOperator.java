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

import com.google.common.base.Preconditions;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase.PartitionMethod;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.Keys.SelectorFunctionKeys;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * This operator represents a partitioning.
 *
 * @param <T> The type of the data being partitioned.
 */
public class PartitionOperator<T> extends SingleInputOperator<T, T, PartitionOperator<T>> {
	
	private final Keys<T> pKeys;
	private final PartitionMethod pMethod;
	private final String partitionLocationName;
	private final Partitioner<?> customPartitioner;
	
	
	public PartitionOperator(DataSet<T> input, PartitionMethod pMethod, Keys<T> pKeys, String partitionLocationName) {
		this(input, pMethod, pKeys, null, null, partitionLocationName);
	}
	
	public PartitionOperator(DataSet<T> input, PartitionMethod pMethod, String partitionLocationName) {
		this(input, pMethod, null, null, null, partitionLocationName);
	}
	
	public PartitionOperator(DataSet<T> input, Keys<T> pKeys, Partitioner<?> customPartitioner, String partitionLocationName) {
		this(input, PartitionMethod.CUSTOM, pKeys, customPartitioner, null, partitionLocationName);
	}
	
	public <P> PartitionOperator(DataSet<T> input, Keys<T> pKeys, Partitioner<P> customPartitioner, 
			TypeInformation<P> partitionerTypeInfo, String partitionLocationName)
	{
		this(input, PartitionMethod.CUSTOM, pKeys, customPartitioner, partitionerTypeInfo, partitionLocationName);
	}
	
	private <P> PartitionOperator(DataSet<T> input, PartitionMethod pMethod, Keys<T> pKeys, Partitioner<P> customPartitioner, 
			TypeInformation<P> partitionerTypeInfo, String partitionLocationName)
	{
		super(input, input.getType());
		
		Preconditions.checkNotNull(pMethod);
		Preconditions.checkArgument(pKeys != null || pMethod == PartitionMethod.REBALANCE, "Partitioning requires keys");
		Preconditions.checkArgument(pMethod != PartitionMethod.CUSTOM || customPartitioner != null, "Custom partioning requires a partitioner.");

		if (customPartitioner != null) {
			pKeys.validateCustomPartitioner(customPartitioner, partitionerTypeInfo);
		}
		
		this.pMethod = pMethod;
		this.pKeys = pKeys;
		this.partitionLocationName = partitionLocationName;
		this.customPartitioner = customPartitioner;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the custom partitioner from this partitioning.
	 * 
	 * @return The custom partitioner.
	 */
	public Partitioner<?> getCustomPartitioner() {
		return customPartitioner;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Translation
	// --------------------------------------------------------------------------------------------
	
	protected org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?> translateToDataFlow(Operator<T> input) {
	
		String name = "Partition at " + partitionLocationName;
		
		// distinguish between partition types
		if (pMethod == PartitionMethod.REBALANCE) {
			
			UnaryOperatorInformation<T, T> operatorInfo = new UnaryOperatorInformation<>(getType(), getType());
			PartitionOperatorBase<T> rebalancedInput = new PartitionOperatorBase<>(operatorInfo, pMethod, name);
			rebalancedInput.setInput(input);
			rebalancedInput.setParallelism(getParallelism());
			
			return rebalancedInput;
		} 
		else if (pMethod == PartitionMethod.HASH || pMethod == PartitionMethod.CUSTOM || pMethod == PartitionMethod.RANGE) {
			
			if (pKeys instanceof Keys.ExpressionKeys) {
				
				int[] logicalKeyPositions = pKeys.computeLogicalKeyPositions();
				UnaryOperatorInformation<T, T> operatorInfo = new UnaryOperatorInformation<>(getType(), getType());
				PartitionOperatorBase<T> partitionedInput = new PartitionOperatorBase<>(operatorInfo, pMethod, logicalKeyPositions, name);
				partitionedInput.setInput(input);
				partitionedInput.setParallelism(getParallelism());
				partitionedInput.setCustomPartitioner(customPartitioner);
				
				return partitionedInput;
			}
			else if (pKeys instanceof Keys.SelectorFunctionKeys) {
				
				@SuppressWarnings("unchecked")
				Keys.SelectorFunctionKeys<T, ?> selectorKeys = (Keys.SelectorFunctionKeys<T, ?>) pKeys;
				return translateSelectorFunctionPartitioner(selectorKeys, pMethod, name, input, getParallelism(), customPartitioner);
			}
			else {
				throw new UnsupportedOperationException("Unrecognized key type.");
			}
			
		} 
		else {
			throw new UnsupportedOperationException("Unsupported partitioning method: " + pMethod.name());
		}
	}

	@SuppressWarnings("unchecked")
	private static <T, K> org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?> translateSelectorFunctionPartitioner(
		SelectorFunctionKeys<T, ?> rawKeys,
		PartitionMethod pMethod,
		String name,
		Operator<T> input,
		int partitionDop,
		Partitioner<?> customPartitioner)
	{
		final SelectorFunctionKeys<T, K> keys = (SelectorFunctionKeys<T, K>) rawKeys;
		TypeInformation<Tuple2<K, T>> typeInfoWithKey = SelectorFunctionKeys.createTypeWithKey(keys);

		Operator<Tuple2<K, T>> keyedInput = SelectorFunctionKeys.appendKeyExtractor(input, keys);

		PartitionOperatorBase<Tuple2<K, T>> keyedPartitionedInput =
			new PartitionOperatorBase<>(new UnaryOperatorInformation<>(typeInfoWithKey, typeInfoWithKey), pMethod, new int[]{0}, name);
		keyedPartitionedInput.setInput(keyedInput);
		keyedPartitionedInput.setCustomPartitioner(customPartitioner);
		keyedPartitionedInput.setParallelism(partitionDop);

		return SelectorFunctionKeys.appendKeyRemover(keyedPartitionedInput, keys);
	}

	
}
