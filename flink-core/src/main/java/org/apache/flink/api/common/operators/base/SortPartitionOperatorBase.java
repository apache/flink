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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.NoOpFunction;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @param <IN> The input and result type.
 */
@Internal
public class SortPartitionOperatorBase<IN> extends SingleInputOperator<IN, IN, NoOpFunction> {

	private final Ordering partitionOrdering;


	public SortPartitionOperatorBase(UnaryOperatorInformation<IN, IN> operatorInfo, Ordering partitionOrdering, String name) {
		super(new UserCodeObjectWrapper<NoOpFunction>(new NoOpFunction()), operatorInfo, name);
		this.partitionOrdering = partitionOrdering;
	}

	public Ordering getPartitionOrdering() {
		return partitionOrdering;
	}

	@Override
	public SingleInputSemanticProperties getSemanticProperties() {
		return new SingleInputSemanticProperties.AllFieldsForwardedProperties();
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	protected List<IN> executeOnCollections(List<IN> inputData, RuntimeContext runtimeContext, ExecutionConfig executionConfig) {

		TypeInformation<IN> inputType = getInput().getOperatorInfo().getOutputType();

		int[] sortColumns = this.partitionOrdering.getFieldPositions();
		boolean[] sortOrderings = this.partitionOrdering.getFieldSortDirections();

		final TypeComparator<IN> sortComparator;
		if (inputType instanceof CompositeType) {
			sortComparator = ((CompositeType<IN>) inputType).createComparator(sortColumns, sortOrderings, 0, executionConfig);
		} else if (inputType instanceof AtomicType) {
			sortComparator = ((AtomicType) inputType).createComparator(sortOrderings[0], executionConfig);
		} else {
			throw new UnsupportedOperationException("Partition sorting does not support type "+inputType+" yet.");
		}

		Collections.sort(inputData, new Comparator<IN>() {
			@Override
			public int compare(IN o1, IN o2) {
				return sortComparator.compare(o1, o2);
			}
		});

		return inputData;
	}
}
