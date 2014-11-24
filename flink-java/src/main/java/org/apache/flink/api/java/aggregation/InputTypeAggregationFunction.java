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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

/**
 * Base class for an AggregationFunction that has the same type for the
 * aggregation results as the aggregated inputs.
 * 
 * @param <T> The input and output type.
 */
public abstract class InputTypeAggregationFunction<T>
		extends FieldAggregationFunction<T, T> {
	private static final long serialVersionUID = -3129049288957424646L;

	private Class<T> type = null;
	
	public InputTypeAggregationFunction(String name, int field) {
		super(name, field);
	}

	@Override
	public ResultTypeBehavior getResultTypeBehavior() {
		return ResultTypeBehavior.INPUT;
	}

	@Override
	public BasicTypeInfo<T> getResultType() {
		return BasicTypeInfo.getInfoFor(type);
	}

	@Override
	public void setInputType(BasicTypeInfo<T> inputType) {
		type = inputType.getTypeClass();
	}

	@Override
	public T initialize(T value) {
		return value;
	}

}