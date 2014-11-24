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
 * Average aggregation function.
 * 
 * <p><b>Note: The input type must be set using {@link setInputType}
 * before this aggregation function can be used.</b>
 * 
 * <p>Internally, the computation is delegated to
 * {@link SumAggregationFunction} and {@link CountAggregationFunction}.
 * 
 * @param <T> The input and output type. Must extend {@link Number}.
 */
public class AverageAggregationFunction<T extends Number> extends FieldAggregationFunction<T, Double> {
	private static final long serialVersionUID = -3901931046368002202L;

	private SumAggregationFunction<T> sumDelegate;
	private CountAggregationFunction countDelegate;
	
	public AverageAggregationFunction(int field) {
		super("average", field);
		sumDelegate = new SumAggregationFunction<T>();
		countDelegate = new CountAggregationFunction();
	}
	
	@Override
	public ResultTypeBehavior getResultTypeBehavior() {
		return ResultTypeBehavior.FIXED;
	}

	@Override
	public BasicTypeInfo<Double> getResultType() {
		return BasicTypeInfo.DOUBLE_TYPE_INFO;
	}

	@Override
	public void setInputType(BasicTypeInfo<T> inputType) {
		sumDelegate.setInputType(inputType);
	}

	@Override
	public void initialize() {
		sumDelegate.initialize();
		countDelegate.initialize();
	}

	@Override
	public void aggregate(T value) {
		sumDelegate.aggregate(value);
		countDelegate.aggregate(value);
	}

	@Override
	public Double getAggregate() {
		double sum = sumDelegate.getAggregate().doubleValue();
		double count = countDelegate.getAggregate().doubleValue();
		return sum / count;
	}

}
