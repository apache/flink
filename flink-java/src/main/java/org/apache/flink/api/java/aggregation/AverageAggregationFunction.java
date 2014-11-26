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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;

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
public class AverageAggregationFunction<T extends Number> extends CompositeAggregationFunction<T, Double> {
	private static final long serialVersionUID = -3901931046368002202L;

	private SumAggregationFunction<T> sumDelegate;
	private CountAggregationFunction countDelegate;
	
	public AverageAggregationFunction(int field) {
		this(field, new SumAggregationFunction<T>(field), new CountAggregationFunction());
	}
	
	AverageAggregationFunction(int field, SumAggregationFunction<T> sumDelegate, CountAggregationFunction countDelegate) {
		super("average", field);
		this.sumDelegate = sumDelegate;
		this.countDelegate = countDelegate; 
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
	public Double initialize(T value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double reduce(Double value1, Double value2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double computeComposite(Tuple tuple) {
		int sumIntermediatePosition = sumDelegate.getIntermediatePosition();
		int countIntermediatePosition = countDelegate.getIntermediatePosition();
		@SuppressWarnings("unchecked")
		T sum = (T) tuple.getField(sumIntermediatePosition);
		Long count = (Long) tuple.getField(countIntermediatePosition);
		Double result = sum.doubleValue() / count.doubleValue();
		return result;
	}

	@Override
	public List<AggregationFunction<?, ?>> getIntermediateAggregationFunctions() {
		List<AggregationFunction<?, ?>> intermediates = new ArrayList<AggregationFunction<?,?>>();
		intermediates.add(sumDelegate);
		intermediates.add(countDelegate);
		return intermediates;
	}

}
