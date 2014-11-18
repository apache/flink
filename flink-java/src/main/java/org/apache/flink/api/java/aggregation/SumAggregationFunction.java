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

import java.io.Serializable;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;


public class SumAggregationFunction<T extends Number> extends InputTypeAggregationFunction<T> implements Serializable {
	private static final long serialVersionUID = -4610234679407339317L;
	
	private DelegatedSumAggregationFunction<T> delegate;

	public SumAggregationFunction() {
		this(-1);
	}
	
	public SumAggregationFunction(int field) {
		super(field);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void setInputType(BasicTypeInfo<T> inputType) {
		super.setInputType(inputType);
		if (inputType.getTypeClass() == Long.class) {
			delegate = (DelegatedSumAggregationFunction<T>) new LongSumAggregationFunction();
		} else if (inputType.getTypeClass() == Integer.class) {
			delegate = (DelegatedSumAggregationFunction<T>) new IntegerSumAggregationFunction();
		} else if (inputType.getTypeClass() == Double.class) {
			delegate = (DelegatedSumAggregationFunction<T>) new DoubleSumAggregationFunction();
		} else if (inputType.getTypeClass() == Float.class) {
			delegate = (DelegatedSumAggregationFunction<T>) new FloatSumAggregationFunction();
		} else if (inputType.getTypeClass() == Short.class) {
			delegate = (DelegatedSumAggregationFunction<T>) new ShortSumAggregationFunction();
		} else if (inputType.getTypeClass() == Byte.class) {
			delegate = (DelegatedSumAggregationFunction<T>) new ByteSumAggregationFunction();
		} else {
			throw new IllegalArgumentException("Unknown input type for sum aggregation function.");
		}
	}

	@Override
	public void initialize() {
		delegate.initialize();
	}

	@Override
	public void aggregate(T value) {
		delegate.aggregate(value);
	}

	@Override
	public T getAggregate() {
		return delegate.getAggregate();
	}
	
	private static interface DelegatedSumAggregationFunction<T extends Number> {
		public void initialize();
		public void aggregate(T value);
		public T getAggregate();
	}
	
	private static class LongSumAggregationFunction implements DelegatedSumAggregationFunction<Long>, Serializable {
		private static final long serialVersionUID = 1047115816680232356L;
		
		private long sum;
		
		@Override
		public void initialize() {
			sum = 0L;
		}
		
		@Override
		public void aggregate(Long value) {
			sum += value;
		}

		@Override
		public Long getAggregate() {
			return sum;
		}
	}

	private static class IntegerSumAggregationFunction implements DelegatedSumAggregationFunction<Integer>, Serializable {
		private static final long serialVersionUID = 8585263516508258383L;

		private int sum;
		
		@Override
		public void initialize() {
			sum = 0;
		}
		
		@Override
		public void aggregate(Integer value) {
			sum += value;
		}

		@Override
		public Integer getAggregate() {
			return sum;
		}
	}

	private static class DoubleSumAggregationFunction implements DelegatedSumAggregationFunction<Double>, Serializable {
		private static final long serialVersionUID = 7694204706135707202L;

		private double sum;
		
		@Override
		public void initialize() {
			sum = 0.0;
		}
				
		@Override
		public void aggregate(Double value) {
			sum += value;
		}

		@Override
		public Double getAggregate() {
			return sum;
		}
	}

	private static class FloatSumAggregationFunction implements DelegatedSumAggregationFunction<Float>, Serializable {
		private static final long serialVersionUID = -556020226468154664L;

		private float sum;
		
		@Override
		public void initialize() {
			sum = (float) 0.0;
		}
		
		@Override
		public void aggregate(Float value) {
			sum += value;
		}

		@Override
		public Float getAggregate() {
			return sum;
		}
	}

	private static class ByteSumAggregationFunction implements DelegatedSumAggregationFunction<Byte>, Serializable {
		private static final long serialVersionUID = 1796225860081673684L;

		private byte sum;
		
		@Override
		public void initialize() {
			sum = (byte) 0;
		}
		
		@Override
		public void aggregate(Byte value) {
			sum += value;
		}

		@Override
		public Byte getAggregate() {
			return sum;
		}
	}

	private static class ShortSumAggregationFunction implements DelegatedSumAggregationFunction<Short>, Serializable {
		private static final long serialVersionUID = 4132230028758671602L;

		private short sum;
		
		@Override
		public void initialize() {
			sum = (short) 0;
		}
		
		@Override
		public void aggregate(Short value) {
			sum += value;
		}

		@Override
		public Short getAggregate() {
			return sum;
		}
	}
}
