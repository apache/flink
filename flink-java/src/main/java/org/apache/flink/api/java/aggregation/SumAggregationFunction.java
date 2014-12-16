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

/**
 * Sum aggregation function.
 * 
 * <p><b>Note: The input type must be set using {@link setInputType}
 * before this aggregation function can be used.</b>
 * 
 * @param <T> The input and output type. Must extend {@link Number}.
 */
public class SumAggregationFunction<T extends Number> extends InputTypeAggregationFunction<T> implements Serializable {
	private static final long serialVersionUID = -4610234679407339317L;
	
	private DelegatedSumAggregationFunction<T> delegate;

	public SumAggregationFunction(int field) {
		super("sum", field);
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

	private static interface DelegatedSumAggregationFunction<T extends Number> {
		public T reduce(T lhs, T rhs);
	}
	
	private static class LongSumAggregationFunction implements DelegatedSumAggregationFunction<Long>, Serializable {
		private static final long serialVersionUID = 1047115816680232356L;
		
		@Override
		public Long reduce(Long lhs, Long rhs) {
			return lhs + rhs;
		}
	}

	private static class IntegerSumAggregationFunction implements DelegatedSumAggregationFunction<Integer>, Serializable {
		private static final long serialVersionUID = 8585263516508258383L;

		@Override
		public Integer reduce(Integer lhs, Integer rhs) {
			return lhs + rhs;
		}
	}

	private static class DoubleSumAggregationFunction implements DelegatedSumAggregationFunction<Double>, Serializable {
		private static final long serialVersionUID = 7694204706135707202L;

		@Override
		public Double reduce(Double lhs, Double rhs) {
			return lhs + rhs;
		}
	}

	private static class FloatSumAggregationFunction implements DelegatedSumAggregationFunction<Float>, Serializable {
		private static final long serialVersionUID = -556020226468154664L;

		@Override
		public Float reduce(Float lhs, Float rhs) {
			return lhs + rhs;
		}
	}

	private static class ByteSumAggregationFunction implements DelegatedSumAggregationFunction<Byte>, Serializable {
		private static final long serialVersionUID = 1796225860081673684L;

		@Override
		public Byte reduce(Byte lhs, Byte rhs) {
			return (byte) (lhs + rhs);
		}
	}

	private static class ShortSumAggregationFunction implements DelegatedSumAggregationFunction<Short>, Serializable {
		private static final long serialVersionUID = 4132230028758671602L;

		@Override
		public Short reduce(Short lhs, Short rhs) {
			return (short) (lhs + rhs);
		}
	}

	@Override
	public T reduce(T value1, T value2) {
		T result = delegate.reduce(value1, value2);
		return result;
	}
}
