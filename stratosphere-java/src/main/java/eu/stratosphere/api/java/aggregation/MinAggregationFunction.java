/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.aggregation;


public class MinAggregationFunction<T extends Comparable<T>> extends AggregationFunction<T> {
	private static final long serialVersionUID = 1L;

	private T value;

	@Override
	public void initializeAggregate() {
		value = null;
	}

	@Override
	public void aggregate(T val) {
		if (value != null) {
			int cmp = value.compareTo(val);
			value = (cmp < 0) ? value : val;
		} else {
			value = val;
		}
	}

	@Override
	public T getAggregate() {
		return value;
	}
	
	@Override
	public String toString() {
		return "MAX";
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class MinAggregationFunctionFactory implements AggregationFunctionFactory {
		private static final long serialVersionUID = 1L;
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public <T> AggregationFunction<T> createAggregationFunction(Class<T> type) {
			if (Comparable.class.isAssignableFrom(type)) {
				return (AggregationFunction<T>) new MinAggregationFunction();
			} else {
				throw new UnsupportedAggregationTypeException("The type " + type.getName() + 
					" is not supported for minimum aggregation. " +
					"Minimum aggregatable types must implement the Comparable interface.");
			}
		}
	}
}
