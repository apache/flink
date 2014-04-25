/***********************************************************************************************************************
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
 **********************************************************************************************************************/
package eu.stratosphere.api.common.aggregators;

import eu.stratosphere.types.DoubleValue;


/**
 * An {@link Aggregator} that sums up {@link DoubleValue} values.
 */
public class DoubleSumAggregator implements Aggregator<DoubleValue> {

	private DoubleValue wrapper = new DoubleValue();
	private double sum;
	
	@Override
	public DoubleValue getAggregate() {
		wrapper.setValue(sum);
		return wrapper;
	}

	@Override
	public void aggregate(DoubleValue element) {
		sum += element.getValue();
	}

	/**
	 * Adds the given value to the current aggregate.
	 * 
	 * @param value The value to add to the aggregate.
	 */
	public void aggregate(double value) {
		sum += value;
	}
	
	@Override
	public void reset() {
		sum = 0;
	}
}
