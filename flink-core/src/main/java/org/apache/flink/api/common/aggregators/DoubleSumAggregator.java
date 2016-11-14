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

package org.apache.flink.api.common.aggregators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.DoubleValue;


/**
 * An {@link Aggregator} that sums up {@link DoubleValue} values.
 */
@SuppressWarnings("serial")
@PublicEvolving
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
