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
package eu.stratosphere.api.common.aggregators;

import eu.stratosphere.types.LongValue;

/**
 * A {@link ConvergenceCriterion} over an {@link Aggregator} that defines convergence as reached once the aggregator
 * holds the value zero. The aggregated data type is a {@link LongValue}.
 */
public class LongZeroConvergence implements ConvergenceCriterion<LongValue> {

	/**
	 * Returns true, if the aggregator value is zero, false otherwise.
	 * 
	 * @param iteration The number of the iteration superstep. Ignored in this case.
	 * @param value The aggregator value, which is compared to zero.
	 * @return True, if the aggregator value is zero, false otherwise.
	 */
	@Override
	public boolean isConverged(int iteration, LongValue value) {
		return value.getValue() == 0;
	}
}
