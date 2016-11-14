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
 * A {@link ConvergenceCriterion} over an {@link Aggregator} that defines convergence as reached once the aggregator
 * holds the value zero. The aggregated data type is a {@link DoubleValue}.
 */
@SuppressWarnings("serial")
@PublicEvolving
public class DoubleZeroConvergence implements ConvergenceCriterion<DoubleValue> {

	/**
	 * Returns true, if the aggregator value is zero, false otherwise.
	 * 
	 * @param iteration The number of the iteration superstep. Ignored in this case.
	 * @param value The aggregator value, which is compared to zero.
	 * @return True, if the aggregator value is zero, false otherwise.
	 */
	@Override
	public boolean isConverged(int iteration, DoubleValue value) {
		return value.getValue() == 0;
	}
}
