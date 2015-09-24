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
package org.apache.flink.contrib.operatorstatistics;

import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * This accumulator wraps the class {@link OperatorStatistics} to track
 * estimated values for count distinct and heavy hitters.
 *
 */
public class OperatorStatisticsAccumulator implements Accumulator<Object, OperatorStatistics> {

	private OperatorStatistics local;

	public OperatorStatisticsAccumulator(){
		local = new OperatorStatistics(new OperatorStatisticsConfig());
	}

	public OperatorStatisticsAccumulator(OperatorStatisticsConfig config){
		local = new OperatorStatistics(config);
	}

	@Override
	public void add(Object value) {
		local.process(value);
	}

	@Override
	public OperatorStatistics getLocalValue() {
		return local;
	}

	@Override
	public void resetLocal() {
		local = new OperatorStatistics(new OperatorStatisticsConfig());
	}

	@Override
	public void merge(Accumulator<Object, OperatorStatistics> other) {
		local.merge(other.getLocalValue());
	}

	@Override
	public Accumulator<Object, OperatorStatistics> clone() {
		OperatorStatisticsAccumulator clone = new OperatorStatisticsAccumulator();
		clone.local = this.local.clone();
		return clone;
	}

}
