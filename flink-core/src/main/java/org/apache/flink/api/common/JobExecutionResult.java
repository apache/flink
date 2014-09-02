/**
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


package org.apache.flink.api.common;

import java.util.Map;

public class JobExecutionResult {
	
	private long netRuntime;
	private Map<String, Object> accumulatorResults;
	
	public JobExecutionResult(long netRuntime, Map<String, Object> accumulators) {
		this.netRuntime = netRuntime;
		this.accumulatorResults = accumulators;
	}
	
	public long getNetRuntime() {
		return this.netRuntime;
	}

	@SuppressWarnings("unchecked")
	public <T> T getAccumulatorResult(String accumulatorName) {
		return (T) this.accumulatorResults.get(accumulatorName);
	}

	public Map<String, Object> getAllAccumulatorResults() {
		return this.accumulatorResults;
	}
	
	/**
	 * @param accumulatorName
	 *            Name of the counter
	 * @return Result of the counter, or null if the counter does not exist
	 */
	public Integer getIntCounterResult(String accumulatorName) {
		Object result = this.accumulatorResults.get(accumulatorName);
		if (result == null) {
			return null;
		}
		if (!(result instanceof Integer)) {
			throw new ClassCastException("Requested result of the accumulator '" + accumulatorName
							+ "' should be Integer but has type " + result.getClass());
		}
		return (Integer) result;
	}

	// TODO Create convenience methods for the other shipped accumulator types

}
