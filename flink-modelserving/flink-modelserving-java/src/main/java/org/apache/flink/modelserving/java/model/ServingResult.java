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

package org.apache.flink.modelserving.java.model;

/**
 * Representation of the model serving result.
 */
public class ServingResult<RESULT> {

	// Serving duration
	private long duration;
	// Serving result
	private RESULT result;

	/**
	 * Create Serving result.
	 *
	 * @param duration Execution time.
	 * @param result Execution result.
	 */
	public ServingResult(long duration, RESULT result){
		this.duration = duration;
		this.result = result;
	}

	/**
	 * Get execution duration.
	 *
	 * @return execution duration.
	 */
	public long getDuration() {
		return duration;
	}

	/**
	 * Get execution result.
	 *
	 * @return execution result.
	 */
	public RESULT getResult() {
		return result;
	}

	/**
	 * Get serving result as a String.
	 *
	 * @return serving result as a String.
	 */
	@Override
	public String toString() {
		return "Model serving in " + duration + "ms with result " + result;
	}
}
