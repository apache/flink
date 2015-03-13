/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.policy;

/**
 * This eviction policy deletes all elements from the buffer in case a trigger
 * occurred. Therefore, it is the default eviction policy to be used for any
 * tumbling window.
 * 
 * By default this policy does not react on fake elements. Wrap it in an
 * {@link ActiveEvictionPolicyWrapper} to make it clearing the buffer even on
 * fake elements.
 * 
 * @param <DATA>
 *            The type of the data points which is handled by this policy
 */
public class TumblingEvictionPolicy<DATA> implements CloneableEvictionPolicy<DATA> {

	/**
	 * Auto generated version ID
	 */
	private static final long serialVersionUID = -4018019069267281155L;

	/**
	 * Counter for the current number of elements in the buffer
	 */
	private int counter = 0;

	/**
	 * This is the default constructor providing no special functionality. This
	 * eviction policy deletes all elements from the buffer in case a trigger
	 * occurred. Therefore, it is the default eviction policy to be used for any
	 * tumbling window.
	 */
	public TumblingEvictionPolicy() {
		// default constructor, no further logic needed
	}

	/**
	 * This constructor allows to set a custom start value for the element
	 * counter.
	 * 
	 * This eviction policy deletes all elements from the buffer in case a
	 * trigger occurred. Therefore, it is the default eviction policy to be used
	 * for any tumbling window.
	 * 
	 * @param startValue
	 *            A start value for the element counter
	 */
	public TumblingEvictionPolicy(int startValue) {
		this.counter = startValue;
	}

	/**
	 * Deletes all elements from the buffer in case the trigger occurred.
	 */
	@Override
	public int notifyEviction(Object datapoint, boolean triggered, int bufferSize) {
		if (triggered) {
			// The current data point will be part of the next window!
			// Therefore the counter needs to be set to one already.
			int tmpCounter = counter;
			counter = 1;
			return tmpCounter;
		} else {
			counter++;
			return 0;
		}
	}

	@Override
	public TumblingEvictionPolicy<DATA> clone() {
		return new TumblingEvictionPolicy<DATA>(counter);
	}

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof TumblingEvictionPolicy)) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public String toString() {
		return "TumblingPolicy";
	}
}
