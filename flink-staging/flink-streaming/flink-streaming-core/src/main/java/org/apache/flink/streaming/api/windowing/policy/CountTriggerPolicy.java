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
 * This policy triggers at every n'th element.
 * 
 * @param <IN>
 *            The type of the data points which are handled by this policy
 */
public class CountTriggerPolicy<IN> implements CloneableTriggerPolicy<IN> {

	/**
	 * Auto generated version ID
	 */
	private static final long serialVersionUID = -6357200688886103968L;

	public static final int DEFAULT_START_VALUE = 0;

	private int counter;
	private int max;
	private int startValue;

	/**
	 * This constructor will set up a count based trigger, which triggers after
	 * max elements have arrived.
	 * 
	 * @param max
	 *            The number of arriving elements before the trigger occurs.
	 */
	public CountTriggerPolicy(int max) {
		this(max, DEFAULT_START_VALUE);
	}

	/**
	 * In addition to {@link CountTriggerPolicy#CountTriggerPolicy(int)} this
	 * constructor allows to set a custom start value for the element counter.
	 * This can be used to delay the first trigger by setting a negative start
	 * value. Often the first trigger should be delayed in case of sliding
	 * windows. For example if the size of a window should be 4 and a trigger
	 * should happen every 2, a start value of -2 would allow to also have the
	 * first window of size 4.
	 * 
	 * @param max
	 *            The number of arriving elements before the trigger occurs.
	 * @param startValue
	 *            The start value for the counter of arriving elements.
	 * @see CountTriggerPolicy#CountTriggerPolicy(int)
	 */
	public CountTriggerPolicy(int max, int startValue) {
		this.max = max;
		this.counter = startValue;
		this.startValue = startValue;
	}

	@Override
	public boolean notifyTrigger(IN datapoint) {
		// The comparison have to be >= and not == to cover case max=0
		if (counter >= max) {
			// The current data point will be part of the next window!
			// Therefore the counter needs to be set to one already.
			counter = 1;
			return true;
		} else {
			counter++;
			return false;
		}
	}

	@Override
	public CountTriggerPolicy<IN> clone() {
		return new CountTriggerPolicy<IN>(max, counter);
	}

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof CountTriggerPolicy)) {
			return false;
		} else {
			try {
				@SuppressWarnings("unchecked")
				CountTriggerPolicy<IN> otherPolicy = (CountTriggerPolicy<IN>) other;
				return max == otherPolicy.max && startValue == otherPolicy.startValue;
			} catch (ClassCastException e) {
				return false;
			}
		}
	}

	public int getSlideSize() {
		return max;
	}
	
	public int getStart() {
		return startValue;
	}

	@Override
	public String toString() {
		return "CountPolicy(" + max + ")";
	}
}
