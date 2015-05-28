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
 * This eviction policy allows the eviction of data points from the buffer using
 * a counter of arriving elements and a threshold (maximal buffer size)
 * 
 * By default this policy does not react on fake elements. Wrap it in an
 * {@link ActiveEvictionPolicyWrapper} to make it count even fake elements.
 * 
 * @param <IN>
 *            the type of the incoming data points
 */
public class CountEvictionPolicy<IN> implements CloneableEvictionPolicy<IN> {

	/**
	 * Auto generated version id
	 */
	private static final long serialVersionUID = 2319201348806427996L;

	private int maxElements;
	private int counter;
	private int deleteOnEviction = 1;
	private int startValue;

	/**
	 * This constructor allows the setup of the simplest possible count based
	 * eviction. It keeps the size of the buffer according to the given
	 * maxElements parameter by deleting the oldest element in the buffer.
	 * Eviction only takes place if the counter of arriving elements would be
	 * higher than maxElements without eviction.
	 * 
	 * @param maxElements
	 *            The maximum number of elements before eviction. As soon as one
	 *            more element arrives, the oldest element will be deleted
	 */
	public CountEvictionPolicy(int maxElements) {
		this(maxElements, 1);
	}

	/**
	 * This constructor allows to set up both, the maximum number of elements
	 * and the number of elements to be deleted in case of an eviction.
	 * 
	 * Eviction only takes place if the counter of arriving elements would be
	 * higher than maxElements without eviction. In such a case deleteOnEviction
	 * elements will be removed from the buffer.
	 * 
	 * The counter of arriving elements is adjusted respectively, but never set
	 * below zero:
	 * counter=(counter-deleteOnEviction<0)?0:counter-deleteOnEviction
	 * 
	 * @param maxElements
	 *            maxElements The maximum number of elements before eviction.
	 * @param deleteOnEviction
	 *            The number of elements to be deleted on eviction. The counter
	 *            will be adjusted respectively but never below zero.
	 */
	public CountEvictionPolicy(int maxElements, int deleteOnEviction) {
		this(maxElements, deleteOnEviction, 0);
	}

	/**
	 * The same as {@link CountEvictionPolicy#CountEvictionPolicy(int, int)}.
	 * Additionally a custom start value for the counter of arriving elements
	 * can be set. By setting a negative start value the first eviction can be
	 * delayed.
	 * 
	 * @param maxElements
	 *            maxElements The maximum number of elements before eviction.
	 * @param deleteOnEviction
	 *            The number of elements to be deleted on eviction. The counter
	 *            will be adjusted respectively but never below zero.
	 * @param startValue
	 *            A custom start value for the counter of arriving elements.
	 * @see CountEvictionPolicy#CountEvictionPolicy(int, int)
	 */
	public CountEvictionPolicy(int maxElements, int deleteOnEviction, int startValue) {
		this.counter = startValue;
		this.deleteOnEviction = deleteOnEviction;
		this.maxElements = maxElements;
		this.startValue = startValue;
	}

	@Override
	public int notifyEviction(IN datapoint, boolean triggered, int bufferSize) {
		// The comparison have to be >= and not == to cover case max=0
		if (counter >= maxElements) {
			// Adjust the counter according to the current eviction
			counter = (counter - deleteOnEviction < 0) ? 0 : counter - deleteOnEviction;
			// The current element will be added after the eviction
			// Therefore, increase counter in any case
			counter++;
			return deleteOnEviction;
		} else {
			counter++;
			return 0;
		}
	}

	@Override
	public CountEvictionPolicy<IN> clone() {
		return new CountEvictionPolicy<IN>(maxElements, deleteOnEviction, counter);
	}

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof CountEvictionPolicy)) {
			return false;
		} else {
			try {
				@SuppressWarnings("unchecked")
				CountEvictionPolicy<IN> otherPolicy = (CountEvictionPolicy<IN>) other;
				return startValue == otherPolicy.startValue
						&& deleteOnEviction == otherPolicy.deleteOnEviction
						&& maxElements == otherPolicy.maxElements;
			} catch (ClassCastException e) {
				return false;
			}
		}
	}

	public int getWindowSize() {
		return maxElements;
	}

	public int getStart() {
		return startValue;
	}
	
	public int getDeleteOnEviction(){
		return deleteOnEviction;
	}

	@Override
	public String toString() {
		return "CountPolicy(" + maxElements + ")";
	}
}
