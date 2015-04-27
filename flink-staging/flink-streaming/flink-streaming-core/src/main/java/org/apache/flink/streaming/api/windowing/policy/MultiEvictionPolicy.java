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

import java.util.LinkedList;
import java.util.List;

/**
 * This policy provides the ability to use multiple eviction policies at the
 * same time. It allows to use both, active and not active evictions.
 * 
 * @param <DATA>
 *            The type of data-items handled by the policies
 */
public class MultiEvictionPolicy<DATA> implements ActiveEvictionPolicy<DATA> {

	/**
	 * Default version id.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * This enum provides the different options for the eviction strategy.
	 * 
	 * You can choose from the following:
	 * <ul>
	 * <li>MIN: The number of elements to evict will be the smallest one which
	 * is greater than 0 and was returned by any of the given policies. If all
	 * policies return 0, the result is 0.</li>
	 * <li>MAX: The number of elements to evict will be the greatest one which
	 * was returned by any of the given policies.</li>
	 * <li>SUM: The number of elements to evict will be the sum of all values
	 * returned by the nested eviction policies.</li>
	 * <li>PRIORITY: Depending on the order in which the policies have been
	 * passed to the constructor, the first return value greater than 0 will be
	 * the the number of elements to evict. If all policies return 0, the result
	 * is 0.</li>
	 * </ul>
	 */
	public enum EvictionStrategy {
		MIN, MAX, SUM, PRIORITY
	}

	private List<EvictionPolicy<DATA>> allEvictionPolicies;
	private List<ActiveEvictionPolicy<DATA>> activeEvictionPolicies;
	private EvictionStrategy selectedStrategy;

	/**
	 * This policy provides the ability to use multiple eviction policies at the
	 * same time. It allows to use both, active and not active evictions.
	 * 
	 * When using this constructor the MAX strategy is used by default. You can
	 * select other strategies using
	 * {@link MultiEvictionPolicy#MultiEvictionPolicy(EvictionStrategy, EvictionPolicy...)}
	 * .
	 * 
	 * @param evictionPolicies
	 *            Any active or not active eviction policies. Both types can be
	 *            used at the same time.
	 */
	public MultiEvictionPolicy(EvictionPolicy<DATA>... evictionPolicies) {
		this(EvictionStrategy.MAX, evictionPolicies);
	}

	/**
	 * This policy provides the ability to use multiple eviction policies at the
	 * same time. It allows to use both, active and not active evictions.
	 * 
	 * @param strategy
	 *            the strategy to be used. See {@link EvictionStrategy} for a
	 *            list of possible options.
	 * @param evictionPolicies
	 *            Any active or not active eviction policies. Both types can be
	 *            used at the same time.
	 */
	public MultiEvictionPolicy(EvictionStrategy strategy, EvictionPolicy<DATA>... evictionPolicies) {
		// initialize lists of policies
		this.allEvictionPolicies = new LinkedList<EvictionPolicy<DATA>>();
		this.activeEvictionPolicies = new LinkedList<ActiveEvictionPolicy<DATA>>();

		// iterate over policies and add them to the lists
		for (EvictionPolicy<DATA> ep : evictionPolicies) {
			this.allEvictionPolicies.add(ep);
			if (ep instanceof ActiveEvictionPolicy) {
				this.activeEvictionPolicies.add((ActiveEvictionPolicy<DATA>) ep);
			}
		}

		// Remember eviction strategy
		this.selectedStrategy = strategy;
	}

	@Override
	public int notifyEviction(DATA datapoint, boolean triggered, int bufferSize) {
		LinkedList<Integer> results = new LinkedList<Integer>();
		for (EvictionPolicy<DATA> policy : allEvictionPolicies) {
			results.add(policy.notifyEviction(datapoint, triggered, bufferSize));
		}
		return getNumToEvict(results);
	}

	@Override
	public int notifyEvictionWithFakeElement(Object datapoint, int bufferSize) {
		LinkedList<Integer> results = new LinkedList<Integer>();
		for (ActiveEvictionPolicy<DATA> policy : activeEvictionPolicies) {
			results.add(policy.notifyEvictionWithFakeElement(datapoint, bufferSize));
		}
		return getNumToEvict(results);
	}

	private int getNumToEvict(LinkedList<Integer> items) {
		int result;
		switch (selectedStrategy) {

		case MIN:
			result = Integer.MAX_VALUE;
			for (Integer item : items) {
				if (result > item) {
					result = item;
				}
			}
			return result;

		case MAX:
			result = 0;
			for (Integer item : items) {
				if (result < item) {
					result = item;
				}
			}
			return result;

		case SUM:
			result = 0;
			for (Integer item : items) {
				result += item;
			}
			return result;

		case PRIORITY:
			for (Integer item : items) {
				if (item > 0) {
					return item;
				}
			}
			return 0;

		default:
			// The following line should never be reached. Just for the
			// compiler.
			return 0;
		}

	}
}
