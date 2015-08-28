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

/**
 * This policy does the same as {@link MultiEvictionPolicy}. Additionally it is
 * cloneable and only cloneable policies can be passed to the constructor.
 * 
 * @param <DATA>
 *            The type of data handled by this policy
 */
public class CloneableMultiEvictionPolicy<DATA> extends MultiEvictionPolicy<DATA> implements
		CloneableEvictionPolicy<DATA> {

	/**
	 * Default version id.
	 */
	private static final long serialVersionUID = 1L;

	private CloneableEvictionPolicy<DATA>[] allPolicies;
	private EvictionStrategy strategy;

	/**
	 * This policy does the same as {@link MultiTriggerPolicy}. Additionally it
	 * is cloneable and only cloneable policies can be passed to the
	 * constructor.
	 * 
	 * When using this constructor the MAX strategy is used by default. You can
	 * select other strategies using
	 * {@link CloneableMultiEvictionPolicy#CloneableMultiEvictionPolicy(EvictionStrategy, CloneableEvictionPolicy...)}
	 * .
	 * 
	 * @param evictionPolicies
	 *            some cloneable policies to be tied together.
	 */
	public CloneableMultiEvictionPolicy(CloneableEvictionPolicy<DATA>... evictionPolicies) {
		this(EvictionStrategy.MAX, evictionPolicies);
	}

	/**
	 * This policy does the same as {@link MultiTriggerPolicy}. Additionally it
	 * is cloneable and only cloneable policies can be passed to the
	 * constructor.
	 * 
	 * @param strategy
	 *            the strategy to be used. See {@link MultiEvictionPolicy.EvictionStrategy} for a
	 *            list of possible options.
	 * @param evictionPolicies
	 *            some cloneable policies to be tied together.
	 */
	public CloneableMultiEvictionPolicy(EvictionStrategy strategy,
			CloneableEvictionPolicy<DATA>... evictionPolicies) {
		super(strategy, evictionPolicies);
		this.allPolicies = evictionPolicies;
		this.strategy = strategy;
	}

	@SuppressWarnings("unchecked")
	public CloneableEvictionPolicy<DATA> clone() {
		LinkedList<CloneableEvictionPolicy<DATA>> clonedPolicies = new LinkedList<CloneableEvictionPolicy<DATA>>();
		for (int i = 0; i < allPolicies.length; i++) {
			clonedPolicies.add(allPolicies[i].clone());
		}
		return new CloneableMultiEvictionPolicy<DATA>(strategy,
				clonedPolicies.toArray(new CloneableEvictionPolicy[allPolicies.length]));
	}
}
