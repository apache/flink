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
 * This policy does the same as {@link MultiTriggerPolicy}. Additionally it is
 * cloneable and only cloneable policies can be passed to the constructor.
 * 
 * @param <DATA>
 *            The type of data handled by this policy
 */
public class CloneableMultiTriggerPolicy<DATA> extends MultiTriggerPolicy<DATA> implements
		CloneableTriggerPolicy<DATA>, Cloneable {

	/**
	 * Default version id.
	 */
	private static final long serialVersionUID = 1L;

	private CloneableTriggerPolicy<DATA>[] allPolicies;

	/**
	 * This policy does the same as {@link MultiTriggerPolicy}. Additionally it
	 * is cloneable and only cloneable policies can be passed to the
	 * constructor.
	 * 
	 * @param policies
	 *            some cloneable policies to be tied together.
	 */
	public CloneableMultiTriggerPolicy(CloneableTriggerPolicy<DATA>... policies) {
		super(policies);
		this.allPolicies = policies;
	}

	@SuppressWarnings("unchecked")
	public CloneableTriggerPolicy<DATA> clone() {
		LinkedList<CloneableTriggerPolicy<DATA>> clonedPolicies = new LinkedList<CloneableTriggerPolicy<DATA>>();
		for (int i = 0; i < allPolicies.length; i++) {
			clonedPolicies.add(allPolicies[i].clone());
		}
		return new CloneableMultiTriggerPolicy<DATA>(
				clonedPolicies.toArray(new CloneableTriggerPolicy[allPolicies.length]));

	}

}
