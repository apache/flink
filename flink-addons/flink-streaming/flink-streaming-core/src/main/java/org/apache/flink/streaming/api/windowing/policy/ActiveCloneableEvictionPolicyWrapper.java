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
 * The {@link ActiveEvictionPolicy} wraps around a non active
 * {@link EvictionPolicy}. It forwards all calls to
 * {@link ActiveEvictionPolicy#notifyEvictionWithFakeElement(Object, int)} to
 * {@link EvictionPolicy#notifyEviction(Object, boolean, int)} while the
 * triggered parameter will be set to true.
 * 
 * This class additionally implements the clone method and can wrap around
 * {@link CloneableEvictionPolicy} to make it active.
 * 
 * @param <DATA>
 *            The data type handled by this policy
 */
public class ActiveCloneableEvictionPolicyWrapper<DATA> extends ActiveEvictionPolicyWrapper<DATA>
		implements CloneableEvictionPolicy<DATA> {

	/**
	 * Auto generated version ID
	 */
	private static final long serialVersionUID = 1520261575300622769L;
	CloneableEvictionPolicy<DATA> nestedPolicy;

	/**
	 * Creates a wrapper which activates the eviction policy which is wrapped
	 * in. This means that the nested policy will get called on fake elements as
	 * well as on real elements.
	 * 
	 * This specialized version of the {@link ActiveEvictionPolicyWrapper} works
	 * with {@link CloneableEvictionPolicy} and is thereby cloneable as well.
	 * 
	 * @param nestedPolicy
	 *            The policy which should be activated/wrapped in.
	 */
	public ActiveCloneableEvictionPolicyWrapper(CloneableEvictionPolicy<DATA> nestedPolicy) {
		super(nestedPolicy);
		this.nestedPolicy = nestedPolicy;
	}

	@Override
	public ActiveCloneableEvictionPolicyWrapper<DATA> clone() {
		return new ActiveCloneableEvictionPolicyWrapper<DATA>(nestedPolicy.clone());
	}
}
