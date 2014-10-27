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

package org.apache.flink.streaming.api.windowing.helper;

import org.apache.flink.streaming.api.windowing.policy.CountEvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.CountTriggerPolicy;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

/**
 * Represents a count based trigger or eviction policy.
 * Use the {@link Count#of(int)} to get an instance.
 */
@SuppressWarnings("rawtypes")
public class Count implements WindowingHelper {

	private int count;

	/**
	 * Specifies on which element a trigger or an eviction should happen (based
	 * on the count of the elements).
	 * 
	 * This constructor does exactly the same as {@link Count#of(int)}. 
	 * 
	 * @param count the number of elements to count before trigger/evict
	 */
	public Count(int count) {
		this.count = count;
	}

	@Override
	public EvictionPolicy<?> toEvict() {
		return new CountEvictionPolicy(count);
	}

	@Override
	public TriggerPolicy<?> toTrigger() {
		return new CountTriggerPolicy(count);
	}

	/**
	 * Specifies on which element a trigger or an eviction should happen (based
	 * on the count of the elements)
	 * 
	 * @param count
	 *            the number of elements to count before trigger/evict
	 * @return An helper representing the policy
	 */
	public static Count of(int count) {
		return new Count(count);
	}

}
