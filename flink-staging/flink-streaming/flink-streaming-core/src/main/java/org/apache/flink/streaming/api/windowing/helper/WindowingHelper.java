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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

/**
 * A helper representing a count or eviction policy. Such helper classes are
 * used to provide a nice and well readable API.
 * 
 * @param <DATA>
 *            the type of input data handled by this helper
 * @see Count
 * @see Time
 * @see Delta
 */
public abstract class WindowingHelper<DATA> {

	/**
	 * Provides information for initial value serialization
	 * in {@link Delta}, unused in other subclasses.
	 */
	protected ExecutionConfig executionConfig;

	/**
	 * Method for encapsulating the {@link EvictionPolicy}.
	 * @return the eviction policy
	 */
	public abstract EvictionPolicy<DATA> toEvict();

	/**
	 * Method for encapsulating the {@link TriggerPolicy}.
	 * @return the trigger policy
	 */
	public abstract TriggerPolicy<DATA> toTrigger();

	/**
	 * Setter for the {@link ExecutionConfig} field.
	 * @param executionConfig Desired value
	 */
	public final void setExecutionConfig(ExecutionConfig executionConfig){
		this.executionConfig = executionConfig;
	}
}
