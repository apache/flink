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

package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;

/**
 * This option specifies the failover strategy, i.e. how the job computation recovers from task failures.
 */
@PublicEvolving
public enum FailoverStrategyType {

	/**
	 * Config name for the RestartAllStrategy.
	 * Restarts all tasks when a failure occurred
	 *
	 * */
	FULL_RESTART_STRATEGY("full"),

	/**
	 *  Config name for the RestartIndividualStrategy
	 *  Restarts only the failed task. Should only be used if all tasks are independent components
	 * */
	INDIVIDUAL_RESTART_STRATEGY("individual"),

	/**
	 * Config name for the RestartPipelinedRegionStrategy
	 * Restarts all tasks that could be affected by the task failure.
	 * */
	PIPELINED_REGION_RESTART_STRATEGY("region"),

	/**
	 * Faiolver strategy not defined, will use the default strategy of cluster configuration
	 * */
	None("none");

	private String value;

	FailoverStrategyType(final String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	@Override
	public String toString() {
		return this.getValue();
	}
}
