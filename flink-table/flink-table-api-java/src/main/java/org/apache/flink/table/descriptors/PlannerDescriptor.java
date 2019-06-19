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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Common class that defines necessary properties to choose and create both
 * {@link Executor} and {@link Planner}.
 */
public class PlannerDescriptor {
	public static final String BATCH_MODE = "batch-mode";
	public static final String CLASS_NAME = "class-name";

	private final DescriptorProperties commonProperties = new DescriptorProperties();
	private final String plannerClass;
	private final String executorClass;

	@VisibleForTesting
	private PlannerDescriptor(@Nullable String plannerClass, @Nullable String executorClass) {
		this.plannerClass = plannerClass;
		this.executorClass = executorClass;
	}

	/**
	 * Sets the legacy planner as the required module.
	 */
	public static PlannerDescriptor legacy() {
		return new PlannerDescriptor(
			"org.apache.flink.table.planner.StreamPlannerFactory",
			"org.apache.flink.table.executor.StreamExecutorFactory");
	}

	/**
	 * Sets the blink planner as the required module.
	 */
	public static PlannerDescriptor blink() {
		throw new UnsupportedOperationException("Blink planner is not supported yet.");
	}

	/**
	 * Does not specify the required planner. It will use the components, if those are the only components available on
	 * the classpath.
	 */
	public static PlannerDescriptor any() {
		return new PlannerDescriptor(null, null);
	}

	/**
	 * Sets that the components should work in a batch mode.
	 */
	public PlannerDescriptor batch() {
		commonProperties.putBoolean(BATCH_MODE, true);
		return this;
	}

	/**
	 * Sets that the components should work in a stream mode.
	 */
	public PlannerDescriptor stream() {
		commonProperties.putBoolean(BATCH_MODE, false);
		return this;
	}

	public Map<String, String> toPlannerProperties() {
		Map<String, String> properties = commonProperties.asMap();
		if (plannerClass != null) {
			properties.put(CLASS_NAME, plannerClass);
		}
		return properties;
	}

	public Map<String, String> toExecutorProperties() {
		Map<String, String> properties = commonProperties.asMap();
		if (executorClass != null) {
			properties.put(CLASS_NAME, executorClass);
		}
		return properties;
	}
}
