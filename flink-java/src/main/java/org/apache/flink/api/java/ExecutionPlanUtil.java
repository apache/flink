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
 *
 */

package org.apache.flink.api.java;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.Plan;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility for extracting an execution plan (as JSON) from a {@link Plan}.
 */
@Internal
public class ExecutionPlanUtil {

	private static final String PLAN_GENERATOR_CLASS_NAME = "org.apache.flink.optimizer.plandump.ExecutionPlanJSONGenerator";

	/**
	 * Extracts the execution plan (as JSON) from the given {@link Plan}.
	 */
	public static String getExecutionPlanAsJSON(Plan plan) {
		checkNotNull(plan);
		ExecutionPlanJSONGenerator jsonGenerator = getJSONGenerator();
		return jsonGenerator.getExecutionPlan(plan);
	}

	private static ExecutionPlanJSONGenerator getJSONGenerator() {
		Class<? extends ExecutionPlanJSONGenerator> planGeneratorClass = loadJSONGeneratorClass(
				PLAN_GENERATOR_CLASS_NAME);

		try {
			return planGeneratorClass.getConstructor().newInstance();
		} catch (Throwable t) {
			throw new RuntimeException("An error occurred while loading the plan generator ("
					+ PLAN_GENERATOR_CLASS_NAME + ").", t);
		}
	}

	private static Class<? extends ExecutionPlanJSONGenerator> loadJSONGeneratorClass(String className) {
		try {
			Class<?> generatorClass = Class.forName(className);
			return generatorClass.asSubclass(ExecutionPlanJSONGenerator.class);
		} catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("Could not load the plan generator class (" + className
					+ "). Do you have the 'flink-optimizer' project in your dependencies?");
		} catch (Throwable t) {
			throw new RuntimeException(
					"An error occurred while loading the plan generator (" + className + ").",
					t);
		}
	}

	/**
	 * Internal interface for the JSON plan generator that has to reside in the optimizer package.
	 * We load the actual subclass using reflection.
	 */
	@Internal
	public interface ExecutionPlanJSONGenerator {

		/**
		 * Returns the execution plan as a JSON string.
		 */
		String getExecutionPlan(Plan plan);
	}
}
