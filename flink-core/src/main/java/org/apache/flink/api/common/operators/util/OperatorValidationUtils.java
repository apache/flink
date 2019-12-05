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

package org.apache.flink.api.common.operators.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.util.Preconditions;

/**
 * Utils for checking operators' resource and parallelism settings.
 */
public class OperatorValidationUtils {

	private OperatorValidationUtils() {}

	public static void validateParallelism(int parallelism) {
		validateParallelism(parallelism, true);
	}

	public static void validateParallelism(int parallelism, boolean canBeParallel) {
		Preconditions.checkArgument(canBeParallel || parallelism == 1,
			"The parallelism of non parallel operator must be 1.");
		Preconditions.checkArgument(parallelism > 0 || parallelism == ExecutionConfig.PARALLELISM_DEFAULT,
			"The parallelism of an operator must be at least 1, or ExecutionConfig.PARALLELISM_DEFAULT (use system default).");
	}

	public static void validateMaxParallelism(int maxParallelism) {
		validateMaxParallelism(maxParallelism, Integer.MAX_VALUE, true);
	}

	public static void validateMaxParallelism(int maxParallelism, int upperBound) {
		validateMaxParallelism(maxParallelism, upperBound, true);
	}

	public static void validateMaxParallelism(int maxParallelism, boolean canBeParallel) {
		validateMaxParallelism(maxParallelism, Integer.MAX_VALUE, canBeParallel);
	}

	public static void validateMaxParallelism(int maxParallelism, int upperBound, boolean canBeParallel) {
		Preconditions.checkArgument(maxParallelism > 0,
			"The maximum parallelism must be greater than 0.");
		Preconditions.checkArgument(canBeParallel || maxParallelism == 1,
			"The maximum parallelism of non parallel operator must be 1.");
		Preconditions.checkArgument(maxParallelism > 0 && maxParallelism <= upperBound,
			"Maximum parallelism must be between 1 and " + upperBound + ". Found: " + maxParallelism);
	}

	public static void validateResources(ResourceSpec resources) {
		Preconditions.checkNotNull(resources, "The resources must be not null.");
	}

	public static void validateMinAndPreferredResources(ResourceSpec minResources, ResourceSpec preferredResources) {
		Preconditions.checkNotNull(minResources, "The min resources must be not null.");
		Preconditions.checkNotNull(preferredResources, "The preferred resources must be not null.");
		Preconditions.checkArgument(minResources.lessThanOrEqual(preferredResources),
			"The resources must be either both UNKNOWN or both not UNKNOWN. If not UNKNOWN,"
				+ " the preferred resources must be greater than or equal to the min resources.");
	}

	public static void validateResourceRequirements(
			final ResourceSpec minResources,
			final ResourceSpec preferredResources,
			final int managedMemoryWeight) {

		validateMinAndPreferredResources(minResources, preferredResources);
		Preconditions.checkArgument(
			managedMemoryWeight >= 0,
			"managed memory weight must be no less than zero, was: " + managedMemoryWeight);
		Preconditions.checkArgument(
			minResources.equals(ResourceSpec.UNKNOWN) || managedMemoryWeight == Transformation.DEFAULT_MANAGED_MEMORY_WEIGHT,
			"The resources and managed memory weight should not be specified at the same time. " +
				"resources: " + minResources + ", managed memory weight: " + managedMemoryWeight);
	}
}
