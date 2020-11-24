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

package org.apache.flink.runtime.util.config.memory;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.state.StateBackendLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utils for configuration and calculations related to managed memory and its various use cases.
 */
public enum ManagedMemoryUtils {
	;

	private static final Logger LOG = LoggerFactory.getLogger(ManagedMemoryUtils.class);

	private static final int MANAGED_MEMORY_FRACTION_SCALE = 16;

	/** Valid names of managed memory consumers. */
	private static final String[] MANAGED_MEMORY_CONSUMER_VALID_NAMES = {
		TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_DATAPROC,
		TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_PYTHON
	};

	public static double convertToFractionOfSlot(
			ManagedMemoryUseCase useCase,
			double fractionOfUseCase,
			Set<ManagedMemoryUseCase> allUseCases,
			Configuration config,
			Optional<Boolean> stateBackendFromApplicationUsesManagedMemory,
			ClassLoader classLoader) {

		final boolean stateBackendUsesManagedMemory = StateBackendLoader
				.stateBackendFromApplicationOrConfigOrDefaultUseManagedMemory(config, stateBackendFromApplicationUsesManagedMemory, classLoader);

		if (useCase.equals(ManagedMemoryUseCase.STATE_BACKEND) && !stateBackendUsesManagedMemory) {
			return 0.0;
		}

		final Map<ManagedMemoryUseCase, Integer> allUseCaseWeights = getManagedMemoryUseCaseWeightsFromConfig(config);
		final int totalWeights = allUseCases.stream()
			.filter((uc) -> !uc.equals(ManagedMemoryUseCase.STATE_BACKEND) || stateBackendUsesManagedMemory)
			.mapToInt((uc) -> allUseCaseWeights.getOrDefault(uc, 0))
			.sum();
		final int useCaseWeight = allUseCaseWeights.getOrDefault(useCase, 0);
		final double useCaseFractionOfSlot = totalWeights > 0 ?
			getFractionRoundedDown(useCaseWeight, totalWeights) :
			0.0;

		return fractionOfUseCase * useCaseFractionOfSlot;
	}

	@VisibleForTesting
	static Map<ManagedMemoryUseCase, Integer> getManagedMemoryUseCaseWeightsFromConfig(Configuration config) {
		final Map<String, String> consumerWeights = config.get(TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS);

		for (String consumer : MANAGED_MEMORY_CONSUMER_VALID_NAMES) {
			if (!consumerWeights.containsKey(consumer)) {
				LOG.debug("Managed memory consumer weight for {} is not configured. Jobs containing this type of " +
					"managed memory consumers may fail due to not being able to allocate managed memory.", consumer);
			}
		}

		return consumerWeights.entrySet().stream()
			.flatMap((entry) -> {
				final String consumer = entry.getKey();
				final int weight = Integer.parseInt(entry.getValue());

				if (weight < 0) {
					throw new IllegalConfigurationException(String.format(
						"Managed memory weight should not be negative. Configured weight for %s is %d.", consumer, weight));
				}

				if (weight == 0) {
					LOG.debug("Managed memory consumer weight for {} is configured to 0. Jobs containing this type of " +
						"managed memory consumers may fail due to not being able to allocate managed memory.", consumer);
				}

				switch (consumer) {
					case TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_DATAPROC:
						return Stream.of(
							Tuple2.of(ManagedMemoryUseCase.BATCH_OP, weight),
							Tuple2.of(ManagedMemoryUseCase.STATE_BACKEND, weight));
					case TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_PYTHON:
						return Stream.of(Tuple2.of(ManagedMemoryUseCase.PYTHON, weight));
					default:
						throw new IllegalConfigurationException("Unknown managed memory consumer: " + consumer);
				}
			})
			.collect(Collectors.toMap(
				(tuple2) -> tuple2.f0,
				(tuple2) -> tuple2.f1));
	}

	public static double getFractionRoundedDown(final long dividend, final long divisor) {
		return BigDecimal.valueOf(dividend)
			.divide(BigDecimal.valueOf(divisor), MANAGED_MEMORY_FRACTION_SCALE, BigDecimal.ROUND_DOWN)
			.doubleValue();
	}
}
