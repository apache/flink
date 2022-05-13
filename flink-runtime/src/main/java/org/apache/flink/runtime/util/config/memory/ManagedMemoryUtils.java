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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.state.StateBackendLoader;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/** Utils for configuration and calculations related to managed memory and its various use cases. */
public enum ManagedMemoryUtils {
    ;

    private static final Logger LOG = LoggerFactory.getLogger(ManagedMemoryUtils.class);

    private static final int MANAGED_MEMORY_FRACTION_SCALE = 16;

    /** Names of managed memory use cases, in the fallback order. */
    @SuppressWarnings("deprecation")
    private static final Map<ManagedMemoryUseCase, List<String>> USE_CASE_CONSUMER_NAMES =
            ImmutableMap.of(
                    ManagedMemoryUseCase.OPERATOR,
                    ImmutableList.of(
                            TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_OPERATOR,
                            TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_DATAPROC),
                    ManagedMemoryUseCase.STATE_BACKEND,
                    ImmutableList.of(
                            TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_STATE_BACKEND,
                            TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_DATAPROC),
                    ManagedMemoryUseCase.PYTHON,
                    ImmutableList.of(TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_PYTHON));

    public static double convertToFractionOfSlot(
            ManagedMemoryUseCase useCase,
            double fractionOfUseCase,
            Set<ManagedMemoryUseCase> allUseCases,
            Configuration config,
            Optional<Boolean> stateBackendFromApplicationUsesManagedMemory,
            ClassLoader classLoader) {

        final boolean stateBackendUsesManagedMemory =
                StateBackendLoader.stateBackendFromApplicationOrConfigOrDefaultUseManagedMemory(
                        config, stateBackendFromApplicationUsesManagedMemory, classLoader);

        if (useCase.equals(ManagedMemoryUseCase.STATE_BACKEND) && !stateBackendUsesManagedMemory) {
            return 0.0;
        }

        final Map<ManagedMemoryUseCase, Integer> allUseCaseWeights =
                getManagedMemoryUseCaseWeightsFromConfig(config);
        final int totalWeights =
                allUseCases.stream()
                        .filter(
                                (uc) ->
                                        !uc.equals(ManagedMemoryUseCase.STATE_BACKEND)
                                                || stateBackendUsesManagedMemory)
                        .mapToInt((uc) -> allUseCaseWeights.getOrDefault(uc, 0))
                        .sum();
        final int useCaseWeight = allUseCaseWeights.getOrDefault(useCase, 0);
        final double useCaseFractionOfSlot =
                totalWeights > 0 ? getFractionRoundedDown(useCaseWeight, totalWeights) : 0.0;

        return fractionOfUseCase * useCaseFractionOfSlot;
    }

    @VisibleForTesting
    static Map<ManagedMemoryUseCase, Integer> getManagedMemoryUseCaseWeightsFromConfig(
            Configuration config) {
        final Map<String, String> configuredWeights =
                config.get(TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS);
        final Map<ManagedMemoryUseCase, Integer> effectiveWeights = new HashMap<>();

        for (Map.Entry<ManagedMemoryUseCase, List<String>> entry :
                USE_CASE_CONSUMER_NAMES.entrySet()) {
            final ManagedMemoryUseCase useCase = entry.getKey();
            final Iterator<String> nameIter = entry.getValue().iterator();

            boolean findWeight = false;
            while (!findWeight && nameIter.hasNext()) {
                final String name = nameIter.next();
                final String weightStr = configuredWeights.get(name);
                if (weightStr != null) {
                    final int weight = Integer.parseInt(weightStr);
                    findWeight = true;

                    if (weight < 0) {
                        throw new IllegalConfigurationException(
                                String.format(
                                        "Managed memory weight should not be negative. Configured "
                                                + "weight for %s is %d.",
                                        useCase, weight));
                    }

                    if (weight == 0) {
                        LOG.debug(
                                "Managed memory consumer weight for {} is configured to 0. Jobs "
                                        + "containing this type of managed memory consumers may "
                                        + "fail due to not being able to allocate managed memory.",
                                useCase);
                    }

                    effectiveWeights.put(useCase, weight);
                }
            }

            if (!findWeight) {
                LOG.debug(
                        "Managed memory consumer weight for {} is not configured. Jobs containing "
                                + "this type of managed memory consumers may fail due to not being "
                                + "able to allocate managed memory.",
                        useCase);
            }
        }

        return effectiveWeights;
    }

    public static double getFractionRoundedDown(final long dividend, final long divisor) {
        return BigDecimal.valueOf(dividend)
                .divide(
                        BigDecimal.valueOf(divisor),
                        MANAGED_MEMORY_FRACTION_SCALE,
                        BigDecimal.ROUND_DOWN)
                .doubleValue();
    }

    public static void validateUseCaseWeightsNotConflict(
            Map<ManagedMemoryUseCase, Integer> weights1,
            Map<ManagedMemoryUseCase, Integer> weights2) {
        weights1.forEach(
                (useCase, weight1) ->
                        checkState(
                                weights2.getOrDefault(useCase, weight1).equals(weight1),
                                String.format(
                                        "Conflict managed memory consumer weights for '%s' were configured: '%d' and '%d'.",
                                        useCase, weight1, weights2.get(useCase))));
    }
}
