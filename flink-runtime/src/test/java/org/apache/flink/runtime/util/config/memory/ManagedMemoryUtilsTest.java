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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ManagedMemoryUtils}. */
class ManagedMemoryUtilsTest {

    private static final double DELTA = 0.000001;

    private static final int DATA_PROC_WEIGHT = 111;
    private static final int PYTHON_WEIGHT = 222;
    private static final int OPERATOR_WEIGHT = 333;
    private static final int STATE_BACKEND_WEIGHT = 444;
    private static final int TOTAL_WEIGHT = PYTHON_WEIGHT + OPERATOR_WEIGHT + STATE_BACKEND_WEIGHT;

    private static final UnmodifiableConfiguration CONFIG_WITH_ALL_USE_CASES =
            new UnmodifiableConfiguration(
                    new Configuration() {
                        {
                            set(
                                    TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS,
                                    new HashMap<String, String>() {
                                        {
                                            put(
                                                    TaskManagerOptions
                                                            .MANAGED_MEMORY_CONSUMER_NAME_DATAPROC,
                                                    String.valueOf(DATA_PROC_WEIGHT));
                                            put(
                                                    TaskManagerOptions
                                                            .MANAGED_MEMORY_CONSUMER_NAME_PYTHON,
                                                    String.valueOf(PYTHON_WEIGHT));
                                            put(
                                                    TaskManagerOptions
                                                            .MANAGED_MEMORY_CONSUMER_NAME_OPERATOR,
                                                    String.valueOf(OPERATOR_WEIGHT));
                                            put(
                                                    TaskManagerOptions
                                                            .MANAGED_MEMORY_CONSUMER_NAME_STATE_BACKEND,
                                                    String.valueOf(STATE_BACKEND_WEIGHT));
                                        }
                                    });
                        }
                    });

    private static final UnmodifiableConfiguration CONFIG_WITH_LEGACY_USE_CASES =
            new UnmodifiableConfiguration(
                    new Configuration() {
                        {
                            set(
                                    TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS,
                                    new HashMap<String, String>() {
                                        {
                                            put(
                                                    TaskManagerOptions
                                                            .MANAGED_MEMORY_CONSUMER_NAME_DATAPROC,
                                                    String.valueOf(DATA_PROC_WEIGHT));
                                            put(
                                                    TaskManagerOptions
                                                            .MANAGED_MEMORY_CONSUMER_NAME_PYTHON,
                                                    String.valueOf(PYTHON_WEIGHT));
                                        }
                                    });
                        }
                    });

    @Test
    void testGetWeightsFromConfig() {
        final Map<ManagedMemoryUseCase, Integer> expectedWeights =
                new HashMap<ManagedMemoryUseCase, Integer>() {
                    {
                        put(ManagedMemoryUseCase.OPERATOR, OPERATOR_WEIGHT);
                        put(ManagedMemoryUseCase.STATE_BACKEND, STATE_BACKEND_WEIGHT);
                        put(ManagedMemoryUseCase.PYTHON, PYTHON_WEIGHT);
                    }
                };

        final Map<ManagedMemoryUseCase, Integer> configuredWeights =
                ManagedMemoryUtils.getManagedMemoryUseCaseWeightsFromConfig(
                        CONFIG_WITH_ALL_USE_CASES);

        assertThat(configuredWeights).isEqualTo(expectedWeights);
    }

    @Test
    void testGetWeightsFromConfigLegacy() {
        final Map<ManagedMemoryUseCase, Integer> expectedWeights =
                new HashMap<ManagedMemoryUseCase, Integer>() {
                    {
                        put(ManagedMemoryUseCase.OPERATOR, DATA_PROC_WEIGHT);
                        put(ManagedMemoryUseCase.STATE_BACKEND, DATA_PROC_WEIGHT);
                        put(ManagedMemoryUseCase.PYTHON, PYTHON_WEIGHT);
                    }
                };

        final Map<ManagedMemoryUseCase, Integer> configuredWeights =
                ManagedMemoryUtils.getManagedMemoryUseCaseWeightsFromConfig(
                        CONFIG_WITH_LEGACY_USE_CASES);

        assertThat(configuredWeights).isEqualTo(expectedWeights);
    }

    @Test
    void testGetWeightsFromConfigFailNegativeWeight() {
        final Configuration config =
                new Configuration() {
                    {
                        set(
                                TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS,
                                Collections.singletonMap(
                                        TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_OPERATOR,
                                        "-123"));
                    }
                };
        assertThatThrownBy(
                        () -> ManagedMemoryUtils.getManagedMemoryUseCaseWeightsFromConfig(config))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void testConvertToFractionOfSlot() {
        final ManagedMemoryUseCase useCase = ManagedMemoryUseCase.OPERATOR;
        final double fractionOfUseCase = 0.3;

        final double fractionOfSlot =
                ManagedMemoryUtils.convertToFractionOfSlot(
                        useCase,
                        fractionOfUseCase,
                        new HashSet<ManagedMemoryUseCase>() {
                            {
                                add(ManagedMemoryUseCase.OPERATOR);
                                add(ManagedMemoryUseCase.STATE_BACKEND);
                                add(ManagedMemoryUseCase.PYTHON);
                            }
                        },
                        CONFIG_WITH_ALL_USE_CASES,
                        Optional.of(true),
                        ClassLoader.getSystemClassLoader());

        assertThat(fractionOfSlot)
                .isCloseTo(
                        fractionOfUseCase * OPERATOR_WEIGHT / TOTAL_WEIGHT, Offset.offset(DELTA));
    }

    @Test
    void testConvertToFractionOfSlotWeightNotConfigured() {
        final ManagedMemoryUseCase useCase = ManagedMemoryUseCase.OPERATOR;
        final double fractionOfUseCase = 0.3;

        final Configuration config =
                new Configuration() {
                    {
                        set(
                                TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS,
                                Collections.emptyMap());
                    }
                };

        final double fractionOfSlot =
                ManagedMemoryUtils.convertToFractionOfSlot(
                        useCase,
                        fractionOfUseCase,
                        new HashSet<ManagedMemoryUseCase>() {
                            {
                                add(ManagedMemoryUseCase.OPERATOR);
                                add(ManagedMemoryUseCase.STATE_BACKEND);
                                add(ManagedMemoryUseCase.PYTHON);
                            }
                        },
                        config,
                        Optional.of(true),
                        ClassLoader.getSystemClassLoader());

        assertThat(fractionOfSlot).isCloseTo(0.0, Offset.offset(DELTA));
    }

    @Test
    void testConvertToFractionOfSlotStateBackendUseManagedMemory() {
        testConvertToFractionOfSlotGivenWhetherStateBackendUsesManagedMemory(
                true,
                1.0 * OPERATOR_WEIGHT / TOTAL_WEIGHT,
                1.0 * STATE_BACKEND_WEIGHT / TOTAL_WEIGHT,
                1.0 * PYTHON_WEIGHT / TOTAL_WEIGHT);
    }

    @Test
    void testConvertToFractionOfSlotStateBackendNotUserManagedMemory() {
        final int totalWeight = OPERATOR_WEIGHT + PYTHON_WEIGHT;
        testConvertToFractionOfSlotGivenWhetherStateBackendUsesManagedMemory(
                false, 1.0 * OPERATOR_WEIGHT / totalWeight, 0.0, 1.0 * PYTHON_WEIGHT / totalWeight);
    }

    private void testConvertToFractionOfSlotGivenWhetherStateBackendUsesManagedMemory(
            boolean stateBackendUsesManagedMemory,
            double expectedOperatorFractionOfSlot,
            double expectedStateFractionOfSlot,
            double expectedPythonFractionOfSlot) {

        final Set<ManagedMemoryUseCase> allUseCases =
                new HashSet<ManagedMemoryUseCase>() {
                    {
                        add(ManagedMemoryUseCase.OPERATOR);
                        add(ManagedMemoryUseCase.STATE_BACKEND);
                        add(ManagedMemoryUseCase.PYTHON);
                    }
                };

        final double opFractionOfSlot =
                ManagedMemoryUtils.convertToFractionOfSlot(
                        ManagedMemoryUseCase.OPERATOR,
                        1.0,
                        allUseCases,
                        CONFIG_WITH_ALL_USE_CASES,
                        Optional.of(stateBackendUsesManagedMemory),
                        ClassLoader.getSystemClassLoader());
        final double stateFractionOfSlot =
                ManagedMemoryUtils.convertToFractionOfSlot(
                        ManagedMemoryUseCase.STATE_BACKEND,
                        1.0,
                        allUseCases,
                        CONFIG_WITH_ALL_USE_CASES,
                        Optional.of(stateBackendUsesManagedMemory),
                        ClassLoader.getSystemClassLoader());
        final double pythonFractionOfSlot =
                ManagedMemoryUtils.convertToFractionOfSlot(
                        ManagedMemoryUseCase.PYTHON,
                        1.0,
                        allUseCases,
                        CONFIG_WITH_ALL_USE_CASES,
                        Optional.of(stateBackendUsesManagedMemory),
                        ClassLoader.getSystemClassLoader());

        assertThat(opFractionOfSlot)
                .isCloseTo(expectedOperatorFractionOfSlot, Offset.offset(DELTA));
        assertThat(stateFractionOfSlot)
                .isCloseTo(expectedStateFractionOfSlot, Offset.offset(DELTA));
        assertThat(pythonFractionOfSlot)
                .isCloseTo(expectedPythonFractionOfSlot, Offset.offset(DELTA));
    }

    @Test
    void testUseCaseWeightsConfiguredWithConsistentValue() {
        final Map<ManagedMemoryUseCase, Integer> existingWeights =
                new HashMap<ManagedMemoryUseCase, Integer>() {
                    {
                        put(ManagedMemoryUseCase.OPERATOR, 123);
                    }
                };

        final Map<ManagedMemoryUseCase, Integer> newWeights =
                new HashMap<ManagedMemoryUseCase, Integer>() {
                    {
                        put(ManagedMemoryUseCase.OPERATOR, 123);
                        put(ManagedMemoryUseCase.STATE_BACKEND, 456);
                    }
                };

        ManagedMemoryUtils.validateUseCaseWeightsNotConflict(existingWeights, newWeights);
    }

    @Test
    void testUseCaseWeightsConfiguredWithConflictValue() {
        final Map<ManagedMemoryUseCase, Integer> existingWeights =
                new HashMap<ManagedMemoryUseCase, Integer>() {
                    {
                        put(ManagedMemoryUseCase.OPERATOR, 123);
                    }
                };

        final Map<ManagedMemoryUseCase, Integer> newWeights =
                new HashMap<ManagedMemoryUseCase, Integer>() {
                    {
                        put(ManagedMemoryUseCase.OPERATOR, 456);
                    }
                };

        assertThatThrownBy(
                        () ->
                                ManagedMemoryUtils.validateUseCaseWeightsNotConflict(
                                        existingWeights, newWeights))
                .isInstanceOf(IllegalStateException.class);
    }
}
