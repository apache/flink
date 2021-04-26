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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for {@link ManagedMemoryUtils}. */
public class ManagedMemoryUtilsTest extends TestLogger {

    private static final double DELTA = 0.000001;

    private static final int DATA_PROC_WEIGHT = 111;
    private static final int PYTHON_WEIGHT = 222;

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
                                        }
                                    });
                        }
                    });

    @Test
    public void testGetWeightsFromConfig() {
        final Map<ManagedMemoryUseCase, Integer> expectedWeights =
                new HashMap<ManagedMemoryUseCase, Integer>() {
                    {
                        put(ManagedMemoryUseCase.STATE_BACKEND, DATA_PROC_WEIGHT);
                        put(ManagedMemoryUseCase.OPERATOR, DATA_PROC_WEIGHT);
                        put(ManagedMemoryUseCase.PYTHON, PYTHON_WEIGHT);
                    }
                };

        final Map<ManagedMemoryUseCase, Integer> configuredWeights =
                ManagedMemoryUtils.getManagedMemoryUseCaseWeightsFromConfig(
                        CONFIG_WITH_ALL_USE_CASES);

        assertThat(configuredWeights, is(expectedWeights));
    }

    @Test(expected = IllegalConfigurationException.class)
    public void testGetWeightsFromConfigFailUnknownUseCase() {
        final Configuration config =
                new Configuration() {
                    {
                        set(
                                TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS,
                                Collections.singletonMap("UNKNOWN_KEY", "123"));
                    }
                };

        ManagedMemoryUtils.getManagedMemoryUseCaseWeightsFromConfig(config);
    }

    @Test(expected = IllegalConfigurationException.class)
    public void testGetWeightsFromConfigFailNegativeWeight() {
        final Configuration config =
                new Configuration() {
                    {
                        set(
                                TaskManagerOptions.MANAGED_MEMORY_CONSUMER_WEIGHTS,
                                Collections.singletonMap(
                                        TaskManagerOptions.MANAGED_MEMORY_CONSUMER_NAME_DATAPROC,
                                        "-123"));
                    }
                };

        ManagedMemoryUtils.getManagedMemoryUseCaseWeightsFromConfig(config);
    }

    @Test
    public void testConvertToFractionOfSlot() {
        final ManagedMemoryUseCase useCase = ManagedMemoryUseCase.OPERATOR;
        final double fractionOfUseCase = 0.3;

        final double fractionOfSlot =
                ManagedMemoryUtils.convertToFractionOfSlot(
                        useCase,
                        fractionOfUseCase,
                        new HashSet<ManagedMemoryUseCase>() {
                            {
                                add(ManagedMemoryUseCase.OPERATOR);
                                add(ManagedMemoryUseCase.PYTHON);
                            }
                        },
                        CONFIG_WITH_ALL_USE_CASES,
                        Optional.empty(),
                        ClassLoader.getSystemClassLoader());

        assertEquals(fractionOfUseCase / 3, fractionOfSlot, DELTA);
    }

    @Test
    public void testConvertToFractionOfSlotWeightNotConfigured() {
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
                                add(ManagedMemoryUseCase.PYTHON);
                            }
                        },
                        config,
                        Optional.empty(),
                        ClassLoader.getSystemClassLoader());

        assertEquals(0.0, fractionOfSlot, DELTA);
    }

    @Test
    public void testConvertToFractionOfSlotStateBackendUseManagedMemory() {
        testConvertToFractionOfSlotGivenWhetherStateBackendUsesManagedMemory(
                true, 1.0 / 3, 1.0 * 2 / 3);
    }

    @Test
    public void testConvertToFractionOfSlotStateBackendNotUserManagedMemory() {
        testConvertToFractionOfSlotGivenWhetherStateBackendUsesManagedMemory(false, 0.0, 1.0);
    }

    private void testConvertToFractionOfSlotGivenWhetherStateBackendUsesManagedMemory(
            boolean stateBackendUsesManagedMemory,
            double expectedStateFractionOfSlot,
            double expectedPythonFractionOfSlot) {

        final Set<ManagedMemoryUseCase> allUseCases =
                new HashSet<ManagedMemoryUseCase>() {
                    {
                        add(ManagedMemoryUseCase.STATE_BACKEND);
                        add(ManagedMemoryUseCase.PYTHON);
                    }
                };

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

        assertEquals(expectedStateFractionOfSlot, stateFractionOfSlot, DELTA);
        assertEquals(expectedPythonFractionOfSlot, pythonFractionOfSlot, DELTA);
    }
}
