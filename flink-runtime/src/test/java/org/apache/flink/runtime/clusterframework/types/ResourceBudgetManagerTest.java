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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.configuration.MemorySize;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ResourceBudgetManager}. */
public class ResourceBudgetManagerTest {

    @Test
    void testReserve() {
        ResourceBudgetManager budgetManager =
                new ResourceBudgetManager(createResourceProfile(1.0, 100));

        assertThat(budgetManager.reserve(createResourceProfile(0.7, 70))).isEqualTo(true);
        assertThat(budgetManager.getAvailableBudget()).isEqualTo(createResourceProfile(0.3, 30));
    }

    @Test
    void testReserveFail() {
        ResourceBudgetManager budgetManager =
                new ResourceBudgetManager(createResourceProfile(1.0, 100));

        assertThat(budgetManager.reserve(createResourceProfile(1.2, 120))).isEqualTo(false);
        assertThat(budgetManager.getAvailableBudget()).isEqualTo(createResourceProfile(1.0, 100));
    }

    @Test
    void testRelease() {
        ResourceBudgetManager budgetManager =
                new ResourceBudgetManager(createResourceProfile(1.0, 100));

        assertThat(budgetManager.reserve(createResourceProfile(0.7, 70))).isEqualTo(true);
        assertThat(budgetManager.release(createResourceProfile(0.5, 50))).isEqualTo(true);
        assertThat(budgetManager.getAvailableBudget()).isEqualTo(createResourceProfile(0.8, 80));
    }

    @Test
    void testReleaseFail() {
        ResourceBudgetManager budgetManager =
                new ResourceBudgetManager(createResourceProfile(1.0, 100));

        assertThat(budgetManager.reserve(createResourceProfile(0.7, 70))).isEqualTo(true);
        assertThat(budgetManager.release(createResourceProfile(0.8, 80))).isEqualTo(false);
        assertThat(budgetManager.getAvailableBudget()).isEqualTo(createResourceProfile(0.3, 30));
    }

    private static ResourceProfile createResourceProfile(double cpus, int memory) {
        return ResourceProfile.newBuilder()
                .setCpuCores(cpus)
                .setTaskHeapMemory(MemorySize.ofMebiBytes(memory))
                .build();
    }
}
