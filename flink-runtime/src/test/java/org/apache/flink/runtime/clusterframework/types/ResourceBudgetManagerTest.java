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

import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Tests for {@link ResourceBudgetManager}. */
public class ResourceBudgetManagerTest {

    @Test
    public void testReserve() {
        ResourceBudgetManager budgetManager =
                new ResourceBudgetManager(createResourceProfile(1.0, 100));

        assertThat(budgetManager.reserve(createResourceProfile(0.7, 70)), Matchers.is(true));
        assertThat(budgetManager.getAvailableBudget(), is(createResourceProfile(0.3, 30)));
    }

    @Test
    public void testReserveFail() {
        ResourceBudgetManager budgetManager =
                new ResourceBudgetManager(createResourceProfile(1.0, 100));

        assertThat(budgetManager.reserve(createResourceProfile(1.2, 120)), Matchers.is(false));
        assertThat(budgetManager.getAvailableBudget(), is(createResourceProfile(1.0, 100)));
    }

    @Test
    public void testRelease() {
        ResourceBudgetManager budgetManager =
                new ResourceBudgetManager(createResourceProfile(1.0, 100));

        assertThat(budgetManager.reserve(createResourceProfile(0.7, 70)), Matchers.is(true));
        assertThat(budgetManager.release(createResourceProfile(0.5, 50)), Matchers.is(true));
        assertThat(budgetManager.getAvailableBudget(), is(createResourceProfile(0.8, 80)));
    }

    @Test
    public void testReleaseFail() {
        ResourceBudgetManager budgetManager =
                new ResourceBudgetManager(createResourceProfile(1.0, 100));

        assertThat(budgetManager.reserve(createResourceProfile(0.7, 70)), Matchers.is(true));
        assertThat(budgetManager.release(createResourceProfile(0.8, 80)), Matchers.is(false));
        assertThat(budgetManager.getAvailableBudget(), is(createResourceProfile(0.3, 30)));
    }

    private static ResourceProfile createResourceProfile(double cpus, int memory) {
        return ResourceProfile.newBuilder()
                .setCpuCores(cpus)
                .setTaskHeapMemory(MemorySize.ofMebiBytes(memory))
                .build();
    }
}
