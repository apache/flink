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

package org.apache.flink.runtime.memory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Test suite for {@link UnsafeMemoryBudget}. */
class UnsafeMemoryBudgetTest {

    @Test
    void testGetTotalMemory() {
        UnsafeMemoryBudget budget = createUnsafeMemoryBudget();
        assertThat(budget.getTotalMemorySize()).isEqualTo(100L);
    }

    @Test
    void testAvailableMemory() throws MemoryReservationException {
        UnsafeMemoryBudget budget = createUnsafeMemoryBudget();
        assertThat(budget.getAvailableMemorySize()).isEqualTo(100L);

        budget.reserveMemory(10L);
        assertThat(budget.getAvailableMemorySize()).isEqualTo(90L);

        budget.releaseMemory(10L);
        assertThat(budget.getAvailableMemorySize()).isEqualTo(100L);
    }

    @Test
    void testReserveMemory() throws MemoryReservationException {
        UnsafeMemoryBudget budget = createUnsafeMemoryBudget();
        budget.reserveMemory(50L);
        assertThat(budget.getAvailableMemorySize()).isEqualTo(50L);
    }

    @Test
    void testReserveMemoryOverLimitFails() {
        UnsafeMemoryBudget budget = createUnsafeMemoryBudget();
        assertThatExceptionOfType(MemoryReservationException.class)
                .isThrownBy(() -> budget.reserveMemory(120L));
    }

    @Test
    void testReleaseMemory() throws MemoryReservationException {
        UnsafeMemoryBudget budget = createUnsafeMemoryBudget();
        budget.reserveMemory(50L);
        budget.releaseMemory(30L);
        assertThat(budget.getAvailableMemorySize()).isEqualTo(80L);
    }

    @Test
    void testReleaseMemoryMoreThanReservedFails() throws MemoryReservationException {
        UnsafeMemoryBudget budget = createUnsafeMemoryBudget();
        budget.reserveMemory(50L);
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> budget.releaseMemory(70L));
    }

    private static UnsafeMemoryBudget createUnsafeMemoryBudget() {
        return new UnsafeMemoryBudget(100L);
    }
}
