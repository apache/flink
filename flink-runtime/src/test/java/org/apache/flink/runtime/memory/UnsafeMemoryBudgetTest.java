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

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Test suite for {@link UnsafeMemoryBudget}. */
public class UnsafeMemoryBudgetTest extends TestLogger {

    @Test
    public void testGetTotalMemory() {
        UnsafeMemoryBudget budget = createUnsafeMemoryBudget();
        assertThat(budget.getTotalMemorySize(), is(100L));
    }

    @Test
    public void testAvailableMemory() throws MemoryReservationException {
        UnsafeMemoryBudget budget = createUnsafeMemoryBudget();
        assertThat(budget.getAvailableMemorySize(), is(100L));

        budget.reserveMemory(10L);
        assertThat(budget.getAvailableMemorySize(), is(90L));

        budget.releaseMemory(10L);
        assertThat(budget.getAvailableMemorySize(), is(100L));
    }

    @Test
    public void testReserveMemory() throws MemoryReservationException {
        UnsafeMemoryBudget budget = createUnsafeMemoryBudget();
        budget.reserveMemory(50L);
        assertThat(budget.getAvailableMemorySize(), is(50L));
    }

    @Test(expected = MemoryReservationException.class)
    public void testReserveMemoryOverLimitFails() throws MemoryReservationException {
        UnsafeMemoryBudget budget = createUnsafeMemoryBudget();
        budget.reserveMemory(120L);
    }

    @Test
    public void testReleaseMemory() throws MemoryReservationException {
        UnsafeMemoryBudget budget = createUnsafeMemoryBudget();
        budget.reserveMemory(50L);
        budget.releaseMemory(30L);
        assertThat(budget.getAvailableMemorySize(), is(80L));
    }

    @Test(expected = IllegalStateException.class)
    public void testReleaseMemoryMoreThanReservedFails() throws MemoryReservationException {
        UnsafeMemoryBudget budget = createUnsafeMemoryBudget();
        budget.reserveMemory(50L);
        budget.releaseMemory(70L);
    }

    private static UnsafeMemoryBudget createUnsafeMemoryBudget() {
        return new UnsafeMemoryBudget(100L);
    }
}
