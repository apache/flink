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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotTestUtils.createPhysicalSlot;
import static org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotTestUtils.occupyPhysicalSlot;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests whether the slot occupation state of {@link AllocatedSlot} is correctly. */
class AllocatedSlotOccupationTest {

    @Test
    void testSingleTaskOccupyingSlotIndefinitely() {
        final PhysicalSlot physicalSlot = createPhysicalSlot();
        occupyPhysicalSlot(physicalSlot, true);

        assertThat(physicalSlot.willBeOccupiedIndefinitely()).isTrue();
    }

    @Test
    void testSingleTaskNotOccupyingSlotIndefinitely() {
        final PhysicalSlot physicalSlot = createPhysicalSlot();
        occupyPhysicalSlot(physicalSlot, false);

        assertThat(physicalSlot.willBeOccupiedIndefinitely()).isFalse();
    }
}
