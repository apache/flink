/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link SlotManagerUtils}. */
public class SlotManagerUtilsTest extends TestLogger {
    @Test
    public void testGenerateDefaultSlotProfile() {
        final int numSlots = 5;
        final ResourceProfile resourceProfile =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(1)
                        .setTaskOffHeapMemoryMB(2)
                        .setNetworkMemoryMB(3)
                        .setManagedMemoryMB(4)
                        .build();
        final WorkerResourceSpec workerResourceSpec =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0 * numSlots)
                        .setTaskHeapMemoryMB(1 * numSlots)
                        .setTaskOffHeapMemoryMB(2 * numSlots)
                        .setNetworkMemoryMB(3 * numSlots)
                        .setManagedMemoryMB(4 * numSlots)
                        .build();

        assertThat(
                SlotManagerUtils.generateDefaultSlotResourceProfile(workerResourceSpec, numSlots),
                is(resourceProfile));
    }
}
