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

package org.apache.flink.api.common.operators.util;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.resources.ExternalResource;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** Tests for {@link SlotSharingGroupUtils}. */
public class SlotSharingGroupUtilsTest {
    @Test
    public void testCovertToResourceSpec() {
        final ExternalResource gpu = new ExternalResource("gpu", 1);
        final ResourceSpec resourceSpec =
                ResourceSpec.newBuilder(1.0, 100)
                        .setManagedMemoryMB(200)
                        .setTaskOffHeapMemoryMB(300)
                        .setExtendedResource(gpu)
                        .build();
        final SlotSharingGroup slotSharingGroup1 =
                SlotSharingGroup.newBuilder("ssg")
                        .setCpuCores(resourceSpec.getCpuCores().getValue().doubleValue())
                        .setTaskHeapMemory(resourceSpec.getTaskHeapMemory())
                        .setTaskOffHeapMemory(resourceSpec.getTaskOffHeapMemory())
                        .setManagedMemory(resourceSpec.getManagedMemory())
                        .setExternalResource(gpu.getName(), gpu.getValue().doubleValue())
                        .build();
        final SlotSharingGroup slotSharingGroup2 = SlotSharingGroup.newBuilder("ssg").build();

        assertThat(SlotSharingGroupUtils.extractResourceSpec(slotSharingGroup1), is(resourceSpec));
        assertThat(
                SlotSharingGroupUtils.extractResourceSpec(slotSharingGroup2),
                is(ResourceSpec.UNKNOWN));
    }
}
