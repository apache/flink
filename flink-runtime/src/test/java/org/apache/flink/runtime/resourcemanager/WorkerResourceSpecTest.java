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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/** Tests for {@link WorkerResourceSpec}. */
public class WorkerResourceSpecTest extends TestLogger {
    private static final String EXTERNAL_RESOURCE_NAME = "gpu";

    @Test
    public void testEquals() {
        final WorkerResourceSpec spec1 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec2 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec3 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.1)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec4 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(110)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec5 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(110)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec6 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(110)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec7 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(110)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec8 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(2)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec9 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2))
                        .build();
        final WorkerResourceSpec spec10 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .build();

        assertEquals(spec1, spec1);
        assertEquals(spec1, spec2);
        assertNotEquals(spec1, spec3);
        assertNotEquals(spec1, spec4);
        assertNotEquals(spec1, spec5);
        assertNotEquals(spec1, spec6);
        assertNotEquals(spec1, spec7);
        assertNotEquals(spec1, spec8);
        assertNotEquals(spec1, spec9);
        assertNotEquals(spec1, spec10);
    }

    @Test
    public void testHashCodeEquals() {
        final WorkerResourceSpec spec1 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec2 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec3 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.1)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec4 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(110)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec5 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(110)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec6 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(110)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec7 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(110)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec8 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(2)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec9 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2))
                        .build();
        final WorkerResourceSpec spec10 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .build();

        assertEquals(spec1.hashCode(), spec1.hashCode());
        assertEquals(spec1.hashCode(), spec2.hashCode());
        assertNotEquals(spec1.hashCode(), spec3.hashCode());
        assertNotEquals(spec1.hashCode(), spec4.hashCode());
        assertNotEquals(spec1.hashCode(), spec5.hashCode());
        assertNotEquals(spec1.hashCode(), spec6.hashCode());
        assertNotEquals(spec1.hashCode(), spec7.hashCode());
        assertNotEquals(spec1.hashCode(), spec8.hashCode());
        assertNotEquals(spec1.hashCode(), spec9.hashCode());
        assertNotEquals(spec1.hashCode(), spec10.hashCode());
    }

    @Test
    public void testCreateFromTaskExecutorProcessSpec() {
        final Configuration config = new Configuration();
        config.setString(
                ExternalResourceOptions.EXTERNAL_RESOURCE_LIST.key(), EXTERNAL_RESOURCE_NAME);
        config.setLong(
                ExternalResourceOptions.getAmountConfigOptionForResource(EXTERNAL_RESOURCE_NAME),
                1);

        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.newProcessSpecBuilder(config)
                        .withTotalProcessMemory(MemorySize.ofMebiBytes(1024))
                        .build();
        final WorkerResourceSpec workerResourceSpec =
                WorkerResourceSpec.fromTaskExecutorProcessSpec(taskExecutorProcessSpec);
        assertEquals(workerResourceSpec.getCpuCores(), taskExecutorProcessSpec.getCpuCores());
        assertEquals(
                workerResourceSpec.getTaskHeapSize(), taskExecutorProcessSpec.getTaskHeapSize());
        assertEquals(
                workerResourceSpec.getTaskOffHeapSize(),
                taskExecutorProcessSpec.getTaskOffHeapSize());
        assertEquals(
                workerResourceSpec.getNetworkMemSize(),
                taskExecutorProcessSpec.getNetworkMemSize());
        assertEquals(
                workerResourceSpec.getManagedMemSize(),
                taskExecutorProcessSpec.getManagedMemorySize());
        assertEquals(workerResourceSpec.getNumSlots(), taskExecutorProcessSpec.getNumSlots());
        assertEquals(
                workerResourceSpec.getExtendedResources(),
                taskExecutorProcessSpec.getExtendedResources());
    }

    @Test
    public void testCreateFromResourceProfile() {
        final int numSlots = 3;
        final ResourceProfile resourceProfile =
                ResourceProfile.newBuilder()
                        .setCpuCores(1)
                        .setManagedMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setTaskOffHeapMemoryMB(10)
                        .setTaskHeapMemoryMB(10)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec workerResourceSpec =
                WorkerResourceSpec.fromTotalResourceProfile(resourceProfile, numSlots);
        assertEquals(workerResourceSpec.getCpuCores(), resourceProfile.getCpuCores());
        assertEquals(workerResourceSpec.getTaskHeapSize(), resourceProfile.getTaskHeapMemory());
        assertEquals(
                workerResourceSpec.getTaskOffHeapSize(), resourceProfile.getTaskOffHeapMemory());
        assertEquals(workerResourceSpec.getNetworkMemSize(), resourceProfile.getNetworkMemory());
        assertEquals(workerResourceSpec.getManagedMemSize(), resourceProfile.getManagedMemory());
        assertEquals(workerResourceSpec.getNumSlots(), numSlots);
        assertEquals(
                workerResourceSpec.getExtendedResources(), resourceProfile.getExtendedResources());
    }
}
