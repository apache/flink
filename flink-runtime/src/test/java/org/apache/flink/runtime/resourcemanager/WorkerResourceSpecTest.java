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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WorkerResourceSpec}. */
class WorkerResourceSpecTest {
    private static final String EXTERNAL_RESOURCE_NAME = "gpu";

    @Test
    void testEquals() {
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

        assertThat(spec1).isEqualTo(spec1);
        assertThat(spec1).isEqualTo(spec2);
        assertThat(spec1).isNotEqualTo(spec3);
        assertThat(spec1).isNotEqualTo(spec4);
        assertThat(spec1).isNotEqualTo(spec5);
        assertThat(spec1).isNotEqualTo(spec6);
        assertThat(spec1).isNotEqualTo(spec7);
        assertThat(spec1).isNotEqualTo(spec8);
        assertThat(spec1).isNotEqualTo(spec9);
        assertThat(spec1).isNotEqualTo(spec10);
    }

    @Test
    void testHashCodeEquals() {
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

        assertThat(spec1.hashCode()).isEqualTo(spec2.hashCode());
        assertThat(spec1.hashCode()).isEqualTo(spec1.hashCode());
        assertThat(spec1.hashCode()).isNotEqualTo(spec3.hashCode());
        assertThat(spec1.hashCode()).isNotEqualTo(spec4.hashCode());
        assertThat(spec1.hashCode()).isNotEqualTo(spec5.hashCode());
        assertThat(spec1.hashCode()).isNotEqualTo(spec6.hashCode());
        assertThat(spec1.hashCode()).isNotEqualTo(spec7.hashCode());
        assertThat(spec1.hashCode()).isNotEqualTo(spec8.hashCode());
        assertThat(spec1.hashCode()).isNotEqualTo(spec9.hashCode());
        assertThat(spec1.hashCode()).isNotEqualTo(spec10.hashCode());
    }

    @Test
    void testCreateFromTaskExecutorProcessSpec() {
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
        assertThat(workerResourceSpec.getCpuCores())
                .isEqualTo(taskExecutorProcessSpec.getCpuCores());
        assertThat(workerResourceSpec.getTaskHeapSize())
                .isEqualTo(taskExecutorProcessSpec.getTaskHeapSize());
        assertThat(workerResourceSpec.getTaskOffHeapSize())
                .isEqualTo(taskExecutorProcessSpec.getTaskOffHeapSize());
        assertThat(workerResourceSpec.getNetworkMemSize())
                .isEqualTo(taskExecutorProcessSpec.getNetworkMemSize());
        assertThat(workerResourceSpec.getManagedMemSize())
                .isEqualTo(taskExecutorProcessSpec.getManagedMemorySize());
        assertThat(workerResourceSpec.getNumSlots())
                .isEqualTo(taskExecutorProcessSpec.getNumSlots());
        assertThat(workerResourceSpec.getExtendedResources())
                .isEqualTo(taskExecutorProcessSpec.getExtendedResources());
    }

    @Test
    void testCreateFromResourceProfile() {
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
        assertThat(workerResourceSpec.getCpuCores()).isEqualTo(resourceProfile.getCpuCores());
        assertThat(workerResourceSpec.getTaskHeapSize())
                .isEqualTo(resourceProfile.getTaskHeapMemory());
        assertThat(workerResourceSpec.getTaskOffHeapSize())
                .isEqualTo(resourceProfile.getTaskOffHeapMemory());
        assertThat(workerResourceSpec.getNetworkMemSize())
                .isEqualTo(resourceProfile.getNetworkMemory());
        assertThat(workerResourceSpec.getManagedMemSize())
                .isEqualTo(resourceProfile.getManagedMemory());
        assertThat(workerResourceSpec.getNumSlots()).isEqualTo(numSlots);
        assertThat(workerResourceSpec.getExtendedResources())
                .isEqualTo(resourceProfile.getExtendedResources());
    }
}
