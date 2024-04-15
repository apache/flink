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

/**
 * Tests for the shared resource acquisition and initialization through the {@link MemoryManager}.
 */
class MemoryManagerSharedResourcesTest {

    @Test
    void getSameTypeGetsSameHandle() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();

        final OpaqueMemoryResource<TestResource> resource1 =
                memoryManager.getSharedMemoryResourceForManagedMemory(
                        "type", TestResource::new, 0.1);
        final OpaqueMemoryResource<TestResource> resource2 =
                memoryManager.getSharedMemoryResourceForManagedMemory(
                        "type", TestResource::new, 0.1);

        assertThat(resource2).isNotSameAs(resource1);
        assertThat(resource2.getResourceHandle()).isSameAs(resource1.getResourceHandle());
    }

    @Test
    void getDifferentTypeGetsDifferentResources() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();

        final OpaqueMemoryResource<TestResource> resource1 =
                memoryManager.getSharedMemoryResourceForManagedMemory(
                        "type1", TestResource::new, 0.1);
        final OpaqueMemoryResource<TestResource> resource2 =
                memoryManager.getSharedMemoryResourceForManagedMemory(
                        "type2", TestResource::new, 0.1);

        assertThat(resource2).isNotSameAs(resource1);
        assertThat(resource2.getResourceHandle()).isNotSameAs(resource1.getResourceHandle());
    }

    @Test
    void testAllocatesFractionOfTotalMemory() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();
        final double fraction = 0.2;

        final OpaqueMemoryResource<TestResource> resource =
                memoryManager.getSharedMemoryResourceForManagedMemory(
                        "type", TestResource::new, fraction);

        assertThat(resource.getSize()).isEqualTo((long) (0.2 * memoryManager.getMemorySize()));
    }

    @Test
    void getAllocateNewReservesMemory() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();

        memoryManager.getSharedMemoryResourceForManagedMemory("type", TestResource::new, 0.5);

        assertThat(memoryManager.availableMemory()).isEqualTo(memoryManager.getMemorySize() / 2);
    }

    @Test
    void getExistingDoesNotAllocateAdditionalMemory() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();
        memoryManager.getSharedMemoryResourceForManagedMemory("type", TestResource::new, 0.8);
        final long freeMemory = memoryManager.availableMemory();

        memoryManager.getSharedMemoryResourceForManagedMemory("type", TestResource::new, 0.8);

        assertThat(memoryManager.availableMemory()).isEqualTo(freeMemory);
    }

    @Test
    void testFailReservation() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();
        memoryManager.getSharedMemoryResourceForManagedMemory("type", TestResource::new, 0.8);

        assertThatExceptionOfType(MemoryAllocationException.class)
                .isThrownBy(
                        () ->
                                memoryManager.getSharedMemoryResourceForManagedMemory(
                                        "type2", TestResource::new, 0.8));
    }

    @Test
    void testPartialReleaseDoesNotReleaseMemory() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();
        final OpaqueMemoryResource<TestResource> resource1 =
                memoryManager.getSharedMemoryResourceForManagedMemory(
                        "type", TestResource::new, 0.1);
        memoryManager.getSharedMemoryResourceForManagedMemory("type", TestResource::new, 0.1);
        assertThat(memoryManager.verifyEmpty()).isFalse();

        resource1.close();

        assertThat(resource1.getResourceHandle().closed).isFalse();
        assertThat(memoryManager.verifyEmpty()).isFalse();
    }

    @Test
    void testLastReleaseReleasesMemory() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();
        final OpaqueMemoryResource<TestResource> resource1 =
                memoryManager.getSharedMemoryResourceForManagedMemory(
                        "type", TestResource::new, 0.1);
        final OpaqueMemoryResource<TestResource> resource2 =
                memoryManager.getSharedMemoryResourceForManagedMemory(
                        "type", TestResource::new, 0.1);
        assertThat(memoryManager.verifyEmpty()).isFalse();

        resource1.close();
        resource2.close();

        assertThat(resource1.getResourceHandle().closed).isTrue();
        assertThat(memoryManager.verifyEmpty()).isTrue();
    }

    @Test
    void testPartialReleaseDoesNotDisposeResource() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();
        final OpaqueMemoryResource<TestResource> resource1 =
                memoryManager.getSharedMemoryResourceForManagedMemory(
                        "type", TestResource::new, 0.1);
        memoryManager.getSharedMemoryResourceForManagedMemory("type", TestResource::new, 0.1);

        resource1.close();

        assertThat(resource1.getResourceHandle().closed).isFalse();
        assertThat(memoryManager.verifyEmpty()).isFalse();
    }

    @Test
    void testLastReleaseDisposesResource() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();
        final OpaqueMemoryResource<TestResource> resource1 =
                memoryManager.getSharedMemoryResourceForManagedMemory(
                        "type", TestResource::new, 0.1);
        final OpaqueMemoryResource<TestResource> resource2 =
                memoryManager.getSharedMemoryResourceForManagedMemory(
                        "type", TestResource::new, 0.1);

        resource1.close();
        resource2.close();

        assertThat(resource1.getResourceHandle().closed).isTrue();
        assertThat(resource2.getResourceHandle().closed).isTrue();
        assertThat(memoryManager.verifyEmpty()).isTrue();
    }

    @Test
    void getAllocateExternalResource() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();
        final OpaqueMemoryResource<TestResource> resource =
                memoryManager.getExternalSharedMemoryResource(
                        "external-type", TestResource::new, 1337);

        assertThat(resource.getSize()).isEqualTo(1337);
    }

    @Test
    void getExistingExternalResource() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();
        final OpaqueMemoryResource<TestResource> resource1 =
                memoryManager.getExternalSharedMemoryResource(
                        "external-type", TestResource::new, 1337);

        final OpaqueMemoryResource<TestResource> resource2 =
                memoryManager.getExternalSharedMemoryResource(
                        "external-type", TestResource::new, 1337);

        assertThat(resource2).isNotSameAs(resource1);
        assertThat(resource2.getResourceHandle()).isSameAs(resource1.getResourceHandle());
    }

    @Test
    void getDifferentExternalResources() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();
        final OpaqueMemoryResource<TestResource> resource1 =
                memoryManager.getExternalSharedMemoryResource(
                        "external-type-1", TestResource::new, 1337);

        final OpaqueMemoryResource<TestResource> resource2 =
                memoryManager.getExternalSharedMemoryResource(
                        "external-type-2", TestResource::new, 1337);

        assertThat(resource2).isNotSameAs(resource1);
        assertThat(resource2.getResourceHandle()).isNotSameAs(resource1.getResourceHandle());
    }

    @Test
    void testReleaseDisposesExternalResource() throws Exception {
        final MemoryManager memoryManager = createMemoryManager();
        final OpaqueMemoryResource<TestResource> resource =
                memoryManager.getExternalSharedMemoryResource(
                        "external-type", TestResource::new, 1337);

        resource.close();

        assertThat(resource.getResourceHandle().closed).isTrue();
    }

    @Test
    void testAllocateResourceInitializeFail() {
        final MemoryManager memoryManager = createMemoryManager();

        assertThatExceptionOfType(Throwable.class)
                .isThrownBy(
                        () ->
                                memoryManager.getSharedMemoryResourceForManagedMemory(
                                        "type",
                                        (ignore) -> {
                                            throw new RuntimeException("initialization fail");
                                        },
                                        0.1));
        assertThat(memoryManager.verifyEmpty()).isTrue();
    }
    // ------------------------------------------------------------------------
    //  Utils
    // ------------------------------------------------------------------------

    private static MemoryManager createMemoryManager() {
        final long size = 128 * 1024 * 1024;
        final MemoryManager mm = MemoryManagerBuilder.newBuilder().setMemorySize(size).build();

        // this is to guard test assumptions
        assertThat(mm.getMemorySize()).isEqualTo(size);
        assertThat(mm.availableMemory()).isEqualTo(size);

        return mm;
    }

    private static final class TestResource implements AutoCloseable {

        final long size;
        boolean closed;

        TestResource(long size) {
            this.size = size;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
