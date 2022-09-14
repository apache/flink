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

package org.apache.flink.changelog.fs;

import org.apache.flink.runtime.state.TestingStreamStateHandle;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.util.concurrent.Executors.directExecutor;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** {@link TaskChangelogRegistryImpl} test. */
public class TaskChangelogRegistryImplTest {

    @Test
    public void testDiscardedWhenNotUsed() {
        TaskChangelogRegistry registry = new TaskChangelogRegistryImpl(directExecutor());
        TestingStreamStateHandle handle = new TestingStreamStateHandle();
        List<UUID> backends = Arrays.asList(UUID.randomUUID(), UUID.randomUUID());
        registry.startTracking(handle, new HashSet<>(backends));
        for (UUID backend : backends) {
            assertFalse(handle.isDisposed());
            registry.notUsed(handle, backend);
        }
        assertTrue(handle.isDisposed());
    }

    @Test
    public void testNotDiscardedIfStoppedTracking() {
        TaskChangelogRegistry registry = new TaskChangelogRegistryImpl(directExecutor());
        TestingStreamStateHandle handle = new TestingStreamStateHandle();
        List<UUID> backends = Arrays.asList(UUID.randomUUID(), UUID.randomUUID());
        registry.startTracking(handle, new HashSet<>(backends));
        registry.stopTracking(handle);
        backends.forEach(id -> registry.notUsed(handle, id));
        assertFalse(handle.isDisposed());
    }
}
