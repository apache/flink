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

import static org.apache.flink.util.concurrent.Executors.directExecutor;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** {@link TaskChangelogRegistryImpl} test. */
public class TaskChangelogRegistryImplTest {

    @Test
    public void testDiscardedWhenNotUsed() {
        TaskChangelogRegistry registry = new TaskChangelogRegistryImpl(directExecutor());
        TestingStreamStateHandle handle = new TestingStreamStateHandle();
        long refCount = 2;
        registry.startTracking(handle, refCount);
        for (int i = 0; i < refCount; i++) {
            assertFalse(handle.isDisposed());
            registry.release(handle);
        }
        assertTrue(handle.isDisposed());
    }

    @Test
    public void testNotDiscardedIfStoppedTracking() {
        TaskChangelogRegistry registry = new TaskChangelogRegistryImpl(directExecutor());
        TestingStreamStateHandle handle = new TestingStreamStateHandle();
        long refCount = 2;
        registry.startTracking(handle, refCount);
        registry.stopTracking(handle);
        for (int i = 0; i < refCount; i++) {
            assertFalse(handle.isDisposed());
            registry.release(handle);
        }
        assertFalse(handle.isDisposed());
    }
}
